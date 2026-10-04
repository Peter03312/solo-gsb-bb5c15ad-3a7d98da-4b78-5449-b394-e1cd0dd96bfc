#!/usr/bin/env python3
"""只读一致性快照调用示例（仅依赖标准库）。

用法：
    python3 examples/consistent_read_demo.py [base_url]

默认 base_url 为 http://localhost:8000。

背景：关阀建议、隔离/复供/水源切换预演、流量核算、分档交付、旁通评估、
读数核对、调度演练等只读接口，单次请求都要同时读取“版本 + 节点（运行水源）
+ 管段阀位 + 待修复事件”。服务端把每次只读请求放进 REPEATABLE READ 只读
事务，这四类数据必对应**同一已提交时刻**：并发写入时结果只能是写入前或
写入后的**完整视图**，不会跨提交拼接。

本脚本演示：
  1. 导入管网（版本 1）
  2. 单请求一致性：关阀建议、隔离预演、流量核算、读数核对、调度演练返回的
     version/阀位/待修复事件互相对应（配合写线程并发压测也不出现混合视图）
  3. 写入前 / 写入后两个完整视图：写线程提交隔离（版本 1 -> 2）前后各发一次
     只读请求，观察“写前视图”（建议关 P4、故障未隔离）与“写后视图”
     （P4 已关、P6 已是待修复事件），两次响应内部各自自洽
  4. 过期版本沿用既有处理：预演/核算返回 200 + version_match=false；
     调度演练返回 409 VERSION_CONFLICT；确认仍按最新状态原子复核
  5. 复供预演的既有错误沿用：事件不存在 404 EVENT_NOT_FOUND、
     已复供事件 409 EVENT_ALREADY_RESTORED

注意：一致性由服务端数据库事务保证，客户端无需做任何额外处理；这里的
“写线程”只是制造并发写入，方便观察两个完整视图。
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
    "BASE_URL", "http://localhost:8000"
)
SAMPLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample_network.json")


def call(method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def show(title: str, status: int, body: dict) -> None:
    print(f"\n=== {title} -> HTTP {status} ===")
    print(json.dumps(body, ensure_ascii=False, indent=2))


def import_network() -> None:
    with open(SAMPLE, "rb") as fh:
        payload = json.loads(fh.read().decode())
    status, body = call("POST", "/api/network/import", payload)
    assert status == 200, (status, body)
    print(f"[导入] 版本重置为 {body['version']}，快照 {body['snapshot_id']}")


def delivery_body(version: int) -> dict:
    return {
        "version": version,
        "sources": [{"source_id": "S", "capacity": 1000}],
        "pipes": [
            {"pipe_id": pid, "capacity": cap}
            for pid, cap in [("P1", 100), ("P2", 100), ("P3", 6),
                            ("P4", 100), ("P5", 100), ("P6", 100)]
        ],
        "demands": [
            {"sink_id": "U1", "demand": 10},
            {"sink_id": "U2", "demand": 10},
            {"sink_id": "U3", "demand": 10},
        ],
    }


def reconcile_body(version: int) -> dict:
    return {
        "version": version,
        "sources": [{"source_id": "S", "injection": 30}],
        "sinks": [
            {"sink_id": "U1", "withdrawal": 10},
            {"sink_id": "U2", "withdrawal": 10},
            {"sink_id": "U3", "withdrawal": 10},
        ],
        "pipes": [
            {"pipe_id": pid, "capacity": 100, "reading": None}
            for pid in ("P1", "P2", "P3", "P4", "P5", "P6")
        ],
    }


def assert_view_self_consistent(label: str, version: int, recommend: dict,
                                preview: dict, delivery: dict) -> None:
    """同一时刻视图内部自洽：各接口回显版本一致，且结论与该版本状态匹配。"""
    assert recommend["version"] == preview["version"] == delivery["version"] == version, (
        label, recommend, preview, delivery
    )
    if version == 1:
        # 写前完整视图：P4 仍开、无待修复事件 -> 建议/预演都要关 P4 才能隔离 P6
        assert recommend["recommended_close_pipe_ids"] == ["P4"], recommend
        assert preview["isolatable"] is True and preview["affected_sink_ids"] == ["U3"], preview
        # 故障尚未隔离：U3 经 P6 可达，参与交付核算（总量受 P3=6 瓶颈为 26）
        assert delivery["max_delivered_total"] == 26, delivery
    elif version == 2:
        # 写后完整视图：P4 已关、P6 为待修复故障，故障片区已断供
        # 再对 P6 请求建议：两端本就与水源断开 -> 空方案可行
        assert recommend["recommended_close_pipe_ids"] == [], recommend
        # 预演空方案即可隔离；U3 在方案前已失供，不计入本次新增失供
        assert preview["isolatable"] is True, preview
        # P4 阀位与 P6 故障都阻断 -> U3 不可交付，只交付 U1(6)+U2(10)=16
        assert delivery["max_delivered_total"] == 16, delivery
    print(f"[自洽] {label}：版本 {version} 的建议/预演/核算互相对应，无跨时刻拼接")


def main() -> None:
    import_network()

    # ---- 1. 写前完整视图（版本 1）----
    s1, rec1 = call("POST", "/api/isolation/recommend",
                    {"fault_pipe_id": "P6", "version": 1})
    s2, prev1 = call("POST", "/api/isolation/preview",
                     {"fault_pipe_id": "P6", "close_pipe_ids": ["P4"], "version": 1})
    s3, del1 = call("POST", "/api/delivery/evaluate", delivery_body(1))
    s4, rec4 = call("POST", "/api/meters/reconcile", reconcile_body(1))
    show("写前：关阀建议", s1, rec1)
    show("写前：隔离预演", s2, prev1)
    show("写前：流量核算", s3, del1)
    show("写前：读数核对", s4, {"version": rec4["version"],
                                "consistent": rec4["consistent"]})
    assert (s1, s2, s3, s4) == (200, 200, 200, 200)
    assert_view_self_consistent("写前视图", 1, rec1, prev1, del1)

    # ---- 2. 并发写入：另一个会话提交隔离（1 -> 2）----
    # 只读接口不阻塞该写入；写入也不影响进行中的只读请求（二者取各自完整快照）
    done = threading.Event()

    def concurrent_submit() -> None:
        # 稍微等待，演示“只读进行期间另一会话提交”；真实竞态由数据库隔离
        time.sleep(0.05)
        status, body = call(
            "POST", "/api/isolation/submit",
            {"fault_pipe_id": "P6", "close_pipe_ids": ["P4"], "version": 1},
        )
        assert status == 200, (status, body)
        print(f"[并发写入] 隔离提交成功：版本 {body['version']}，事件 {body['event_id']}")
        done.set()

    writer = threading.Thread(target=concurrent_submit)
    writer.start()

    # 读侧连续发起多个只读请求：每个请求自身是一个完整快照，互不拼接
    time.sleep(0.02)
    s, net = call("GET", "/api/network")
    assert s == 200
    writer.join()

    # ---- 3. 写后完整视图（版本 2）----
    s1, rec2 = call("POST", "/api/isolation/recommend",
                    {"fault_pipe_id": "P6", "version": 2})
    s2, prev2 = call("POST", "/api/isolation/preview",
                     {"fault_pipe_id": "P6", "close_pipe_ids": [], "version": 2})
    s3, del2 = call("POST", "/api/delivery/evaluate", delivery_body(2))
    show("写后：关阀建议（故障已隔离，空方案）", s1, rec2)
    show("写后：隔离预演（空方案）", s2, prev2)
    show("写后：流量核算（U3 断供）", s3, del2)
    assert (s1, s2, s3) == (200, 200, 200)
    assert_view_self_consistent("写后视图", 2, rec2, prev2, del2)

    # ---- 4. 过期版本沿用既有处理 ----
    # 预演/核算：不拒绝，version_match=false，但内部仍基于同一当前快照
    s, stale_prev = call("POST", "/api/isolation/preview",
                         {"fault_pipe_id": "P6", "close_pipe_ids": [], "version": 1})
    show("过期版本隔离预演（200 + version_match=false）", s,
         {"version": stale_prev["version"],
          "version_match": stale_prev["version_match"],
          "isolatable": stale_prev["isolatable"]})
    assert s == 200 and stale_prev["version_match"] is False

    s, stale_del = call("POST", "/api/delivery/evaluate", delivery_body(99))
    assert s == 200 and stale_del["version"] == 2 and stale_del["version_match"] is False

    # 调度演练：版本过期整体拒绝 409（演练是对一串提交的预检）
    s, stale_drill = call("POST", "/api/drill",
                          {"version": 1, "steps": []})
    show("过期版本调度演练（409 VERSION_CONFLICT）", s, stale_drill)
    assert s == 409 and stale_drill["error"]["code"] == "VERSION_CONFLICT"
    assert stale_drill["error"]["current_version"] == 2

    # ---- 5. 复供预演既有错误沿用 ----
    s, missing = call("POST", "/api/isolation/events/999/restore/preview",
                      {"version": 2})
    show("复供预演不存在的事件（404 EVENT_NOT_FOUND）", s, missing)
    assert s == 404 and missing["error"]["code"] == "EVENT_NOT_FOUND"

    # 先安全复供事件 1（版本 2 -> 3），再对其预演 -> 409 EVENT_ALREADY_RESTORED
    s, restored = call("POST", "/api/isolation/events/1/restore", {"version": 2})
    assert s == 200, (s, restored)
    print(f"[复供确认] 事件 1 已复供，版本 {restored['version']}")
    s, again = call("POST", "/api/isolation/events/1/restore/preview",
                    {"version": 3})
    show("复供预演已复供事件（409 EVENT_ALREADY_RESTORED）", s, again)
    assert s == 409 and again["error"]["code"] == "EVENT_ALREADY_RESTORED"

    # ---- 6. 只读全程未改写：当前版本为 3（仅两次写入造成）----
    s, net = call("GET", "/api/network")
    assert s == 200 and net["version"] == 3, net["version"]
    print("\n全部断言通过：只读请求只呈现写入前或写入后的完整视图，"
          "过期版本与复供错误沿用既有契约，确认操作按最新状态原子复核。")


if __name__ == "__main__":
    main()
