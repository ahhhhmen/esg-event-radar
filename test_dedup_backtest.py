#!/usr/bin/env python3
"""
基准数据集回测 — 去重管道验证
══════════════════════════════════════════════════════════
测试集包含：
  · 5 组跨语言重复事件（中/英/印尼/法 — 应合并）
  · 3 组时间相近但主题不同的事件（应保持独立）
  · 2 条缺少 standard_name_en 的退化数据（降级路径）

验证指标：漏合并（FN） vs 误合并（FP）
"""
from __future__ import annotations

import hashlib
import logging
import re

# 极简日志
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
logger = logging.getLogger("backtest")

# ── 本地导入 ─────────────────────────────────────────────
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from schemas.event import EventItem


# ═══════════════════════════════════════════════════════════
# 1) 构建基准测试数据集
# ═══════════════════════════════════════════════════════════

def make_event(**overrides) -> EventItem:
    """用最小必要字段创建 EventItem，其余取默认值。"""
    defaults = {
        "event_id":  "",
        "name":      "placeholder",
        "organizer": "Test Org",
        "start_date":"2026-06-15",
        "end_date":  "2026-06-16",
        "timezone":  "UTC",
        "is_recurring": False,
        "format":       "in-person",
        "city":         None,
        "country":      None,
        "venue":        None,
        "registration_url": None,
        "registration_deadline": None,
        "fee_tier":          "unknown",
        "topics":            ["esg"],
        "audience":          [],
        "agenda_highlights": "",
        "exec_value_score":  3,
        "exec_value_rationale": "",
        "source_url":    "https://example.com/test",
        "source_org":    "Test",
        "discovered_at": "2026-06-10T00:00:00Z",
        "raw_snippet":   "",
        "standard_name_en": "",
        "display_name_zh": "",
    }
    defaults.update(overrides)
    ev = EventItem(**defaults)
    # 统一生成 event_id
    raw = f"{ev.standard_name_en.strip().lower()}|{ev.start_date.strip()}"
    ev.event_id = hashlib.sha256(raw.encode()).hexdigest()[:12]
    return ev


# ── 5 组跨语言重复对（每组 2-3 条，共 12 条）─────────────
# 预期：每组合并为 1 条，共 5 条输出

duplicate_pairs = [
    # Pair 1: EN / ZH
    [
        make_event(name="EU ESG Summit", standard_name_en="EU ESG Summit",
                   display_name_zh="欧盟 ESG 峰会", start_date="2026-09-10"),
        make_event(name="EU ESG Summit — 欧盟 ESG 峰会", standard_name_en="EU ESG Summit",
                   display_name_zh="欧盟 ESG 峰会", start_date="2026-09-11"),  # 日期差 1 天
    ],
    # Pair 2: ZH / EN (反向)
    [
        make_event(name="联合国气候变化大会", standard_name_en="UN Climate Change Conference",
                   display_name_zh="联合国气候变化大会", start_date="2026-11-01"),
        make_event(name="UN Climate Change Conference — 联合国气候变化大会",
                   standard_name_en="UN Climate Change Conference",
                   display_name_zh="联合国气候变化大会", start_date="2026-10-31"),  # 日期差 1 天
    ],
    # Pair 3: 印尼 / EN
    [
        make_event(name="Konferensi Keberlanjutan ASEAN",
                   standard_name_en="ASEAN Sustainability Conference",
                   display_name_zh="东盟可持续发展会议", start_date="2026-08-20"),
        make_event(name="ASEAN Sustainability Conference — 东盟可持续发展会议",
                   standard_name_en="ASEAN Sustainability Conference",
                   display_name_zh="东盟可持续发展会议", start_date="2026-08-20"),
    ],
    # Pair 4: 法语 / EN
    [
        make_event(name="Conférence sur le Développement Durable",
                   standard_name_en="Sustainable Development Conference",
                   display_name_zh="可持续发展会议", start_date="2027-03-05"),
        make_event(name="Sustainable Development Conference — 可持续发展会议",
                   standard_name_en="Sustainable Development Conference",
                   display_name_zh="可持续发展会议", start_date="2027-03-06"),  # 差 1 天
    ],
    # Pair 5: 三语（EN + ZH + 印尼），来自 3 个源
    [
        make_event(name="Global Net Zero Forum — 全球净零论坛",
                   standard_name_en="Global Net Zero Forum",
                   display_name_zh="全球净零论坛", start_date="2026-10-01"),
        make_event(name="Forum Net Zero Global — 全球净零论坛",
                   standard_name_en="Global Net Zero Forum",
                   display_name_zh="全球净零论坛", start_date="2026-09-30"),
        make_event(name="Forum Net Zero Global (Indonesia)",
                   standard_name_en="Global Net Zero Forum",
                   display_name_zh="全球净零论坛", start_date="2026-10-01"),
    ],
]

# ── 3 组时间相近但主题不同（应保持独立）─────────────────
same_day_different = [
    [
        make_event(name="Carbon Market Workshop — 碳市场研讨会",
                   standard_name_en="Carbon Market Workshop",
                   display_name_zh="碳市场研讨会", start_date="2026-12-01"),
        make_event(name="Biodiversity Policy Forum — 生物多样性政策论坛",
                   standard_name_en="Biodiversity Policy Forum",
                   display_name_zh="生物多样性政策论坛", start_date="2026-12-02"),
    ],
    [
        make_event(name="Green Finance Summit — 绿色金融峰会",
                   standard_name_en="Green Finance Summit",
                   display_name_zh="绿色金融峰会", start_date="2027-01-15"),
        make_event(name="Renewable Energy Expo — 可再生能源博览会",
                   standard_name_en="Renewable Energy Expo",
                   display_name_zh="可再生能源博览会", start_date="2027-01-15"),
    ],
    [
        make_event(name="ESG Reporting Workshop — ESG 报告研讨会",
                   standard_name_en="ESG Reporting Workshop",
                   display_name_zh="ESG 报告研讨会", start_date="2026-07-01"),
        make_event(name="Human Rights Due Diligence Forum — 人权尽职调查论坛",
                   standard_name_en="Human Rights Due Diligence Forum",
                   display_name_zh="人权尽职调查论坛", start_date="2026-07-02"),
        make_event(name="Circular Economy Roundtable — 循环经济圆桌",
                   standard_name_en="Circular Economy Roundtable",
                   display_name_zh="循环经济圆桌", start_date="2026-07-01"),
    ],
]

# ── 2 条退化数据（缺少 standard_name_en）────────────────
degraded_events = [
    make_event(name="Some Random Event", standard_name_en="",
               display_name_zh="", start_date="2026-12-25"),  # 完全缺失
    make_event(name="Another Event — 另一个会议", standard_name_en="",
               display_name_zh="另一个会议", start_date="2026-12-26"),  # 部分缺失
]


# ═══════════════════════════════════════════════════════════
# 2) 去重算法（从 EventRadarAgent 提取的独立版本）
# ═══════════════════════════════════════════════════════════

def run_dedup(events: list[EventItem]) -> tuple[list[EventItem], int]:
    """对事件列表执行 v1.5 去重，返回 (去重后列表, 移除数量)。"""
    from datetime import date as date_type
    from collections import defaultdict

    if len(events) <= 1:
        return events, 0

    # 解析日期
    parsed_dates: dict[int, date_type | None] = {}
    for i, ev in enumerate(events):
        if ev.start_date and ev.start_date not in ("unknown", ""):
            try:
                parsed_dates[i] = date_type.fromisoformat(ev.start_date)
            except ValueError:
                parsed_dates[i] = None
        else:
            parsed_dates[i] = None

    n = len(events)
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x: int, y: int) -> None:
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[rx] = ry

    def _std_key(ev: EventItem) -> str:
        s = (ev.standard_name_en or ev.name).strip().lower()
        for suffix in ["conference", "summit", "forum", "meeting", "event",
                       "workshop", "webinar", "symposium", "congress", "assembly",
                       "seminar", "expo", "roundtable", "dialogue",
                       "annual", "international", "global", "world",
                       "2024", "2025", "2026", "2027", "2028"]:
            s = re.sub(rf"\b{re.escape(suffix)}\b", "", s)
        s = re.sub(r"[^\w\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    # 策略 1: standard_name_en 精确匹配 + 日期 ±3 天
    for i in range(n):
        for j in range(i + 1, n):
            if find(i) == find(j):
                continue
            date_a = parsed_dates.get(i)
            date_b = parsed_dates.get(j)
            dates_close = False
            if date_a is not None and date_b is not None:
                diff = abs((date_a - date_b).days)
                dates_close = diff <= 3
            elif date_a is None and date_b is None:
                dates_close = True
            if not dates_close:
                continue

            key_a = _std_key(events[i])
            key_b = _std_key(events[j])
            if key_a and key_b and key_a == key_b:
                union(i, j)
                logger.debug(f"  [精确去重] std_en='{key_a}' | {events[i].name[:60]} <-> {events[j].name[:60]}")

    # 策略 2: Jaccard ≥ 0.70 + 日期 ±7 天
    for i in range(n):
        for j in range(i + 1, n):
            if find(i) == find(j):
                continue
            date_a = parsed_dates.get(i)
            date_b = parsed_dates.get(j)
            if date_a is None or date_b is None:
                continue
            diff = abs((date_a - date_b).days)
            if diff > 7:
                continue
            words_a = set(_std_key(events[i]).split())
            words_b = set(_std_key(events[j]).split())
            if not words_a or not words_b:
                continue
            jaccard = len(words_a & words_b) / len(words_a | words_b)
            if jaccard >= 0.70:
                union(i, j)
                logger.debug(f"  [退避去重] jaccard={jaccard:.2f} diff={diff}d | {events[i].name[:60]} <-> {events[j].name[:60]}")

    # 聚类
    clusters_map: dict[int, list[int]] = defaultdict(list)
    for i in range(n):
        clusters_map[find(i)].append(i)

    merged: list[EventItem] = []
    removed = 0
    for indices in clusters_map.values():
        cluster = [events[i] for i in indices]
        if len(cluster) == 1:
            merged.append(cluster[0])
        else:
            # 简单合并：取评分最高的
            best = max(cluster, key=lambda e: e.exec_value_score)
            removed += len(cluster) - 1
            merged.append(best)
            logger.debug(f"  [合并] {len(cluster)}条→1条: {best.name[:80]}")

    return merged, removed


# ═══════════════════════════════════════════════════════════
# 3) 回测执行与统计
# ═══════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("基准数据集回测 — ESG Event Radar v1.5 去重管道")
    print("=" * 70)

    # ── 汇总所有事件 ──────────────────────────────────────
    all_events: list[EventItem] = []
    pair_labels: list[str] = []          # 每个事件属于哪个 pair
    pair_expected_merged: dict[int, bool] = {}  # pair_idx → 预期是否合并

    pair_idx = 0
    # 跨语言重复对（应合并）
    for pair in duplicate_pairs:
        for ev in pair:
            all_events.append(ev)
            pair_labels.append(f"dup_{pair_idx}")
        pair_expected_merged[pair_idx] = True
        pair_idx += 1

    # 不同主题对（应保持独立）
    diff_pair_start = pair_idx
    for pair in same_day_different:
        for ev in pair:
            all_events.append(ev)
            pair_labels.append(f"diff_{pair_idx}")
        pair_expected_merged[pair_idx] = False
        pair_idx += 1

    # 退化数据
    deg_start = pair_idx
    for i, ev in enumerate(degraded_events):
        all_events.append(ev)
        pair_labels.append(f"deg_{pair_idx + i}")
        pair_expected_merged[pair_idx + i] = False  # 退化数据不应误合并
    pair_idx += len(degraded_events)

    total_input = len(all_events)
    print(f"\n输入事件总数: {total_input}")
    print(f"  - 跨语言重复对: {len(duplicate_pairs)} 组 ({sum(len(p) for p in duplicate_pairs)} 条)")
    print(f"  - 不同主题对:   {len(same_day_different)} 组 ({sum(len(p) for p in same_day_different)} 条)")
    print(f"  - 退化数据:     {len(degraded_events)} 条")
    print()

    # ── 执行去重 ──────────────────────────────────────────
    merged, removed = run_dedup(all_events)
    print(f"\n去重后事件数: {len(merged)} (移除 {removed} 条)")

    # ── 统计 ──────────────────────────────────────────────
    # 构建输出中每个 pair 的存活情况
    # 用聚类标识分组
    from collections import defaultdict
    cluster_of: dict[int, int] = {}  # event_idx_in_all → cluster_id
    # 重新跑一次简单聚类来追踪（用 run_dedup 内的逻辑无法直接拿到映射）
    # 改用简便方法：按 standard_name_en + start_date 窗口手动分组

    # 更简单：用已实现的 run_dedup 返回 merged 列表，
    # 但 merged 列表丢失了原始索引。我们改用手动映射。

    # 返回 merged 列表的每个元素，和原始 all_events 做 identity 比较
    # EventItem 是 Pydantic model，value equality 可行
    # 但 merged 中的 best 可能恰好等于原始事件（同对象），可以映射

    # 简便方案：重新跑一次聚类，输出每个 pair 的合并结果
    # 直接用 run_dedup 已经够了，我们手动对照

    # ── 手动验证 ──────────────────────────────────────────
    fp_count = 0  # 误合并（FP）：不应合并的被合并了
    fn_count = 0  # 漏合并（FN）：应合并的没合并

    # 检查每对跨语言重复：输出中它们的 standard_name_en 是否只出现一次
    output_names = [ev.standard_name_en.strip().lower() for ev in merged]

    # 对每个 duplicate pair，检查去重后是否<=1条
    for pair_idx, pair in enumerate(duplicate_pairs):
        std_names = {ev.standard_name_en.strip().lower() for ev in pair}
        # 这个 pair 的唯一 std_en
        pair_std = list(std_names)[0] if std_names else ""
        occurrences = output_names.count(pair_std)
        if occurrences <= 1:
            logger.info(f"  ✅ dup_{pair_idx}: 正确合并 ({len(pair)}条→{occurrences}条) | std_en='{pair_std}'")
        else:
            fn_count += 1
            logger.error(f"  ❌ dup_{pair_idx}: 漏合并！{len(pair)}条→{occurrences}条 | std_en='{pair_std}'")

    # 对每个 different pair，检查是否被错误合并
    for pair_idx_rel, pair in enumerate(same_day_different):
        actual_idx = diff_pair_start + pair_idx_rel
        # 取 pair 中每个事件的 std_en，在输出中应各出现 1 次
        all_present = True
        for ev in pair:
            std = ev.standard_name_en.strip().lower()
            if output_names.count(std) != 1:
                all_present = False
                break
        if all_present:
            logger.info(f"  ✅ diff_{actual_idx}: 正确保持独立 ({len(pair)}条→{len(pair)}条)")
        else:
            fp_count += 1
            logger.error(f"  ❌ diff_{actual_idx}: 误合并！预期 {len(pair)} 条独立，但输出中丢失")

    # 退化数据检查：不应崩溃
    deg_names_in = [ev.standard_name_en or ev.name for ev in degraded_events]
    logger.info(f"  ✅ 退化数据处理: {len(degraded_events)} 条输入 → 管道未崩溃")

    # ── 最终报告 ──────────────────────────────────────────
    print("\n" + "=" * 70)
    print("回测结果")
    print("=" * 70)

    dup_pairs_count = len(duplicate_pairs)
    diff_pairs_count = len(same_day_different)

    precision = (diff_pairs_count - fp_count) / diff_pairs_count if diff_pairs_count else 1.0
    recall = (dup_pairs_count - fn_count) / dup_pairs_count if dup_pairs_count else 1.0

    print(f"  漏合并 (FN): {fn_count}/{dup_pairs_count}  (应合未合)")
    print(f"  误合并 (FP): {fp_count}/{diff_pairs_count}  (不应合被合)")
    print(f"  准确率 (Precision): {precision:.1%}")
    print(f"  召回率 (Recall):    {recall:.1%}")
    print()

    if fn_count == 0 and fp_count == 0:
        print("  ✅ 所有指标通过！去重管道工作正常。")
    else:
        print("  ⚠️  存在未通过的测试用例，请检查上方详细日志。")

    # 额外检查：退化数据 event_id 是否正确生成
    print("\n── 退化数据 event_id 降级检查 ──")
    for ev in degraded_events:
        has_id = bool(ev.event_id)
        print(f"  name='{ev.name[:50]}' → event_id={'✅ ' + ev.event_id if has_id else '❌ 缺失'}")

    print("\n" + "=" * 70)
    print("回测完成")
    print("=" * 70)


if __name__ == "__main__":
    main()