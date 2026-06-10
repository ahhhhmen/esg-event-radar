#!/usr/bin/env python3
"""
E2E 极简闭环测试 — Phase 0→1→3→4→4.3→6
══════════════════════════════════════════════════════
选取 3 个代表性源（RSS + Google News + HTML Calendar），
跑通完整管道并报告每阶段产出来与 Notion 写入稳定生。

依赖检查
────────
  纯 Python 无 C 扩展。核心：feedparser / requests /
  beautifulsoup4 / pydantic / openai / icalendar / tenacity

用法
────
  python test_e2e_mini.py               # 含 Notion 写入
  python test_e2e_mini.py --no-notion    # 跳过 Notion
"""
import argparse
import copy
import json
import logging
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("e2e_mini")

# ═══════════════════════════════════════════════════════════
# 1) 代表性源定义（3 轨各选 1 个，覆盖不同格式与语言）
# ═══════════════════════════════════════════════════════════
MINI_SOURCES = [
    {
        "name": "ESG Today (RSS EN)",
        "url": "https://www.esgtoday.com/feed/",
        "type": "rss",
        "tier": 3,
        "region": "global",
        "org": "ESG Today",
        "tags": ["ESG-news", "conference-announcement"],
        "active": True,
    },
    {
        "name": "GNews: Tier1 Org Events",
        "url": (
            "https://news.google.com/rss/search?"
            "q=%28WBCSD+OR+%22UN+Global+Compact%22+OR+SBTi+OR+CDP%29"
            "+%28summit+OR+forum+OR+conference%29+2026"
            "&hl=en-US&gl=US&ceid=US:en"
        ),
        "type": "google_news",
        "tier": 1,
        "region": "global",
        "org": "Google News",
        "tags": ["WBCSD", "UNGC", "SBTi", "CDP"],
        "active": True,
    },
    {
        "name": "UNGC Events Page (HTML Calendar)",
        "url": "https://www.unglobalcompact.org/take-action/events",
        "type": "html_calendar",
        "tier": 1,
        "region": "global",
        "org": "UN Global Compact",
        "tags": ["SDGs", "CEO-commitment", "Leaders-Summit"],
        "active": True,
    },
]


# ═══════════════════════════════════════════════════════════
# 2) 依赖分析
# ═══════════════════════════════════════════════════════════
def analyze_dependencies():
    """检查所有 import 是否为纯 Python（无 C 扩展）。"""
    import importlib
    deps = [
        "feedparser", "requests", "bs4", "pydantic", "openai",
        "icalendar", "tenacity", "yaml", "dotenv",
    ]
    results = {}
    all_pure = True

    for dep in deps:
        try:
            mod = importlib.import_module(dep)
            has_c_ext = False
            if hasattr(mod, "__file__") and mod.__file__:
                ext = Path(mod.__file__).suffix
                if ext in (".so", ".pyd", ".dylib"):
                    has_c_ext = True
                    all_pure = False
            results[dep] = {
                "version": getattr(mod, "__version__", "N/A"),
                "c_extension": has_c_ext,
            }
        except ImportError as e:
            results[dep] = {"version": None, "error": str(e)}
            all_pure = False

    return results, all_pure


# ═══════════════════════════════════════════════════════════
# 3) E2E 管道执行
# ═══════════════════════════════════════════════════════════
def run_e2e(no_notion: bool = False):
    from event_radar_agent import EventRadarAgent

    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "sources_used": [s["name"] for s in MINI_SOURCES],
        "stages": {},
    }

    # ── 环境变量检查 ──────────────────────────────────────
    env_status = {
        "DEEPSEEK_API_KEY": bool(os.environ.get("DEEPSEEK_API_KEY")),
        "NOTION_TOKEN": bool(os.environ.get("NOTION_TOKEN")),
        "NOTION_DATABASE_ID": bool(os.environ.get("NOTION_DATABASE_ID")),
    }
    report["env_status"] = env_status
    if not env_status["DEEPSEEK_API_KEY"]:
        logger.error("❌ DEEPSEEK_API_KEY 未设置，无法运行 LLM 提取")
        return report, False

    logger.info("══ E2E Mini 集成测试启动 ══")
    logger.info(f"  源: {', '.join(report['sources_used'])}")
    logger.info(f"  跳过 Notion: {no_notion}")

    # ── 初始化 Agent ─────────────────────────────────────
    agent = EventRadarAgent()
    agent._sources = copy.deepcopy(MINI_SOURCES)
    logger.info(f"[Phase 0] 加载 {len(agent._sources)} 个测试源")

    # ── Phase 1: 抓取 ────────────────────────────────────
    t_start = time.monotonic()
    agent._fetch_html_calendars()
    agent._fetch_all_sources()
    report["stages"]["fetch"] = {
        "raw_items": len(agent._raw_items),
        "elapsed_s": round(time.monotonic() - t_start, 1),
    }
    logger.info(
        f"[Phase 1] 抓取: {len(agent._raw_items)} 条 "
        f"({report['stages']['fetch']['elapsed_s']}s)"
    )
    if not agent._raw_items:
        logger.error("❌ 零数据 — 所有源抓取量=0")
        return report, False

    # ── Phase 3: 正文提取 ────────────────────────────────
    t_start = time.monotonic()
    agent._enrich_content()
    bodies = sum(1 for it in agent._raw_items if it.get("body") and len(it["body"]) > 20)
    report["stages"]["content"] = {
        "total": len(agent._raw_items),
        "bodies": bodies,
        "elapsed_s": round(time.monotonic() - t_start, 1),
    }
    logger.info(f"[Phase 3] 正文: {bodies}/{len(agent._raw_items)} 条")

    # ── Phase 4: LLM 提取 ────────────────────────────────
    t_start = time.monotonic()
    agent._extract_events_with_llm()
    normal = sum(1 for e in agent._events if not e.is_degraded)
    degraded = sum(1 for e in agent._events if e.is_degraded)
    report["stages"]["llm"] = {
        "total": len(agent._events),
        "normal": normal,
        "degraded": degraded,
        "elapsed_s": round(time.monotonic() - t_start, 1),
    }
    logger.info(
        f"[Phase 4] LLM: {normal} normal + {degraded} degraded "
        f"({report['stages']['llm']['elapsed_s']}s)"
    )
    if not agent._events:
        logger.info("→ 无事件，静默阻断")
        return report, True

    # ── Phase 4.3: 去重 ──────────────────────────────────
    before = len(agent._events)
    agent._deduplicate_events()
    after = len(agent._events)
    removed = before - after
    report["stages"]["dedup"] = {
        "before": before, "after": after, "removed": removed,
    }
    dup_rate = f"{removed / before:.0%}" if before else "N/A"
    logger.info(f"[Phase 4.3] 去重: {before}→{after} (移除{removed}条, 拦截率{dup_rate})")

    # ── 打印事件 ─────────────────────────────────────────
    logger.info("── 事件清单 ──")
    for ev in sorted(agent._events, key=lambda e: e.exec_value_score, reverse=True):
        flag = "⚠️" if ev.is_degraded else "  "
        logger.info(
            f"  {flag} [{ev.exec_value_score}★] {ev.name[:70]}"
            f"  | std_en={ev.standard_name_en[:40]}"
            f"  | {ev.start_date} | {ev.format}"
        )

    # ── Phase 6: Notion ──────────────────────────────────
    if not no_notion and env_status["NOTION_TOKEN"] and env_status["NOTION_DATABASE_ID"]:
        t_start = time.monotonic()
        # 临时清空 _events 再重新赋值为去重后的结果 — 防止之前日志干扰
        events_for_notion = list(agent._events)
        # 手动调用 upsert（agent._upsert_to_notion 内部读 self._events）
        agent._events = events_for_notion
        agent._upsert_to_notion()
        report["stages"]["notion"] = {
            "count": len(events_for_notion),
            "elapsed_s": round(time.monotonic() - t_start, 1),
        }
        logger.info(
            f"[Phase 6] Notion: {len(events_for_notion)} 条 "
            f"({report['stages']['notion']['elapsed_s']}s)"
        )
    else:
        logger.info("[Phase 6] Notion 跳过")

    report["success"] = True
    return report, True


# ═══════════════════════════════════════════════════════════
# main
# ═══════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="E2E Mini Integration Test")
    parser.add_argument("--no-notion", action="store_true", help="跳过 Notion 写入")
    args = parser.parse_args()

    print("=" * 60)
    print("E2E Mini 集成测试")
    print("=" * 60)

    # ── 依赖分析 ─────────────────────────────────────────
    deps, all_pure = analyze_dependencies()
    print("\n── 依赖分析 ──")
    for name, info in deps.items():
        if info.get("error"):
            print(f"  ❌ {name}: MISSING ({info['error']})")
        else:
            c_flag = "  ⚠️ C扩展" if info["c_extension"] else "✅ 纯Python"
            print(f"  {c_flag}  {name}=={info['version']}")
    print(f"  综合: {'✅ 全纯Python，可Docker化' if all_pure else '⚠️ 含C扩展，需额外编译'}")

    # ── 运行管道 ─────────────────────────────────────────
    print("\n── 管道运行 ──")
    report, success = run_e2e(no_notion=args.no_notion)

    # ── 总结 ─────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("E2E 结果概要")
    print("=" * 60)
    for stage, info in report.get("stages", {}).items():
        stage_names = {
            "fetch": "Phase 1  抓取",
            "content": "Phase 3  正文提取",
            "llm": "Phase 4  LLM提取",
            "dedup": "Phase 4.3 去重",
            "notion": "Phase 6  Notion",
        }
        name = stage_names.get(stage, stage)
        print(f"  {name}: {json.dumps(info, ensure_ascii=False)}")

    print(f"\n  最终结果: {'✅ 通过' if success else '❌ 失败'}")

    # JSON 报告导出
    out_file = Path(__file__).parent / "e2e_report.json"
    out_file.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    print(f"  详细报告: {out_file}")


if __name__ == "__main__":
    main()