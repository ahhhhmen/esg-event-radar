"""
ICS 日历文件生成器 (Phase 7)
───────────────────────────
从 EventItem 列表生成 .ics 日历订阅文件。
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

from icalendar import Calendar, Event as ICalEvent

from schemas.event import EventItem

logger = logging.getLogger("event_radar.ics")


def _parse_date(date_str: str) -> Optional[date]:
    """将 YYYY-MM-DD 字符串转为 date 对象。"""
    if not date_str or date_str in ("unknown", ""):
        return None
    try:
        return date.fromisoformat(date_str)
    except (ValueError, TypeError):
        return None


def _build_description(ev: EventItem) -> str:
    """构建 ICS 事件的描述文本。"""
    parts = [
        f"主办方: {ev.organizer}",
        f"形式: {ev.format}",
    ]
    if ev.fee_tier:
        parts.append(f"费用: {ev.fee_tier}")
    if ev.agenda_highlights:
        parts.append(f"议程: {ev.agenda_highlights}")
    parts.append(f"高管价值: {'★' * ev.exec_value_score}{'☆' * (5 - ev.exec_value_score)}")
    parts.append(f"评分依据: {ev.exec_value_rationale}")
    if ev.registration_url:
        parts.append(f"报名: {ev.registration_url}")
    parts.append(f"来源: {ev.source_url}")
    return "\n".join(parts)


def write_ics(events: list[EventItem], output_path: Path) -> str | None:
    """将事件列表写入 .ics 文件，返回输出路径，失败返回 None。

    Args:
        events: EventItem 列表
        output_path: .ics 文件输出路径
    """
    cal = Calendar()
    cal.add("prodid", "-//ESG Event Radar//esg-event-radar//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("method", "PUBLISH")
    cal.add("x-wr-calname", "ESG 全球会议日历")
    cal.add("x-wr-caldesc", "自动扫描全球 ESG 与可持续发展会议，高管参会决策参考")

    added = 0
    for ev in events:
        try:
            ical = ICalEvent()
            ical.add("summary", ev.name)
            ical.add("uid", f"{ev.event_id}@esg-event-radar")

            dt_start = _parse_date(ev.start_date)
            dt_end = _parse_date(ev.end_date) if ev.end_date != ev.start_date else dt_start

            if dt_start is None:
                ical.add("description", _build_description(ev))
                cal.add_component(ical)
                added += 1
                continue

            ical.add("dtstart", dt_start)
            ical.add("dtend", dt_end)

            location_parts = [p for p in [ev.city, ev.country, ev.venue] if p]
            if location_parts:
                ical.add("location", ", ".join(location_parts))

            ical.add("description", _build_description(ev))

            if ev.registration_url and "news.google.com" not in ev.registration_url:
                ical.add("url", ev.registration_url)

            if ev.topics:
                ical.add("categories", [t[:50] for t in ev.topics[:5]])

            cal.add_component(ical)
            added += 1
        except Exception as exc:
            logger.debug(f"[ICS] 事件 {ev.name[:40]} 写入失败: {exc}")

    try:
        output_path.write_bytes(cal.to_ical())
        logger.info(f"[ICS] 生成完成: {added}/{len(events)} 个事件 → {output_path}")
        return str(output_path)
    except Exception as exc:
        logger.error(f"[ICS] 写入失败: {exc}")
        return None
