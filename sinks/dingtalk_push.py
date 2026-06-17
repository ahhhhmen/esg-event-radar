"""
钉钉机器人推送模块 (Phase 8)
──────────────────────────
向钉钉群发送 Top 10 ESG 活动预警卡片。
"""

import json
import logging
import os

import requests

from schemas.event import EventItem

logger = logging.getLogger("event_radar.dingtalk")


def push_to_dingtalk(events: list[EventItem], webhook_url: str,
                     notion_database_id: str, run_ts: str) -> None:
    """将 Top 10 评分最高的事件推送到钉钉群。

    Args:
        events: 事件列表（按评分降序预先排序）
        webhook_url: 钉钉机器人 Webhook URL
        notion_database_id: Notion 数据库 ID（用于生成链接）
        run_ts: 本次运行时间戳
    """
    if not webhook_url:
        logger.info("[DingTalk] Webhook 未配置，跳过推送。")
        return
    if not events:
        logger.info("[DingTalk] 无事件，静默阻断推送。")
        return

    import re
    from urllib.parse import urlparse

    # Google News URL 解密（仅针对入选事件，控制请求量）
    def resolve_news_url(url: str, timeout: int = 6) -> str:
        if not url or "news.google.com" not in url:
            return url
        try:
            resp = requests.head(url, timeout=timeout,
                                 headers={"User-Agent": "Mozilla/5.0"},
                                 allow_redirects=True)
            final = resp.url
            if final != url and "google.com" not in final and len(final) > 15:
                return final
        except Exception:
            pass
        return url

    top_events = sorted(events, key=lambda e: e.exec_value_score, reverse=True)[:10]

    for ev in top_events:
        if ev.registration_url and "news.google.com" in ev.registration_url:
            ev.registration_url = resolve_news_url(ev.registration_url)
        if "news.google.com" in ev.source_url:
            ev.source_url = resolve_news_url(ev.source_url)

    lines = [f"## 🌍 ESG 全球会议预警周报 ({run_ts[:10]})\n"]
    lines.append(f"> 本周扫描 {len(events)} 场活动，以下为高管优先关注清单：\n")

    for i, ev in enumerate(top_events, 1):
        date_range = ev.start_date if ev.start_date == ev.end_date else f"{ev.start_date} ~ {ev.end_date}"
        location = ev.city or ev.format
        reg_link = f"[报名]({ev.registration_url})" if ev.registration_url else "链接待确认"
        score_stars = "★" * ev.exec_value_score + "☆" * (5 - ev.exec_value_score)

        lines.append(
            f"**{i}. {ev.name}**  \n"
            f"📅 {date_range} | 📍 {location} | 🏛 {ev.organizer}  \n"
            f"⭐ 高管价值: {score_stars} | {ev.exec_value_rationale}  \n"
            f"🔗 {reg_link}\n"
        )

    notion_link = (
        f"https://www.notion.so/{notion_database_id.replace('-', '')}"
        if notion_database_id else "#"
    )
    lines.append(f"\n> 📓 完整活动库: [Notion 数据库]({notion_link})")

    content = "\n".join(lines)
    # 钉钉 markdown 消息限 20000 字节，按事件边界截断（避免切断 markdown 结构）
    max_bytes = 19000
    if len(content.encode("utf-8")) > max_bytes:
        truncated_lines = [lines[0], lines[1]]  # 保留标题和概要
        byte_count = sum(len(l.encode("utf-8")) for l in truncated_lines) + 2
        event_count = 0
        for line in lines:
            if not line.startswith("**"):
                continue
            line_bytes = len(line.encode("utf-8")) + 1  # +1 for \n
            if byte_count + line_bytes > max_bytes - 200:  # 预留截断提示空间
                break
            truncated_lines.append(line)
            byte_count += line_bytes
            event_count += 1
        truncated_lines.append(
            f"\n> ⚠️ 仅展示 Top {event_count}/{len(events)} 个活动。完整列表见 Notion。"
        )
        content = "\n".join(truncated_lines)

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": f"ESG 会议预警 {run_ts[:10]}",
            "text": content,
        },
    }
    try:
        resp = requests.post(
            webhook_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=10,
        )
        logger.info(f"[DingTalk] 推送完成: {resp.text[:100]}")
    except Exception as exc:
        logger.error(f"[DingTalk] 推送失败: {exc}")
