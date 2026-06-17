"""
Notion 数据库同步模块 (Phase 6)
─────────────────────────────
从 EventRadarAgent 提取，封装 Notion API 交互：
- 自动创建/补全数据库属性
- 基于 event_id 的 Upsert（创建/更新/缓存去重）
- 流量整形 + tenacity 指数退避
"""

import logging
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from schemas.event import EventItem

logger = logging.getLogger("event_radar.notion")

# ── 常量 ─────────────────────────────────────────────
NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

# 需要在数据库中创建的属性（title 属性"Name"由 Notion 自动创建）
NOTION_REQUIRED_PROPS: dict = {
    "Event ID":            {"rich_text": {}},
    "Original Name":       {"rich_text": {}},
    "Organizer":           {"rich_text": {}},
    "Start Date":          {"date": {}},
    "End Date":            {"date": {}},
    "Format":              {"select": {"options": [
        {"name": "in-person", "color": "green"},
        {"name": "online", "color": "blue"},
        {"name": "hybrid", "color": "orange"},
        {"name": "unknown", "color": "gray"},
    ]}},
    "City":                {"rich_text": {}},
    "Country":             {"rich_text": {}},
    "Registration URL":    {"url": {}},
    "Fee Tier":            {"select": {"options": [
        {"name": "free", "color": "green"},
        {"name": "paid", "color": "yellow"},
        {"name": "invite-only", "color": "red"},
        {"name": "unknown", "color": "gray"},
    ]}},
    "Topics":              {"multi_select": {}},
    "Audience":            {"multi_select": {}},
    "Exec Value Score":    {"number": {"format": "number"}},
    "Exec Value Rationale":{"rich_text": {}},
    "Agenda Highlights":   {"rich_text": {}},
    "Source URL":          {"url": {}},
    "Source Org":          {"rich_text": {}},
    "Discovered At":       {"date": {}},
}

# 速率限制：3 req/s
BASE_INTERVAL = 0.6
MAX_RETRIES = 5
TRAFFIC_SHAPE_DELAY = 0.35


class NotionAPIError(Exception):
    """Notion API 返回非 2xx 状态码时抛出。"""
    def __init__(self, status_code: int, message: str, response_text: str = ""):
        self.status_code = status_code
        self.message = message
        self.response_text = response_text
        super().__init__(f"HTTP {status_code}: {message}")


class NotionSink:
    """Notion 数据库同步器。"""

    def __init__(self, token: str, database_id: str):
        self._token = token
        self._db_id = database_id

    # ── 内部工具 ─────────────────────
    @staticmethod
    def _headers(token: str) -> dict:
        return {
            "Authorization": f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }

    @classmethod
    def _retry_on_rate_limit(cls, func):
        """tenacity 装饰器：指数退避 + 随机抖动，专门处理 Notion 429/5xx。"""
        def _is_retryable(exception: BaseException) -> bool:
            if isinstance(exception, NotionAPIError):
                return exception.status_code in (429, 500, 502, 503, 504)
            if isinstance(exception, requests.RequestException):
                return True
            return False

        return retry(
            wait=wait_random_exponential(multiplier=0.6, max=10),
            stop=stop_after_attempt(MAX_RETRIES),
            retry=retry_if_exception_type((NotionAPIError, requests.RequestException)),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )(func)

    @staticmethod
    def _build_page_props(ev: EventItem, title_prop_name: str = "Name") -> dict:
        """将 EventItem 映射到 Notion page properties dict。"""
        def txt(val: str) -> dict:
            return {"rich_text": [{"text": {"content": str(val or "")[:2000]}}]}

        def dt(val: str) -> dict:
            if not val or val == "unknown":
                return {"date": None}
            return {"date": {"start": val[:10]}}

        def safe_url(val: Optional[str]) -> dict:
            if not val or "news.google.com" in val:
                return {"url": None}
            return {"url": val[:2000]}

        notion_title = ev.display_name_zh or ev.name

        props: dict = {
            title_prop_name:        {"title": [{"text": {"content": notion_title[:200]}}]},
            "Event ID":             txt(ev.event_id),
            "Original Name":        txt(ev.original_name),
            "Organizer":            txt(ev.organizer),
            "Format":               {"select": {"name": ev.format}},
            "City":                 txt(ev.city or ""),
            "Country":              txt(ev.country or ""),
            "Fee Tier":             {"select": {"name": ev.fee_tier}},
            "Topics":               {"multi_select": [{"name": t[:100]} for t in (ev.topics or [])[:10]]},
            "Audience":             {"multi_select": [{"name": a[:100]} for a in (ev.audience or [])[:10]]},
            "Exec Value Score":     {"number": ev.exec_value_score},
            "Exec Value Rationale": txt(ev.exec_value_rationale),
            "Agenda Highlights":    txt(ev.agenda_highlights),
            "Source Org":           txt(ev.source_org),
            "Registration URL":     safe_url(ev.registration_url),
            "Source URL":           safe_url(ev.source_url),
        }

        if ev.start_date and ev.start_date not in ("unknown", ""):
            props["Start Date"] = dt(ev.start_date)
        if ev.end_date and ev.end_date not in ("unknown", ""):
            props["End Date"] = dt(ev.end_date)
        if ev.discovered_at and ev.discovered_at not in ("unknown", ""):
            props["Discovered At"] = dt(ev.discovered_at)
        return props

    # ── 数据库属性确保 ──────────────
    def _ensure_db_properties(self, headers: dict) -> Optional[str]:
        """检查并补全数据库属性；返回 title 属性名，失败返回 None。"""
        r = requests.get(f"{NOTION_API}/databases/{self._db_id}", headers=headers, timeout=15)
        if r.status_code != 200:
            logger.warning(f"[Notion] 读取数据库失败 HTTP {r.status_code}: {r.text[:300]}")
            return None

        body = r.json()
        properties = body.get("properties", {})

        title_prop_name = "Name"
        for prop_name, prop_info in properties.items():
            if prop_info.get("type") == "title":
                title_prop_name = prop_name
                logger.info(f"[Notion] 检测到 title 属性: '{title_prop_name}'")
                break

        existing = set(properties.keys())
        to_add = {k: v for k, v in NOTION_REQUIRED_PROPS.items() if k not in existing}
        if not to_add:
            return title_prop_name

        logger.info(f"[Notion] 补全缺失字段: {list(to_add.keys())}")
        pr = requests.patch(
            f"{NOTION_API}/databases/{self._db_id}",
            headers=headers,
            json={"properties": to_add},
            timeout=15,
        )
        if pr.status_code not in (200, 201):
            logger.warning(f"[Notion] 字段创建失败 HTTP {pr.status_code}: {pr.text[:300]}")
            return None
        return title_prop_name

    # ── 主入口 ──────────────────────
    def upsert(self, events: list[EventItem], run_ts: str) -> None:
        """将事件列表 Upsert 到 Notion 数据库。"""
        headers = self._headers(self._token)
        logger.info(
            f"[Notion] Upsert: {len(events)} 个事件 "
            f"(并发×3, 间隔 {BASE_INTERVAL}s + 主动整形 {TRAFFIC_SHAPE_DELAY}s)"
        )

        title_prop_name = self._ensure_db_properties(headers)
        if title_prop_name is None:
            logger.error("[Notion] 数据库初始化失败，跳过 Upsert。")
            return

        # ── 线程安全的主动流量整形锁 ──
        rate_lock = threading.Lock()
        last_request_time = [0.0]

        def _api_request(method: str, url: str, **kwargs) -> requests.Response:
            with rate_lock:
                jitter = BASE_INTERVAL * 0.15 * (random.random() * 2 - 1)
                interval = max(0.1, BASE_INTERVAL + jitter)
                elapsed = time.monotonic() - last_request_time[0]
                if elapsed < interval:
                    time.sleep(interval - elapsed)
                last_request_time[0] = time.monotonic()

            resp = requests.request(method, url, headers=headers, timeout=15, **kwargs)

            if resp.status_code == 429:
                raise NotionAPIError(429, "Rate limited", resp.text[:300])
            if resp.status_code >= 500:
                raise NotionAPIError(resp.status_code, "Server error", resp.text[:300])
            return resp

        _api_request = self._retry_on_rate_limit(_api_request)

        # ── 内存级防刷缓存 ──
        local_page_cache: dict[str, str] = {}
        cache_lock = threading.Lock()

        def _upsert_one(ev: EventItem) -> str:
            try:
                page_props = self._build_page_props(ev, title_prop_name)

                # 步骤 0: 检查本地缓存
                with cache_lock:
                    cached_page_id = local_page_cache.get(ev.event_id)
                if cached_page_id:
                    try:
                        get_resp = _api_request("GET", f"{NOTION_API}/pages/{cached_page_id}")
                        time.sleep(TRAFFIC_SHAPE_DELAY)
                        if get_resp.status_code == 200:
                            existing_page = get_resp.json()
                            existing_agenda_prop = existing_page.get("properties", {}).get("Agenda Highlights", {})
                            existing_texts = []
                            for rt in existing_agenda_prop.get("rich_text", []):
                                txt = rt.get("plain_text", "")
                                if txt.strip():
                                    existing_texts.append(txt)
                            old_agenda = "; ".join(existing_texts)
                            new_agenda = ev.agenda_highlights.strip() if ev.agenda_highlights else ""
                            if new_agenda and old_agenda and new_agenda not in old_agenda:
                                merged_agenda = f"{old_agenda}\n\n[更新 {run_ts[:10]}] {new_agenda}"[:2000]
                                page_props["Agenda Highlights"] = {
                                    "rich_text": [{"text": {"content": merged_agenda}}]
                                }
                    except Exception:
                        pass

                    ur = _api_request("PATCH", f"{NOTION_API}/pages/{cached_page_id}",
                                      json={"properties": page_props})
                    time.sleep(TRAFFIC_SHAPE_DELAY)
                    if ur.status_code not in (200, 201):
                        logger.warning(f"  Notion 更新失败(缓存) HTTP {ur.status_code}: {ur.text[:200]}")
                        return "failed"
                    logger.debug(f"  更新(缓存): {ev.name}")
                    return "updated"

                # 步骤 1: 查询
                qr = _api_request("POST", f"{NOTION_API}/databases/{self._db_id}/query",
                                  json={"filter": {"property": "Event ID", "rich_text": {"equals": ev.event_id}}})
                time.sleep(TRAFFIC_SHAPE_DELAY)

                if qr.status_code != 200:
                    logger.warning(f"  Notion 查询失败 HTTP {qr.status_code}: {qr.text[:200]}")
                    return "failed"

                results = qr.json().get("results", [])

                if results:
                    page_id = results[0]["id"]
                    with cache_lock:
                        local_page_cache[ev.event_id] = page_id

                    existing_page = results[0]
                    existing_agenda_prop = existing_page.get("properties", {}).get("Agenda Highlights", {})
                    existing_texts = [rt.get("plain_text", "").strip()
                                     for rt in existing_agenda_prop.get("rich_text", [])
                                     if rt.get("plain_text", "").strip()]
                    old_agenda = "; ".join(existing_texts)
                    new_agenda = ev.agenda_highlights.strip() if ev.agenda_highlights else ""
                    if new_agenda and old_agenda and new_agenda not in old_agenda:
                        merged_agenda = f"{old_agenda}\n\n[更新 {run_ts[:10]}] {new_agenda}"[:2000]
                        page_props["Agenda Highlights"] = {"rich_text": [{"text": {"content": merged_agenda}}]}

                    ur = _api_request("PATCH", f"{NOTION_API}/pages/{page_id}",
                                      json={"properties": page_props})
                    time.sleep(TRAFFIC_SHAPE_DELAY)
                    if ur.status_code not in (200, 201):
                        logger.warning(f"  Notion 更新失败 HTTP {ur.status_code}: {ur.text[:200]}")
                        return "failed"
                    logger.debug(f"  更新: {ev.name}")
                    return "updated"
                else:
                    cr = _api_request("POST", f"{NOTION_API}/pages",
                                      json={"parent": {"database_id": self._db_id}, "properties": page_props})
                    time.sleep(TRAFFIC_SHAPE_DELAY)
                    if cr.status_code not in (200, 201):
                        logger.warning(f"  Notion 创建失败 HTTP {cr.status_code}: {cr.text[:200]}")
                        return "failed"
                    new_page_id = cr.json().get("id", "")
                    if new_page_id:
                        with cache_lock:
                            local_page_cache[ev.event_id] = new_page_id
                    logger.debug(f"  新建: {ev.name}")
                    return "created"
            except NotionAPIError as exc:
                logger.warning(f"  Upsert 重试耗尽 [HTTP {exc.status_code}]: {exc.message}")
                return "failed"
            except Exception as exc:
                logger.warning(f"  Upsert 异常 [{ev.name[:40]}]: {exc}")
                return "failed"

        created = updated = failed = 0
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(_upsert_one, ev): ev for ev in events}
            for fut in as_completed(futures):
                result = fut.result()
                if result == "created":
                    created += 1
                elif result == "updated":
                    updated += 1
                else:
                    failed += 1

        logger.info(f"[Notion] Upsert 完成: 新建 {created}, 更新 {updated}, 失败 {failed}")
