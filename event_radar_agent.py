#!/usr/bin/env python3
"""
全球 ESG 与可持续发展会议/活动动态预警系统 (Event Radar) v1.3
══════════════════════════════════════════════════════════════════════════════
架构层级
─────────
  EventRadarAgent.run()
    Phase 0  : 加载供料源清单（sources.yaml）
    Phase 1a : HTML Calendar 爬虫（轨道 A — Tier 1 组织官网 Events 页）
    Phase 1b : 三轨 RSS 抓取（轨道 B 行业媒体 + 轨道 C Google News）
    Phase 2  : 去重（URL + 标题双键）+ 日期窗口过滤
    Phase 3  : 深度正文提取（ContentExtractor + 三级回退）
    Phase 4  : LLM 语义抽取 → EventItem JSON Array
               （基于 scoring_criteria.md 五维评分锚点）
    Phase 4.3: 跨源/跨语言事件去重（名称相似度 + 日期匹配）
    Phase 4.5: 过期事件过滤（>14 天前）
    Phase 5  : 零数据熔断 / 无事件静默阻断
    Phase 6  : Notion Database Upsert（全量写入，自动建字段）
    Phase 7  : .ics 日历订阅文件生成
    Phase 8  : 钉钉群精简卡片推送（Top 10 按评分排序）

v1.3 变更:
  · Phase 4.3 跨源/跨语言去重：按日期+名称关键词匹配，合并同一活动
  · LLM prompt 强制输出中文活动名称（非中文名称翻译为中文）
  · Notion Upsert 增加 HTTP 状态码检查和详细错误日志
  · 修复 _upsert_one 中未检查 API 响应状态码的问题
══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import hashlib
import html
import json
import logging
import os
import random
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime as email_parse_date
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

import feedparser
import requests
import yaml
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from icalendar import Calendar, Event as ICalEvent, vText
from radar_infra.llm import (
    DeepSeekProvider, CachedLLMClient, create_llm_retry_decorator,
)
from radar_infra.support import RunMetrics, MetricsStore

load_dotenv()

from schemas.event import EventItem

# 交付层模块 (Phase 6-8)
from sinks.notion_sync import NotionSink
from sinks.ics_writer import write_ics as _ics_write
from sinks.dingtalk_push import push_to_dingtalk as _dingtalk_push

# ─────────────────────────────────────────────────────────
# 日志
# ─────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("event_radar")

# ─────────────────────────────────────────────────────────
# 常量
# ─────────────────────────────────────────────────────────

FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

SOURCES_PATH = Path(__file__).parent / "sources.yaml"
SCORING_CRITERIA_PATH = Path(__file__).parent / "scoring_criteria.md"
_OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", str(Path(__file__).parent)))
OUTPUT_ICS_PATH = _OUTPUT_DIR / "esg_events.ics"

# 钉钉系统报警 Webhook — 仅在 DINGTALK_WEBHOOK_RADAR 环境变量有值时生效
DINGTALK_WEBHOOK_URL: str = os.environ.get("DINGTALK_WEBHOOK_RADAR", "")

DEEPSEEK_MODEL = "deepseek-chat"

# LLM batch 窗口：每批最多多少条 RSS 摘要送 LLM 处理
LLM_BATCH_SIZE = 15

# 每次扫描窗口（天）
# RSS 新闻源：8 天（周频运行的滚动窗口）
FETCH_WINDOW_DAYS_RSS = 8
# Google News 查询：90 天（会议公告提前数月发布，不能用短窗口截断）
FETCH_WINDOW_DAYS_GNEWS = 90


# ─────────────────────────────────────────────────────────
# 工具函数（继承自基座）
# ─────────────────────────────────────────────────────────

def strip_html(raw: str) -> str:
    if not raw:
        return ""
    unescaped = html.unescape(raw)
    try:
        text = BeautifulSoup(unescaped, "html.parser").get_text(separator=" ", strip=True)
    except Exception:
        text = re.sub(r"<[^>]+>", "", unescaped)
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_rss_date(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        return email_parse_date(date_str)
    except Exception:
        try:
            return datetime.strptime(date_str[:25], "%a, %d %b %Y %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        except Exception:
            return None


def resolve_news_url(url: str, timeout: int = 6) -> str:
    """将 Google News 重定向链接还原为源站直链（继承自基座 v3 策略）。"""
    if not url or "news.google.com" not in url:
        return url
    try:
        resp = requests.head(
            url, headers=FETCH_HEADERS, allow_redirects=True, timeout=timeout
        )
        final = resp.url
        if (
            final != url
            and "google.com" not in final
            and len(final) > 15
            and final.lower().startswith("http")
        ):
            return final
    except Exception as exc:
        logger.debug(f"URL resolution failed [{url[:60]}]: {exc}")
    return url


def make_event_id(name: str, start_date: str) -> str:
    """SHA256[:12] 作为 Notion Upsert 去重主键（v1 兼容，内部使用）。"""
    raw = f"{name.strip().lower()}|{start_date.strip()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def normalize_event_name(name: str) -> str:
    """标准化事件名称用于去重匹配。
    
    去除标点、转为小写、移除常见后缀词（如 Conference, Summit, Forum 等变体）。
    """
    if not name:
        return ""
    # 转小写
    n = name.lower().strip()
    # 移除括号内容
    n = re.sub(r"\([^)]*\)", "", n)
    # 移除常见后缀词（英文 + 中文 + 印尼语 + 法语）
    suffix_words = [
        "conference", "summit", "forum", "meeting", "event", "workshop",
        "webinar", "symposium", "congress", "assembly", "seminar", "expo",
        "roundtable", "dialogue", "session", "annual", "international",
        "global", "world", "week", "day", "series",
        "会议", "峰会", "论坛", "研讨会", "大会", "活动",
        "konferensi", "pertemuan", "lokakarya",
        "conférence", "sommet", "forum", "événement", "colloque",
        "2024", "2025", "2026", "2027", "2028",
    ]
    for w in sorted(suffix_words, key=len, reverse=True):
        n = re.sub(rf"\b{re.escape(w)}\b", "", n)
    # 移除标点和多余空格
    n = re.sub(r"[^\w\s]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def name_similarity(a: str, b: str) -> float:
    """计算两个标准化事件名称的相似度 (0.0 ~ 1.0)。"""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


# ─────────────────────────────────────────────────────────
# RSS 抓取器（v2：feedparser 解析，捕获 source.href 供 Google News 溯源）
# ─────────────────────────────────────────────────────────

class NewsFetcher:
    """垂直域 RSS 抓取器。v2 改用 feedparser，正确解析 source url 属性。"""

    TIMEOUT = 20
    MAX_RESULTS = 20

    @classmethod
    def fetch(cls, url: str, resolve_google: bool = False) -> list[dict]:
        articles: list[dict] = []
        try:
            # 先用 requests 带 timeout 获取内容，避免 feedparser.parse(url) 无超时挂起
            resp = requests.get(
                url, headers=FETCH_HEADERS, timeout=cls.TIMEOUT, allow_redirects=True
            )
            resp.raise_for_status()
            feed = feedparser.parse(resp.text)
            if feed.bozo and not feed.entries:
                # bozo 为非致命错误（如非标准 XML），有 entries 仍可使用
                logger.debug(f"RSS parse warning [{url[:60]}]: {feed.bozo_exception}")
            for entry in feed.entries[: cls.MAX_RESULTS]:
                parsed = cls._parse_entry(entry)
                if parsed is None:
                    continue
                # 清洗 description（保留 HTML summary 作为 fallback 正文来源）
                parsed["description"] = strip_html(parsed.get("description", ""))
                if resolve_google:
                    parsed["url"] = resolve_news_url(parsed["url"])
                articles.append(parsed)
        except Exception as exc:
            logger.debug(f"RSS fetch failed [{url[:60]}]: {exc}")
        return articles

    @staticmethod
    def _parse_entry(entry) -> Optional[dict]:
        title = (entry.get("title") or "").strip()
        link = (entry.get("link") or "").strip()
        if not title or not link:
            return None

        # 提取 source 名称与域名（feedparser 正确解析 <source url="..."> 属性）
        source_href = None
        source_name = "Unknown"
        src = entry.get("source")
        if src:
            source_name = (src.get("title") or source_name).strip()
            source_href = (src.get("href") or "").strip()

        # 提取 summary 作为 fallback 正文
        summary_html = entry.get("summary") or ""

        return {
            "title":       title,
            "date":        entry.get("published") or "",
            "source":      source_name,
            "source_href": source_href or "",     # 新增：源站域名，供正文提取失败时作为上下文
            "url":         link,
            "description": summary_html,           # 保留 HTML summary，后续 strip
        }


# ─────────────────────────────────────────────────────────
# 内容提取器（继承自基座，原样保留）
# ─────────────────────────────────────────────────────────

class ContentExtractor:
    """向原始 URL 发送 GET，提取正文前 300 字纯文本（会议页正文比新闻短，适当放宽）。"""

    TIMEOUT = 5  # 正文提取超时（秒）；失败时有 RSS 摘要兜底，不影响主线
    MAX_CHARS = 300
    _SEMANTIC_RE = re.compile(r"article|content|post|story|body|main|event", re.I)

    @classmethod
    def extract(cls, url: str) -> str:
        try:
            resp = requests.get(
                url, headers=FETCH_HEADERS, timeout=cls.TIMEOUT, allow_redirects=True
            )
            if resp.status_code != 200:
                return ""
            return cls._parse_body(resp.text)
        except Exception as exc:
            logger.debug(f"ContentExtractor failed [{url[:70]}]: {exc}")
            return ""

    @classmethod
    def _parse_body(cls, html_text: str) -> str:
        soup = BeautifulSoup(html_text, "html.parser")
        for noise in soup.find_all(
            ["script", "style", "nav", "footer", "header", "aside", "noscript"]
        ):
            noise.decompose()
        container = (
            soup.find("article")
            or soup.find(["div", "section", "main"], class_=cls._SEMANTIC_RE)
            or soup.find(["div", "section", "main"], id=cls._SEMANTIC_RE)
            or soup
        )
        paragraphs = container.find_all("p")
        texts: list[str] = []
        total = 0
        for p in paragraphs:
            t = re.sub(r"\s+", " ", p.get_text(separator=" ", strip=True)).strip()
            if len(t) > 15:
                texts.append(t)
                total += len(t)
            if total >= cls.MAX_CHARS:
                break
        return " ".join(texts)[: cls.MAX_CHARS]


# ─────────────────────────────────────────────────────────
# 主智能体
# ─────────────────────────────────────────────────────────

class EventRadarAgent:
    """全球 ESG 会议/活动动态预警主控。"""

    def __init__(self) -> None:
        self._sources: list[dict] = []
        self._raw_items: list[dict] = []          # Phase 1 抓取结果
        self._seen_urls: set[str] = set()
        self._seen_titles: set[str] = set()
        self._events: list[EventItem] = []        # Phase 4 LLM 提取结果
        self._run_ts: str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        api_key = os.environ.get("DEEPSEEK_API_KEY", "")
        if not api_key:
            logger.warning("DEEPSEEK_API_KEY 未设置，LLM 提取阶段将跳过。")
            self._llm = None
        else:
            provider = DeepSeekProvider(api_key=api_key)
            self._llm = CachedLLMClient(provider)

        self._scoring_criteria: str = ""
        if SCORING_CRITERIA_PATH.exists():
            self._scoring_criteria = SCORING_CRITERIA_PATH.read_text(encoding="utf-8")

    # ── 系统级 FATAL 报警（继承自基座） ──────────────────

    @staticmethod
    def _send_system_alert(message: str) -> None:
        """向钉钉 Webhook 发送 FATAL 级系统报警；Webhook 未配置时仅打印 ERROR 日志。"""
        logger.error(f"[SYSTEM_ALERT] {message}")
        if not DINGTALK_WEBHOOK_URL:
            return
        try:
            payload = {"msgtype": "text", "text": {"content": message}}
            resp = requests.post(
                DINGTALK_WEBHOOK_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=10,
            )
            logger.info(f"[SYSTEM_ALERT] 钉钉报警已发送，响应: {resp.status_code}")
        except Exception as exc:
            logger.error(f"[SYSTEM_ALERT] 钉钉报警发送失败: {exc}")

    # ── Phase 0: 加载供料源 ───────────────────────────────

    def _load_sources(self) -> None:
        with open(SOURCES_PATH, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        self._sources = [s for s in data.get("sources", []) if s.get("active", True)]
        logger.info(f"[源清单] 已加载 {len(self._sources)} 条活跃源")

    # ── Phase 1a: HTML Calendar 爬虫（轨道 A — Tier 1 组织官网 Events 页）──

    def _fetch_html_calendars(self) -> None:
        """爬取 sources.yaml 中 type=html_calendar 的 Tier 1 组织官网活动页。
        
        策略：
        - 向官网 Events 页发送 GET，用 BeautifulSoup 提取活动列表
        - 各组织页面结构不同，使用通用选择器（时间 + 链接 + 标题）
        - 将提取结果注入 self._raw_items，与 RSS 条目混合供 LLM 处理
        """
        html_sources = [s for s in self._sources if s.get("type") == "html_calendar"]
        if not html_sources:
            logger.info("[Phase 1b] 无活跃 html_calendar 源，跳过爬虫。")
            return

        logger.info(f"[Phase 1b] HTML Calendar 爬虫: {len(html_sources)} 个源")

        def _crawl_one(idx: int, src: dict) -> tuple[int, int, list[dict]]:
            """返回 (index, injected_count, raw_items)"""
            url = src["url"]
            org = src.get("org", src["name"])
            tags = src.get("tags", [])
            results: list[dict] = []

            try:
                resp = requests.get(
                    url, headers=FETCH_HEADERS, timeout=15, allow_redirects=True
                )
                if resp.status_code != 200:
                    logger.warning(f"  [{idx}] {org}: HTTP {resp.status_code}")
                    return (idx, 0, [])

                soup = BeautifulSoup(resp.text, "html.parser")

                # 策略 1: 结构化 event list（article / li 含 link）
                event_candidates = []
                for selector in [
                    "article", "li.event", "div.event-item", ".event-listing",
                    "li.has-link", "div[class*='event']", "li[class*='event']",
                    "tr td a[href*='event']", ".views-row",
                ]:
                    event_candidates = soup.select(selector)
                    if event_candidates:
                        break

                # 策略 2: fallback — 查找所有带链接的标题/列表项
                if not event_candidates:
                    event_candidates = soup.select("li a[href], h2 a[href], h3 a[href]")

                for elem in event_candidates[:20]:
                    link = elem.find("a") if elem.name != "a" else elem
                    if link is None:
                        continue
                    href_raw = link.get("href", "")
                    href = str(href_raw) if href_raw else ""
                    if not href:
                        continue

                    # 构造绝对 URL
                    if href.startswith("/"):
                        parsed_base = urlparse(url)
                        href = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
                    elif not href.startswith("http"):
                        continue

                    title = link.get_text(separator=" ", strip=True)
                    if not title or len(title) < 5:
                        continue

                    # 尝试提取日期文本（同一父节点或兄弟元素中的 time/date 标签）
                    parent = elem if elem.name != "a" else elem.parent
                    if parent is None:
                        parent = elem
                    date_text = ""
                    for tag_name, class_hint in [
                        ("time", "date"), ("span", "date"), ("div", "date"),
                        ("time", None), ("span", "meta"),
                    ]:
                        if class_hint:
                            el = parent.find(tag_name, class_=re.compile(r"\b" + re.escape(class_hint) + r"\b", re.I))
                        else:
                            el = parent.find(tag_name)
                        if el:
                            dt_attr = el.get("datetime", "")
                            date_text = dt_attr if dt_attr else el.get_text(separator=" ", strip=True)
                            break

                    results.append({
                        "title": title,
                        "date": date_text or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        "source": org,
                        "source_href": urlparse(url).netloc,
                        "url": href,
                        "description": f"HTML Calendar 爬取 | 来源: {org} | {date_text}",
                        "source_org": org,
                        "source_tags": tags,
                        "source_tier": src.get("tier", 1),
                        "parsed_date": parse_rss_date(str(date_text)) if date_text else datetime.now(timezone.utc),
                        "_is_google_news": False,
                    })

            except Exception as exc:
                logger.debug(f"  [{idx}] {org} 爬取异常: {exc}")

            return (idx, len(results), results)

        with ThreadPoolExecutor(max_workers=6) as pool:
            futures_map = {
                pool.submit(_crawl_one, idx, src): idx
                for idx, src in enumerate(html_sources, 1)
            }
            import threading
            lock = threading.Lock()
            for fut in as_completed(futures_map):
                try:
                    idx, injected, results = fut.result()
                    total_injected = 0
                    with lock:
                        for item in results:
                            if item["url"] in self._seen_urls:
                                continue
                            title_key = item["title"].strip().lower()
                            if title_key in self._seen_titles:
                                continue
                            self._seen_urls.add(item["url"])
                            self._seen_titles.add(title_key)
                            self._raw_items.append(item)
                            total_injected += 1
                    logger.info(
                        f"  [cal {idx:>2}/{len(html_sources)}] 抓取 {injected} 条 → "
                        f"去重后注入 {total_injected} 条 | {html_sources[idx-1]['name']}"
                    )
                except Exception as exc:
                    src_idx = futures_map[fut]
                    logger.debug(f"  html_calendar 源 {src_idx} 异常: {exc}")

        html_count = sum(
            1 for it in self._raw_items
            if it.get("description", "").startswith("HTML Calendar")
        )
        logger.info(f"[Phase 1b] HTML Calendar 完成。注入 {html_count} 条原始条目")

    # ── Phase 1: 垂直 RSS 抓取（并发） ─────────────────────

    def _fetch_all_sources(self) -> None:
        from datetime import timedelta
        cutoff_rss    = datetime.now(timezone.utc) - timedelta(days=FETCH_WINDOW_DAYS_RSS)
        cutoff_gnews  = datetime.now(timezone.utc) - timedelta(days=FETCH_WINDOW_DAYS_GNEWS)

        active = [s for s in self._sources if s.get("active", True)]
        rss_sources = [s for s in active if s.get("type") in ("rss", "google_news")]

        logger.info(f"[Phase 1] 并发抓取 {len(rss_sources)} 个源（max_workers=8）")

        def _fetch_one(idx: int, src: dict) -> tuple[int, int, list[dict]]:
            """返回 (index, injected_count, raw_items)"""
            src_type = src.get("type", "rss")
            url = src["url"]
            raw_items = NewsFetcher.fetch(url, resolve_google=False)

            injected = 0
            results: list[dict] = []
            for item in raw_items:
                item_with_meta = {
                    **item,
                    "source_org": src.get("org", src["name"]),
                    "source_tags": src.get("tags", []),
                    "source_tier": src.get("tier", 3),
                    "parsed_date": parse_rss_date(item["date"]),
                    "_is_google_news": src_type == "google_news",  # 标记来源类型，供日期过滤使用
                }
                results.append(item_with_meta)
                injected += 1

            return (idx, injected, results)

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {
                pool.submit(_fetch_one, idx, src): idx
                for idx, src in enumerate(rss_sources, 1)
            }
            # 按提交顺序收集，但合并时需加锁去重
            import threading
            lock = threading.Lock()
            for fut in as_completed(futures):
                try:
                    idx, injected, results = fut.result()
                    with lock:
                        for item in results:
                            if item["url"] in self._seen_urls:
                                continue
                            title_key = item["title"].strip().lower()
                            if title_key in self._seen_titles:
                                continue
                            # 日期过滤：Google News 条目用 90 天窗口，其他 RSS 用 8 天窗口
                            cutoff = cutoff_gnews if item.get("_is_google_news") else cutoff_rss
                            if item["parsed_date"] and item["parsed_date"] < cutoff:
                                continue
                            self._seen_urls.add(item["url"])
                            self._seen_titles.add(title_key)
                            self._raw_items.append(item)
                    logger.info(
                        f"  [{idx:>2}/{len(rss_sources)}] {injected} 条 | "
                        f"{rss_sources[idx-1]['name']}"
                    )
                except Exception as exc:
                    src_idx = futures[fut]
                    logger.debug(f"  源 {src_idx} 抓取异常: {exc}")

        logger.info(f"[Phase 1] 完成。共收集 {len(self._raw_items)} 条原始条目 (并发)")

    # ── Phase 3: 深度正文提取（并发） ────────────────────────

    def _enrich_content(self) -> None:
        # 区分：Google News 条目正文提取必然失败，直接走回退逻辑
        gnews_items = [it for it in self._raw_items if "news.google.com" in it.get("url", "")]
        real_items  = [it for it in self._raw_items if "news.google.com" not in it.get("url", "")]

        logger.info(
            f"[Phase 3] 正文提取: {len(real_items)} 条原文 + "
            f"{len(gnews_items)} 条 GNews（直接回退）"
        )

        # GNews 条目直接应用回退（无需网络请求）
        self._apply_body_fallback(gnews_items)

        if not real_items:
            return

        # 原文条目并发 GET
        def _fetch_one(item: dict) -> None:
            body = ContentExtractor.extract(item["url"])
            if body:
                item["body"] = body
            else:
                item["body"] = ""  # 标记为空，后续统一 fallback

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(_fetch_one, it): it for it in real_items}
            done = 0
            for fut in as_completed(futures):
                done += 1
                if done % 20 == 0:
                    logger.info(f"  进度: {done}/{len(real_items)}")
                try:
                    fut.result()
                except Exception as exc:
                    logger.debug(f"  正文提取异常: {exc}")

        # 对原文中提取失败的条目也应用回退
        empty_items = [it for it in real_items if not it.get("body")]
        self._apply_body_fallback(empty_items)

    @staticmethod
    def _apply_body_fallback(items: list[dict]) -> None:
        """统一回退：RSS description → source_href 域名线索 → 纯标题"""
        for item in items:
            rss_desc = item.get("description", "")
            if rss_desc and len(rss_desc) > 30:
                item["body"] = f"[来源:RSS摘要] {rss_desc}"[:300]
            elif item.get("source_href"):
                item["body"] = (
                    f"[来源站点: {item['source_href']}] "
                    f"标题: {item.get('title', '')}"
                )[:300]
            else:
                item["body"] = item.get("title", "")[:300]

    # ── Phase 4: LLM 事件结构化提取 ──────────────────────

    def _build_extraction_prompt(self, batch: list[dict]) -> str:
        items_text = "\n\n".join(
            f"[{i+1}] 标题: {it['title']}\n"
            f"    来源域名: {it.get('source_href', '无')}\n"
            f"    来源机构: {it['source_org']}\n"
            f"    发布日期: {it.get('date', '未知')}\n"
            f"    正文摘要: {it.get('body', '')[:250]}"
            for i, it in enumerate(batch)
        )

        return f"""你是一个专业的 ESG 会议/活动信息结构化提取引擎。

## 任务
从以下 RSS 条目中，识别并提取每一条**真实的会议/峰会/研讨会/论坛活动公告**，
输出为 JSON Array，每个对象对应 EventItem schema。

## 筛选规则

### ⛔ 时间约束红线 (CRITICAL TEMPORAL RULES — 必须逐条执行，严禁违反)

**规则 1 — 拒绝过去 (Reject Past Events)**
- 如果新闻原文使用了明确的过去时态描述（如 "at last month's summit"、"recently concluded"、"was held on"、"took place"、"刚刚落幕"、"已闭幕"、"已在…举办"），说明该事件**已经结束**
- 对于已结束的事件：**彻底忽略，不输出到 JSON Array 中**
- 注意：不要被"回顾文章"或"事后报道"所迷惑 — 事后报道 ≠ 活动预告

**规则 2 — 拒绝推测 (No Speculation on Dates)**
- **严禁**将文章的"发稿日期"（published date）当作会议的举办日期
- **严禁**将"研究报告的发布日期"当作会议的举办日期
- **严禁**在新闻只提及年份（如 "2026 年的 GreenBiz 大会"）时自行编造具体月份和日期
- 如果你推测出某个日期，但没有原文直接证据，该日期被视为幻觉，必须丢弃

**规则 3 — 宁缺毋滥 (Default to Unknown)**
- 如果你**无法在原文中找到明确的未来举办日期**，`start_date` **必须填 "unknown"**，绝不允许编造
- 如果新闻只说了 "即将举办"、"coming soon"、"2026 年" 但没有具体日期，start_date = "unknown"
- "unknown" 是合法输出，编造的日期不是

### 通用筛选规则
- **只提取未来将举办或近 14 天内刚结束的活动**（今天是 {datetime.now(timezone.utc).strftime("%Y-%m-%d")}）
- 3 个月以上的历史活动 → 跳过
- 只提取确实公告了具体活动（有日期线索）的条目
- 纯政策新闻、报告发布、人事变动、奖项颁发 → 跳过，不输出
- 若一条 RSS 包含多个活动，可拆分为多个 EventItem

## 高管参会价值评分准则
{self._scoring_criteria}

## 输出 JSON Schema（每个对象字段）
{{
  "event_id": "占位，系统会覆盖",
  "name": "活动展示名称，格式: '英文名称 — 中文名称'（与原 standard_name_en / display_name_zh 对应）",
  "original_name": "活动原文名称（保留源语言），如果源语言是英文/印尼文/法文则保留原文，如果源语言是中文则保留中文原文",
  "standard_name_en": "标准英文名，用于去重。无论原文是什么语言，统一翻译为英文。【实体消解约束 v2】规则1（强制剥离）：移除年份(2024-2028)、频次修饰词(Annual/Biannual/Biennial)、届数(8th/2nd/第X届/annuel)及标点。规则2（白名单保护）：UN/UNGC/SBTi/CDP/COP/WBCSD/GRI/PRI/RBA/IRMA/ISSB/IUCN/UNEP/ILO/WRI/TNC/RMI 是品牌核心，严禁剥离。规则3（前缀剔除）：移除 Reuters/Bloomberg/GreenBiz/S&P Global 等媒体前缀。规则4（反过度泛化）：剥离后若仅剩通用名词(Annual Summit/Leaders Forum/Global Conference/Council Meeting)，必须加回机构简称。Few-Shot: 'Reuters Responsible Business Europe 2026'→'Responsible Business Europe'; 'SBTi Annual Summit 2026'→'SBTi Summit'; 'WBCSD Council Meeting 2025'→'WBCSD Council Meeting'; 'UN Global Compact Leaders Summit 2026'→'UN Global Compact Leaders Summit'",
  "display_name_zh": "中文展示名，用于中文显示。无论原文是什么语言，统一翻译为中文",
  "organizer": "主办方（中文或中英双语）",
  "start_date": "YYYY-MM-DD（无法确定时填 unknown）",
  "end_date": "YYYY-MM-DD",
  "timezone": "IANA timezone 或 UTC",
  "is_recurring": true/false,
  "format": "in-person|online|hybrid|unknown",
  "city": "城市（中文）或 null",
  "country": "ISO alpha-2 或 null",
  "venue": "场馆或 null",
  "registration_url": "报名链接或 null",
  "registration_deadline": "YYYY-MM-DD 或 null",
  "fee_tier": "free|paid|invite-only|unknown",
  "topics": ["主题标签列表"],
  "audience": ["受众标签列表"],
  "agenda_highlights": "议程亮点≤120字（中文）",
  "exec_value_score": 1-5,
  "exec_value_rationale": "评分依据≤80字，引用维度代码如 D1/D2",
  "source_url": "原始 RSS 链接",
  "source_org": "来源机构",
  "discovered_at": "{self._run_ts}",
  "raw_snippet": "原始摘要前100字"
}}

## 语言处理
- 条目可能为英语、中文、印尼语（Bahasa Indonesia）或法语（Français）
- **关键要求：所有输出的 name 字段必须以中文呈现。非中文活动名称必须翻译为中文。推荐格式: "English Name — 中文名称"**
- organizer、city、venue、agenda_highlights 也必须以中文或中英双语输出
- 印尼语关键词参考：konferensi=会议, forum=论坛, pertemangan=会议, keberlanjutan=可持续性
- 法语关键词参考：conférence=会议, sommet=峰会, forum=论坛, événement=活动, développement durable=可持续发展, RSE=企业社会责任(CSR), matières premières=原材料, approvisionnement responsable=负责任采购, taxonomie verte=绿色分类法

## 输出要求
- 仅输出纯 JSON Array，不加 markdown 代码块，不加注释
- 若无任何活动，输出空数组 []
"""

    def _extract_events_with_llm(self) -> None:
        if not self._raw_items:
            return

        api_key = os.environ.get("DEEPSEEK_API_KEY", "")
        if not api_key:
            logger.warning("[Phase 4] DEEPSEEK_API_KEY 未设置，跳过 LLM 提取。")
            return

        batches = [
            self._raw_items[i: i + LLM_BATCH_SIZE]
            for i in range(0, len(self._raw_items), LLM_BATCH_SIZE)
        ]
        logger.info(f"[Phase 4] LLM 提取: {len(self._raw_items)} 条 → {len(batches)} 批 (并发×3)")

        @create_llm_retry_decorator(max_attempts=3)
        def _call_llm_with_retry(prompt: str, batch_idx: int) -> list[dict] | None:
            """带 tenacity 指数退避的 LLM 调用（基于 radar_infra retry）。"""
            raw_json = self._llm.chat_completion(
                task_type="event_extraction",
                model=DEEPSEEK_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=8192,
            )
            if raw_json is None:
                logger.error(f"  批次 {batch_idx} LLM 返回空内容")
                return None
            raw_json = raw_json.strip()
            raw_json = re.sub(r"^```(?:json)?\s*", "", raw_json)
            raw_json = re.sub(r"\s*```$", "", raw_json)

            parsed: list[dict] = json.loads(raw_json)
            if not isinstance(parsed, list):
                logger.error(f"  批次 {batch_idx} LLM 输出非数组: {type(parsed)}")
                return None
            return parsed

        # 将 tenacity 包装后，再包一层手动自愈重试（Schema 级）
        def _call_llm(prompt: str, batch_idx: int, attempt: int = 1) -> list[dict] | None:
            try:
                return _call_llm_with_retry(prompt, batch_idx)
            except Exception as e:
                logger.error(f"  批次 {batch_idx} LLM 调用失败 (attempt {attempt}): {e}")
                time.sleep(3)
                return None

        def _try_pydantic_parse(obj: dict) -> EventItem | None:
            """Layer 1: Pydantic 校验（含字段级宽容归一化，在 schema 的 @field_validator 中完成）。
            成功返回 EventItem，失败返回 None。"""
            try:
                return EventItem(**obj)
            except Exception:
                return None

        def _degraded_event(obj: dict, reason: str) -> EventItem | None:
            """Layer 2: 确定性降级链路 — 用 MD5 生成 event_id，标记 is_degraded。"""
            try:
                std_en = (obj.get("standard_name_en") or obj.get("name") or "").strip()
                disp_zh = (obj.get("display_name_zh") or "").strip()
                raw_name = (obj.get("name") or "").strip()

                # 填充缺失的标识字段
                if not std_en:
                    std_en = raw_name or "unknown_event"
                if not disp_zh:
                    disp_zh = raw_name or std_en
                if not raw_name:
                    raw_name = f"{std_en} — {disp_zh}"

                obj.setdefault("event_id", "")
                obj.setdefault("name", raw_name)
                obj.setdefault("standard_name_en", std_en)
                obj.setdefault("display_name_zh", disp_zh)
                obj.setdefault("organizer", obj.get("source_org", "Unknown"))
                obj.setdefault("start_date", "unknown")
                obj.setdefault("end_date", obj.get("start_date", "unknown"))
                obj.setdefault("timezone", "UTC")
                obj.setdefault("is_recurring", False)
                obj.setdefault("format", "unknown")
                obj.setdefault("fee_tier", "unknown")
                obj.setdefault("topics", [])
                obj.setdefault("audience", [])
                obj.setdefault("agenda_highlights", "")
                obj.setdefault("exec_value_score", 3)
                obj.setdefault("exec_value_rationale", f"降级数据: {reason}")
                obj.setdefault("source_url", obj.get("source_url", ""))
                obj.setdefault("source_org", obj.get("source_org", "Unknown"))
                obj.setdefault("discovered_at", self._run_ts)
                obj.setdefault("raw_snippet", "")
                obj.setdefault("city", None)
                obj.setdefault("country", None)
                obj.setdefault("venue", None)
                obj.setdefault("registration_url", None)
                obj.setdefault("registration_deadline", None)

                # 确定性降级 event_id: MD5(coalesce(standard_name_en, name, display_name_zh) + date_str)
                from hashlib import md5
                id_key = std_en or obj["name"] or disp_zh
                date_str = obj.get("start_date", "unknown")
                raw = f"{id_key.strip().lower()}|{date_str.strip()}"
                obj["event_id"] = md5(raw.encode()).hexdigest()[:12]

                # 字段宽容归一化由 EventItem 的 @field_validator 自动处理，无需手动调用。

                # 标记退化
                obj["is_degraded"] = True
                obj["degrade_reason"] = reason

                event = EventItem(**obj)
                return event
            except Exception as e:
                logger.error(f"  [降级] 终极降级也失败: {e} | obj keys: {list(obj.keys())[:10]}")
                return None

        def _validate_and_store(obj: dict, raw_json_snippet: str = "") -> tuple[int, int]:
            """
            三层防御入口:
              1) Pydantic 严格校验
              2) 失败 → 字段宽容修正
              3) 失败 → 降级 EventItem (is_degraded=True)

            返回 (normal_count, degraded_count)
            """
            normal_count = 0
            degraded_count = 0

            # ── 预处理 standard_name_en / display_name_zh ──
            std_en = (obj.get("standard_name_en") or "").strip()
            disp_zh = (obj.get("display_name_zh") or "").strip()
            raw_name = (obj.get("name") or "").strip()

            if not std_en and not disp_zh:
                if " — " in raw_name:
                    parts = raw_name.split(" — ", 1)
                    std_en = parts[0].strip()
                    disp_zh = parts[1].strip() if len(parts) > 1 else raw_name
                else:
                    std_en = raw_name
                    disp_zh = raw_name
            elif not std_en:
                std_en = raw_name
            elif not disp_zh:
                disp_zh = raw_name

            obj["standard_name_en"] = std_en
            obj["display_name_zh"] = disp_zh
            if std_en and disp_zh and std_en != disp_zh:
                obj["name"] = f"{std_en} — {disp_zh}"
            else:
                obj["name"] = std_en or disp_zh or raw_name

            # 用确定性主键生成 event_id（tokenized 名称 + 月份）
            obj["event_id"] = self._make_deterministic_event_id(
                std_en, obj.get("start_date", "")
            )

            # 确保源代码风格字段完整性
            # exec_value_score 的越界钳制由 EventItem 的 @field_validator 自动处理
            obj["source_url"] = obj.get("source_url") or obj.get("registration_url") or ""
            obj["source_org"] = obj.get("source_org") or "Unknown"
            obj["organizer"] = obj.get("organizer") or obj.get("source_org") or "Unknown"

            # 确保 display_name_zh 含中文
            if not self._contains_chinese(disp_zh):
                translated = self._translate_event_name(std_en)
                if translated and self._contains_chinese(translated):
                    obj["display_name_zh"] = translated
                    obj["name"] = f"{std_en} — {translated}"

            # Layer 1: Pydantic 校验（字段级宽容归一化在 schema 内自动完成）
            event = _try_pydantic_parse(obj)
            if event is not None:
                # 校验通过 — 正常的 EventItem；强制标记非降级态
                obj_coerced = dict(obj)
                obj_coerced["is_degraded"] = False
                obj_coerced["degrade_reason"] = ""
                event = EventItem(**obj_coerced)
                if event.exec_value_score > 2:
                    self._events.append(event)
                    normal_count = 1
                return (normal_count, degraded_count)

            # Layer 1 失败 → 记录警告 + 触发降级
            logger.warning(
                f"  [Layer-2 降级] Pydantic 校验失败，启动降级链路。"
                f"  name='{obj.get('name', 'N/A')[:80]}'"
                f"  | raw_snippet: {raw_json_snippet[:120]}"
            )

            degraded = _degraded_event(obj, "LLM_parsing_failed")
            if degraded is not None:
                self._events.append(degraded)
                degraded_count = 1
                logger.warning(
                    f"  [降级完成] 次优数据已注入 | event_id={degraded.event_id}"
                    f"  | is_degraded=True"
                    f"  | name='{degraded.name[:80]}'"
                )
            return (normal_count, degraded_count)

        def _process_batch(batch_idx: int, batch: list[dict]) -> tuple[int, int]:
            """返回 (batch_idx, extracted_count) — 含自愈重试"""
            prompt = self._build_extraction_prompt(batch)

            # ─── 首次 LLM 调用 ───
            logger.info(f"发送批次 {batch_idx} 至 LLM...")
            parsed = _call_llm(prompt, batch_idx, attempt=1)
            if parsed is None:
                # ─── Layer 1-self-heal: 自愈重试（上限 1 次）───
                logger.warning(
                    f"  批次 {batch_idx} 首次解析失败，启动自愈重试..."
                )
                correction_prompt = (
                    f"{prompt}\n\n"
                    f"⚠️ 你上次的输出无法解析为 JSON Array。请严格只输出一个 JSON 数组，"
                    f"不要包含任何 markdown 代码块或额外文本。"
                )
                parsed = _call_llm(correction_prompt, batch_idx, attempt=2)

            if parsed is None:
                # ─── Layer 2: 完全失败 → 整批降级 ───
                logger.error(
                    f"  批次 {batch_idx} 自愈重试也失败，整批降级处理。"
                    f"  原始条目数: {len(batch)}"
                )
                total = 0
                for item in batch:
                    snippet = item.get("body", "") or item.get("title", "")
                    degraded_obj = {
                        "name": item.get("title", "Untitled Event"),
                        "standard_name_en": item.get("title", ""),
                        "display_name_zh": item.get("title", ""),
                        "source_org": item.get("source_org", "Unknown"),
                        "source_url": item.get("url", ""),
                        "start_date": "unknown",
                        "raw_snippet": snippet[:100],
                    }
                    norm, deg = _validate_and_store(degraded_obj, snippet[:120])
                    total += norm + deg
                logger.warning(
                    f"  批次 {batch_idx} 整批降级完成: {total} 条次优数据已注入"
                )
                return (batch_idx, total)

            # ─── 逐条校验 ───
            extracted = 0
            degraded_total = 0
            for obj in parsed:
                snippet = json.dumps(obj, ensure_ascii=False)[:120]
                norm, deg = _validate_and_store(obj, snippet)
                extracted += norm
                degraded_total += deg

            logger.info(f"批次 {batch_idx} 解析完成，提取到 {extracted + degraded_total} 个实体")
            if degraded_total:
                logger.warning(
                    f"  批次 {batch_idx}: {extracted} 条正常 + {degraded_total} 条降级"
                )
            logger.info(f"  批次 {batch_idx}/{len(batches)} 提取 {extracted + degraded_total} 个事件")
            return (batch_idx, extracted + degraded_total)

        import threading
        merge_lock = threading.Lock()
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {
                pool.submit(_process_batch, i, batch): i
                for i, batch in enumerate(batches, 1)
            }
            total_extracted = 0
            for fut in as_completed(futures):
                try:
                    batch_idx, extracted = fut.result()
                    total_extracted += extracted
                except Exception as exc:
                    logger.error(f"  批次处理异常: {exc}")

        degraded_count = sum(1 for e in self._events if e.is_degraded)
        if degraded_count:
            logger.warning(
                f"[Phase 4] 完成。共提取 {len(self._events)} 个事件，"
                f"其中 {degraded_count} 条为降级次优数据 (is_degraded=True)"
            )
        else:
            logger.info(f"[Phase 4] 完成。共提取 {len(self._events)} 个有效事件")

        self._ensure_chinese_names()

    @staticmethod
    def _contains_chinese(text: str) -> bool:
        """检查文本是否包含中文字符。"""
        return any('\u4e00' <= c <= '\u9fff' for c in text)

    def _translate_event_name(self, name: str) -> Optional[str]:
        """尝试用内置字典翻译常见 ESG 活动名称关键词（离线回退）。
        
        如果 LLM 未按要求输出中文名称，使用此方法提供基本翻译。
        对于复杂名称，返回 None 表示无法翻译。
        """
        if not name:
            return None

        # ESG 活动常见关键词中英对照表
        translations = {
            # 组织/身份
            "united nations": "联合国",
            "world bank": "世界银行",
            "european commission": "欧盟委员会",
            "european union": "欧盟",
            "international energy agency": "国际能源署",
            "world economic forum": "世界经济论坛",
            "international sustainability standards board": "国际可持续准则理事会",
            "issb": "国际可持续准则理事会",
            "gri": "全球报告倡议组织",
            "tcfd": "气候相关财务信息披露工作组",
            "tnfd": "自然相关财务信息披露工作组",
            "sasb": "可持续会计准则委员会",
            "cdp": "碳信息披露项目",
            "unep": "联合国环境规划署",
            "undp": "联合国开发计划署",
            "unfccc": "联合国气候变化框架公约",
            "ipcc": "政府间气候变化专门委员会",
            # 活动类型
            "conference": "会议",
            "summit": "峰会",
            "forum": "论坛",
            "workshop": "研讨会",
            "webinar": "网络研讨会",
            "symposium": "专题讨论会",
            "congress": "大会",
            "assembly": "大会",
            "expo": "博览会",
            "exhibition": "展览会",
            "roundtable": "圆桌会议",
            "dialogue": "对话",
            "session": "会议",
            "annual meeting": "年会",
            "general assembly": "全体大会",
            "ministerial": "部长级",
            "high-level": "高级别",
            # 主题
            "climate": "气候",
            "climate change": "气候变化",
            "sustainability": "可持续发展",
            "sustainable development": "可持续发展",
            "esg": "ESG",
            "net zero": "净零",
            "net-zero": "净零",
            "carbon": "碳",
            "carbon market": "碳市场",
            "carbon pricing": "碳定价",
            "carbon credit": "碳信用",
            "decarbonization": "脱碳",
            "decarbonisation": "脱碳",
            "renewable energy": "可再生能源",
            "clean energy": "清洁能源",
            "energy transition": "能源转型",
            "green finance": "绿色金融",
            "sustainable finance": "可持续金融",
            "green bond": "绿色债券",
            "biodiversity": "生物多样性",
            "nature-based": "基于自然的",
            "circular economy": "循环经济",
            "waste management": "废物管理",
            "water": "水资源",
            "deforestation": "森林砍伐",
            "supply chain": "供应链",
            "due diligence": "尽职调查",
            "human rights": "人权",
            "labor rights": "劳工权利",
            "social impact": "社会影响",
            "corporate governance": "公司治理",
            "transparency": "透明度",
            "reporting": "报告",
            "disclosure": "披露",
            "taxonomy": "分类法",
            "green taxonomy": "绿色分类法",
            "csrd": "企业可持续发展报告指令",
            "csddd": "企业可持续发展尽职调查指令",
            "sfdr": "可持续金融披露条例",
            "eu taxonomy": "欧盟分类法",
            "paris agreement": "巴黎协定",
            "cop": "缔约方会议",
            "ndc": "国家自主贡献",
            "adaptation": "适应",
            "resilience": "韧性",
            "stakeholder": "利益相关方",
            "engagement": "参与",
            "innovation": "创新",
            "technology": "技术",
            "digital": "数字化",
            "ai": "人工智能",
            "artificial intelligence": "人工智能",
        }

        n = name.strip()
        # 先尝试精确匹配（不区分大小写）
        lower = n.lower()
        if lower in translations:
            return translations[lower]

        # 逐词替换翻译
        result = n
        # 按长度降序替换（长词优先）
        for en, cn in sorted(translations.items(), key=lambda x: len(x[0]), reverse=True):
            pattern = re.compile(re.escape(en), re.IGNORECASE)
            if pattern.search(result):
                result = pattern.sub(cn, result)

        # 如果翻译后与原名称明显不同（有变化），返回翻译结果
        if result != n and self._contains_chinese(result):
            # 清理多余空格和标点
            result = re.sub(r'\s+', ' ', result).strip()
            result = re.sub(r'\s*-\s*', '', result)  # 移除残留连字符
            return result

        # 如果仍未翻译成功，尝试用简单方法：在名称前添加通用提示
        # 检测是否为纯英文
        if re.match(r'^[A-Za-z0-9\s\-\'",.!?:;()&@#$%^*+=/\\[\]{}|~` ]+$', n):
            # 纯英文但无法用字典翻译，返回 None 让调用方决定
            return None

        return None

    def _ensure_chinese_names(self) -> None:
        """后处理：确保 Phase 4.3 合并后的所有事件名称都包含中文。
        
        对于合并后仍不含中文的名称，尝试重新翻译。
        """
        fixed = 0
        for ev in self._events:
            if self._contains_chinese(ev.name):
                continue
            # 尝试提取英文部分（格式 "English Name — Chinese Name" 中的英文部分）
            # 如果名称不含 " — " 且不含中文，说明 LLM 完全未遵守指令
            if " — " in ev.name:
                # 可能 LLM 输出格式为 "EN — CN" 但 CN 不含中文
                parts = ev.name.split(" — ", 1)
                en_part = parts[0].strip()
                cn_part = parts[1].strip() if len(parts) > 1 else ""
                if self._contains_chinese(cn_part):
                    continue  # 中文部分确实有中文，无需处理
                # 尝试翻译英文部分
                translated = self._translate_event_name(en_part)
                if translated and translated != en_part:
                    ev.name = f"{en_part} — {translated}"
                    ev.event_id = self._make_deterministic_event_id(
                        ev.standard_name_en, ev.start_date
                    )
                    fixed += 1
                    logger.warning(f"  [翻译后补] '{en_part[:60]}' → '{ev.name[:80]}'")
            else:
                # 纯非中文名称，尝试翻译
                translated = self._translate_event_name(ev.name)
                if translated and translated != ev.name:
                    ev.name = f"{ev.name} — {translated}"
                    ev.event_id = self._make_deterministic_event_id(
                        ev.standard_name_en, ev.start_date
                    )
                    fixed += 1
                    logger.warning(f"  [翻译后补] '{ev.name[:60]}' → 已补充中文")
        if fixed:
            logger.info(f"[Phase 4.1] 名称中文化后处理: 修正 {fixed} 个事件名称")

    # ── 确定性主键生成 ────────────────────────────────────

    @staticmethod
    def _make_deterministic_event_id(standard_name_en: str, start_date: str, city: Optional[str] = None) -> str:
        """基于 tokenized 名称 + 月份 生成稳定的 MD5[:12] 主键。

        与 v1 的关键区别：
        - 使用 _tokenize_name 对 standard_name_en 做词根化
        - 对词根排序后拼接，保证哈希稳定（不受原始词序波动影响）
        - 去除 city 因子：city 提取不稳定（有的新闻有城市，有的没有），
          会导致同构事件产生不同 event_id
        - start_date 截断到月份（YYYY-MM）：容忍同月内日期的微小漂移
          （实际举办日与新闻发布日可能有几天差异），防止同一事件因日期漂移而重复创建；
          同年不同月举办的活动使用不同月份键，不会碰撞
        """
        tokens = sorted(EventRadarAgent._tokenize_name(standard_name_en))
        name_key = " ".join(tokens)
        month_key = (start_date or "").strip()[:7]  # YYYY-MM
        raw = f"{name_key}|{month_key}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    # ── Phase 4.3: 跨源/跨语言事件去重 ──────────────────────

    @staticmethod
    def _tokenize_name(name: str) -> set[str]:
        """将事件名称转为词根集合，用于 Jaccard 相似度计算。

        处理步骤：
        1. 全小写
        2. 移除年份数字（2024-2028）和所有标点符号
        3. 移除停用词（the/in/of/on/at + 会议通用后缀）
        4. 按空格拆分为 set
        """
        if not name:
            return set()
        n = name.strip().lower()
        # 移除年份数字（独立词或连在词边）
        n = re.sub(r"\b20(2[4-9]|3[0-9]|4[0-8])\b", " ", n)
        # 移除标点
        n = re.sub(r"[^\w\s]", " ", n)
        # 移除停用词
        stopwords = {
            "the", "in", "of", "on", "at",
            "summit", "conference", "forum", "annual", "event", "seminar",
        }
        tokens = [w for w in n.split() if w and w not in stopwords]
        if not tokens:
            tokens = n.split()
        return set(tokens)

    @staticmethod
    def _jaccard_similarity(a: set[str], b: set[str]) -> float:
        """计算两个词根集合的 Jaccard 相似度。分母为 0 时返回 0.0。"""
        if not a or not b:
            return 0.0
        intersection = a & b
        union = a | b
        if not union:
            return 0.0
        return len(intersection) / len(union)

    def _deduplicate_events(self) -> None:
        """合并来自不同语言/来源的同一活动。

        策略（v3.0.5 — 折中：同月/≤30天 + Jaccard≥0.55）：

        策略A（名称精确匹配·同月或 ≤30天）：
            standard_name_en 完全一致（忽略大小写）
            + 同年同月 或 日期差 ≤30天 或 任一方日期未知 → 合并。
            会议公告常被不同媒体在不同时间报道，日期漂移可跨月。

        策略B（空间锚点·中窗口）：
            city 字段均非空且一致（忽略大小写）
            + start_date 相差 ≤7 天
            + 名称 Jaccard 相似度 ≥0.35 → 合并。

        策略C（名称高似·同月）：
            standard_name_en 经 _tokenize_name 后 Jaccard ≥0.55
            + 同年同月（或双方日期均 unknown）→ 合并。
            捕捉轻微命名差异但属于同一活动的情况。

        每个聚类保留信息最完整的一条，合并互补标签。
        """
        if len(self._events) <= 1:
            return

        from datetime import date as date_type

        # 解析所有事件的日期
        parsed_dates: dict[int, Optional[date_type]] = {}
        for i, ev in enumerate(self._events):
            if ev.start_date and ev.start_date not in ("unknown", ""):
                try:
                    parsed_dates[i] = date_type.fromisoformat(ev.start_date)
                except ValueError:
                    parsed_dates[i] = None
            else:
                parsed_dates[i] = None

        n = len(self._events)
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

        def _same_year(d1: date_type, d2: date_type) -> bool:
            return d1.year == d2.year

        def _same_month(d1: date_type, d2: date_type) -> bool:
            return d1.year == d2.year and d1.month == d2.month

        # ── 策略A: standard_name_en 精确匹配（忽略大小写）+ 宽日期窗口 ──
        for i in range(n):
            for j in range(i + 1, n):
                if find(i) == find(j):
                    continue
                ev_a, ev_b = self._events[i], self._events[j]

                # standard_name_en 忽略大小写完全一致
                key_a = (ev_a.standard_name_en or "").strip().lower()
                key_b = (ev_b.standard_name_en or "").strip().lower()
                if not (key_a and key_b and key_a == key_b):
                    continue

                # 日期容差：同年同月 / 日期差 ≤30天 / 任一方 unknown → 通过
                date_a = parsed_dates.get(i)
                date_b = parsed_dates.get(j)
                if date_a is not None and date_b is not None:
                    if not (_same_month(date_a, date_b)
                            or abs((date_a - date_b).days) <= 30):
                        continue
                # date_a is None or date_b is None → 通过（unknown 亦合并）

                union(i, j)
                day_diff = (abs((date_a - date_b).days)
                            if date_a and date_b else "?")
                logger.debug(
                    f"  [策略A 精确去重] std_en='{key_a}' diff={day_diff}d | "
                    f"{ev_a.name[:50]} <-> {ev_b.name[:50]}"
                )

        # ── 策略B: 空间锚点 — city + 日期 ±7 天 + Jaccard ≥0.35 ──
        for i in range(n):
            for j in range(i + 1, n):
                if find(i) == find(j):
                    continue
                ev_a, ev_b = self._events[i], self._events[j]

                # 空间校验：city 均非空且完全一致（忽略大小写）
                city_a = (ev_a.city or "").strip().lower()
                city_b = (ev_b.city or "").strip().lower()
                if not city_a or not city_b or city_a != city_b:
                    continue

                # 时间校验：start_date 相差 ≤7 天
                date_a = parsed_dates.get(i)
                date_b = parsed_dates.get(j)
                if date_a is None or date_b is None:
                    continue
                if abs((date_a - date_b).days) > 7:
                    continue

                # 文本弱校验：standard_name_en 经 _tokenize_name 后 Jaccard ≥0.35
                tokens_a = self._tokenize_name(ev_a.standard_name_en or "")
                tokens_b = self._tokenize_name(ev_b.standard_name_en or "")
                jaccard = self._jaccard_similarity(tokens_a, tokens_b)
                if jaccard >= 0.35:
                    union(i, j)
                    logger.debug(
                        f"  [策略B 空间锚点] city='{city_a}' jaccard={jaccard:.2f} "
                        f"diff={abs((date_a - date_b).days)}d | "
                        f"{ev_a.name[:50]} <-> {ev_b.name[:50]}"
                    )

        # ── 策略C: 名称高似·同月 — Jaccard ≥0.55 + 同年同月 ──
        for i in range(n):
            for j in range(i + 1, n):
                if find(i) == find(j):
                    continue
                ev_a, ev_b = self._events[i], self._events[j]

                tokens_a = self._tokenize_name(ev_a.standard_name_en or "")
                tokens_b = self._tokenize_name(ev_b.standard_name_en or "")
                jaccard = self._jaccard_similarity(tokens_a, tokens_b)
                if jaccard < 0.55:
                    continue

                # 月份校验：同年同月 或 双方均 unknown
                date_a = parsed_dates.get(i)
                date_b = parsed_dates.get(j)
                if date_a is not None and date_b is not None:
                    if not _same_month(date_a, date_b):
                        continue
                # 任一方 unknown → 通过

                union(i, j)
                logger.debug(
                    f"  [策略C 名称高似] jaccard={jaccard:.2f} | "
                    f"{ev_a.name[:50]} <-> {ev_b.name[:50]}"
                )

        # 将 Union-Find 结果转换为聚类
        clusters_map: dict[int, list[int]] = defaultdict(list)
        for i in range(n):
            clusters_map[find(i)].append(i)

        # 合并每个聚类
        merged: list[EventItem] = []
        duplicates_removed = 0
        for indices in clusters_map.values():
            cluster = [self._events[i] for i in indices]
            if len(cluster) == 1:
                merged.append(cluster[0])
            else:
                best = self._merge_event_cluster(cluster)
                merged.append(best)
                duplicates_removed += len(cluster) - 1
                logger.debug(
                    f"  [去重合并] {len(cluster)} 条 → 1 条: {best.name[:80]}"
                )

        if duplicates_removed > 0:
            logger.info(
                f"[Phase 4.3] 跨源去重: 移除 {duplicates_removed} 条重复事件，"
                f"保留 {len(merged)} 条"
            )
        self._events = merged

    @staticmethod
    def _merge_event_cluster(cluster: list[EventItem]) -> EventItem:
        """合并同一活动的多条记录，保留最佳信息。"""
        if len(cluster) == 1:
            return cluster[0]

        # 选评分最高的作为基准
        best = max(cluster, key=lambda e: (e.exec_value_score, len(e.topics), len(e.audience)))

        # 收集所有来源的互补信息
        all_topics: set[str] = set(best.topics)
        all_audience: set[str] = set(best.audience)
        all_source_urls: list[str] = [best.source_url]
        all_source_orgs: set[str] = {best.source_org}
        best_city = best.city
        best_venue = best.venue
        best_reg_url = best.registration_url
        best_format = best.format

        for ev in cluster:
            if ev is best:
                continue
            all_topics.update(ev.topics)
            all_audience.update(ev.audience)
            all_source_orgs.add(ev.source_org)
            if ev.source_url and ev.source_url not in all_source_urls:
                all_source_urls.append(ev.source_url)
            # 优先保留非空的地点信息
            if not best_city and ev.city:
                best_city = ev.city
            if not best_venue and ev.venue:
                best_venue = ev.venue
            if not best_reg_url and ev.registration_url:
                best_reg_url = ev.registration_url
            if best_format == "unknown" and ev.format != "unknown":
                best_format = ev.format
            # 优先保留含中文的名称
            if any('\u4e00' <= c <= '\u9fff' for c in ev.name) and not any(
                '\u4e00' <= c <= '\u9fff' for c in best.name
            ):
                best.name = ev.name
            if any('\u4e00' <= c <= '\u9fff' for c in ev.organizer) and not any(
                '\u4e00' <= c <= '\u9fff' for c in best.organizer
            ):
                best.organizer = ev.organizer

        # 更新合并后的字段
        best.topics = sorted(all_topics)[:10]
        best.audience = sorted(all_audience)[:10]
        best.city = best_city
        best.venue = best_venue
        best.registration_url = best_reg_url
        best.format = best_format
        best.source_org = ", ".join(sorted(all_source_orgs)[:3])
        best.source_url = all_source_urls[0]  # 保留第一个非 GNews URL
        for u in all_source_urls:
            if "news.google.com" not in u:
                best.source_url = u
                break
        # 更新 event_id（因为 name/source 可能变了）
        best.event_id = EventRadarAgent._make_deterministic_event_id(
            best.standard_name_en, best.start_date
        )

        return best

    # ── Phase 4.5: 过期事件过滤 ───────────────────────────

    def _filter_future_events(self) -> None:
        """丢弃开始日期超过 14 天前的历史活动，保留未来事件和日期未知的条目。"""
        from datetime import date, timedelta
        cutoff = date.today() - timedelta(days=14)
        kept, dropped = [], 0
        for ev in self._events:
            if ev.start_date in ("unknown", ""):
                kept.append(ev)
                continue
            try:
                if date.fromisoformat(ev.start_date) >= cutoff:
                    kept.append(ev)
                else:
                    logger.debug(f"  [过期] {ev.start_date} {ev.name}")
                    dropped += 1
            except ValueError:
                kept.append(ev)
        if dropped:
            logger.info(f"[Phase 4.5] 过期过滤: 丢弃 {dropped} 个历史活动，保留 {len(kept)} 个")
        self._events = kept

    # ── Phase 6: Notion Upsert ────────────────────────────

    def _upsert_to_notion(self) -> None:
        """委托 sinks/notion_sync.py 执行 Notion Upsert。"""
        notion_token = os.environ.get("NOTION_TOKEN", "")
        notion_db_id = os.environ.get("NOTION_DATABASE_ID", "")

        if not notion_token or not notion_db_id:
            logger.info("[Phase 6] Notion 密钥未就绪，跳过 Upsert。")
            return

        sink = NotionSink(token=notion_token, database_id=notion_db_id)
        sink.upsert(self._events, self._run_ts)

    # ── Phase 7: .ics 日历生成 ───────────────────────────

    def _generate_ics(self) -> Optional[str]:
        """委托 sinks/ics_writer.py 生成 .ics 文件。"""
        return _ics_write(self._events, OUTPUT_ICS_PATH)

    # ── Phase 8: 钉钉精简卡片推送 ────────────────────────

    def push_to_dingtalk(self) -> None:
        """委托 sinks/dingtalk_push.py 执行钉钉推送。"""
        webhook = os.environ.get("DINGTALK_WEBHOOK_RADAR", "")
        notion_db_id = os.environ.get("NOTION_DATABASE_ID", "")
        _dingtalk_push(self._events, webhook, notion_db_id, self._run_ts)

    # ── 主入口 ────────────────────────────────────────────

    def run(self, no_push: bool = False) -> None:
        t0 = time.monotonic()
        logger.info("══ ESG Event Radar v1.3 | 周频扫描启动 ══")

        # Phase 0: 加载源清单
        self._load_sources()

        # Phase 1a: HTML Calendar 爬虫（先跑，为 RSS 补充 Tier 1 数据）
        self._fetch_html_calendars()

        # Phase 1b: RSS 抓取
        self._fetch_all_sources()

        # ── 零数据熔断（继承自基座）──
        if not self._raw_items:
            self._send_system_alert(
                "🚨 [FATAL] ESG Event Radar 周频扫描零数据熔断。"
                "所有 RSS 源本次抓取量为 0，请立即排查网络节点或 sources.yaml 配置！"
            )
            return

        # Phase 3: 正文提取
        self._enrich_content()

        # Phase 4: LLM 结构化提取
        self._extract_events_with_llm()

        # Phase 4.3: 跨源/跨语言去重（新增）
        self._deduplicate_events()

        # ── 无事件静默阻断（继承自基座）──
        if not self._events:
            logger.info(
                f"[静默阻断] 本次扫描 {len(self._raw_items)} 条原始条目，"
                "LLM 判定无符合条件会议事件，执行静默阻断，不推送。"
            )
            return

        logger.info(f"共提取 {len(self._events)} 个事件，进入交付阶段。")

        # Phase 6: Notion Upsert 全量写入（含历史事件，构建长效活动库）
        self._upsert_to_notion()

        # Phase 4.5: 过期过滤 — 仅用于 .ics 和钉钉推送，不影响 Notion
        self._filter_future_events()
        if not self._events:
            logger.info("[静默阻断] 全部为历史活动，Notion 已更新，跳过钉钉推送。")
            return

        # Phase 7: .ics 生成
        self._generate_ics()

        # Phase 8: 钉钉推送
        if not no_push:
            self.push_to_dingtalk()
        else:
            logger.info("[Phase 8] --no-push 已指定，跳过钉钉推送。")

        elapsed = time.monotonic() - t0
        logger.info(f"══ 完成。耗时 {elapsed:.1f}s | 事件数: {len(self._events)} ══")

        # 运行指标收集
        try:
            metrics = RunMetrics(
                timestamp=datetime.now(timezone.utc).isoformat(),
                mode="weekly",
                elapsed_seconds=elapsed,
                total_raw_items=len(self._raw_items),
                after_dedup=len(self._events),
                final_report_items=len(self._events),
            )
            MetricsStore(str(Path(__file__).parent / "metrics.jsonl")).append(metrics)
        except Exception:
            pass

        if self._llm:
            self._llm.print_stats()

        # 调试：打印本次提取的事件清单
        if self._events:
            logger.info("── 本次提取事件清单 ──")
            for ev in sorted(self._events, key=lambda e: e.exec_value_score, reverse=True):
                logger.info(
                    f"  [{ev.exec_value_score}★] {ev.name} | {ev.start_date}~{ev.end_date}"
                    f" | {ev.format} | {ev.city or '未知城市'} | {ev.organizer}"
                )


# ─────────────────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ESG Event Radar — 全球会议/活动预警系统")
    p.add_argument(
        "--no-push",
        action="store_true",
        default=False,
        help="跳过钉钉 Webhook 推送（调试用）",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    agent = EventRadarAgent()
    agent.run(no_push=args.no_push)