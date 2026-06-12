#!/usr/bin/env python3
"""
全球 ESG 与可持续发展会议/活动动态预警系统 (Event Radar) v1.3
══════════════════════════════════════════════════════════════════════════════
架构层级
─────────
  EventRadarAgent.run()
    Phase 0  : 加载供料源清单（sources.yaml）
    Phase 1a : 三轨 RSS 抓取（轨道 B 行业媒体 + 轨道 C Google News）
    Phase 1b : HTML Calendar 爬虫（轨道 A — Tier 1 组织官网 Events 页）
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
from openai import OpenAI
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

load_dotenv()

from schemas.event import EventItem

# Phase 0 供应侧引擎
from sourcing_engine import load_sources as _phase0_load_sources
from sourcing_engine import fetch_rss as _phase0_fetch_rss

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

# DeepSeek API（OpenAI-compatible endpoint）
DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
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
    """SHA256[:12] 作为 Notion Upsert 去重主键。"""
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
            feed = feedparser.parse(url)
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

    TIMEOUT = 3  # 降低超时：正文提取非关键路径，快速失败即可
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
        self._llm = OpenAI(api_key=api_key or "placeholder", base_url=DEEPSEEK_BASE_URL)

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

    # ── Phase 0 (新): 上游供料摄入层 — 透传至 raw_items ─────

    def _run_phase0_ingestion(self) -> None:
        """
        加载 config/sources.yaml 中 enabled 的 google_news_rss 源，
        调用 sourcing_engine 抓取并脱壳，将结果注入 self._raw_items。
        
        输出格式兼容现有 Phase 1-8 管道，不需要修改下游代码。
        """
        try:
            phase0_sources = _phase0_load_sources()
        except Exception as exc:
            logger.warning(f"[Phase 0] 加载 config/sources.yaml 失败: {exc}")
            return

        if not phase0_sources:
            logger.info("[Phase 0] 无启用源，跳过供料摄入。")
            return

        logger.info(f"🚀 [Pipeline Start] Fetching from {len(phase0_sources)} Phase 0 source(s)…")

        total_injected = 0
        for src in phase0_sources:
            sid = src["id"]
            try:
                items = _phase0_fetch_rss(src)
            except Exception as exc:
                logger.error(f"[Phase 0] {sid} 抓取异常: {exc}")
                continue

            for item in items:
                title = item.get("title", "")
                title_key = title.strip().lower()
                # 去重：与已有 raw_items 做标题+URL 双重去重
                if item.get("link", "") in self._seen_urls:
                    continue
                if title_key in self._seen_titles:
                    continue

                # 映射到 raw_items 字典格式（兼容 Phase 3-8 下游管线）
                raw_entry = {
                    "title":       title,
                    "date":        item.get("published_date", ""),
                    "source":      sid,
                    "source_href": sid,
                    "url":         item.get("real_url", item.get("link", "")),
                    "description": item.get("clean_text", "")[:500],
                    "body":        item.get("clean_text", "")[:300],       # 预填充正文，跳过 Phase 3 正文提取
                    "source_org":  sid,
                    "source_tags": [],
                    "source_tier": 2,
                    "parsed_date": parse_rss_date(item.get("published_date", "")),
                    "_is_google_news": True,
                    "_phase0_source_id": sid,
                }

                self._seen_urls.add(item.get("link", ""))
                self._seen_titles.add(title_key)
                self._raw_items.append(raw_entry)
                total_injected += 1

            logger.info(f"  [Phase 0] {sid}: 注入 {len(items)} 条")

        logger.info(f"[Phase 0] 供料完成。总注入 {total_injected} 条纯文本条目")

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
  "standard_name_en": "标准英文名，用于去重。无论原文是什么语言（中文/印尼语/法语/英语），统一翻译为简洁的英文名称",
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

        @retry(
            wait=wait_random_exponential(multiplier=1, max=30),
            stop=stop_after_attempt(3),
            retry=retry_if_exception_type((
                Exception,  # 捕获所有异常（含 openai.APIConnectionError / RateLimitError / Timeout）
            )),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=False,  # 重试耗尽后返回 None，不中断批次处理
        )
        def _call_llm_with_retry(prompt: str, batch_idx: int) -> list[dict] | None:
            """带 tenacity 指数退避的 LLM 调用。3 次重试后返回 None。"""
            resp = self._llm.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=8192,
            )
            raw_json = resp.choices[0].message.content
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
            """Layer 1: 严格 Pydantic 校验。成功返回 EventItem，失败返回 None。"""
            try:
                return EventItem(**obj)
            except Exception:
                # 尝试字段级宽容修正后再解析一次
                try:
                    obj_coerced = dict(obj)
                    if "format" in obj_coerced:
                        obj_coerced["format"] = EventItem._coerce_format(obj_coerced["format"])
                    if "fee_tier" in obj_coerced:
                        obj_coerced["fee_tier"] = EventItem._coerce_fee_tier(obj_coerced["fee_tier"])
                    if "exec_value_score" in obj_coerced:
                        obj_coerced["exec_value_score"] = EventItem._clamp_score(obj_coerced["exec_value_score"])
                    return EventItem(**obj_coerced)
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

                # 字段宽容修正
                obj["format"] = EventItem._coerce_format(obj["format"])
                obj["fee_tier"] = EventItem._coerce_fee_tier(obj["fee_tier"])
                obj["exec_value_score"] = EventItem._clamp_score(obj["exec_value_score"])

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

            # 用 standard_name_en 生成 event_id
            obj["event_id"] = make_event_id(std_en, obj.get("start_date", ""))

            # 确保源代码风格字段完整性
            score = obj.get("exec_value_score", 3)
            obj["exec_value_score"] = max(1, min(5, int(score)))
            obj["source_url"] = obj.get("source_url") or obj.get("registration_url") or ""
            obj["source_org"] = obj.get("source_org") or "Unknown"
            obj["organizer"] = obj.get("organizer") or obj.get("source_org") or "Unknown"

            # 确保 display_name_zh 含中文
            if not self._contains_chinese(disp_zh):
                translated = self._translate_event_name(std_en)
                if translated and self._contains_chinese(translated):
                    obj["display_name_zh"] = translated
                    obj["name"] = f"{std_en} — {translated}"

            # Layer 1: Pydantic 严格校验
            event = _try_pydantic_parse(obj)
            if event is not None:
                # 校验通过 — 正常的 EventItem
                obj_coerced = dict(obj)
                obj_coerced["is_degraded"] = False
                obj_coerced["degrade_reason"] = ""
                obj_coerced["format"] = EventItem._coerce_format(obj_coerced.get("format", "unknown"))
                obj_coerced["fee_tier"] = EventItem._coerce_fee_tier(obj_coerced.get("fee_tier", "unknown"))
                obj_coerced["exec_value_score"] = EventItem._clamp_score(obj_coerced.get("exec_value_score", 3))
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
                    ev.event_id = make_event_id(ev.name, ev.start_date)
                    fixed += 1
                    logger.warning(f"  [翻译后补] '{en_part[:60]}' → '{ev.name[:80]}'")
            else:
                # 纯非中文名称，尝试翻译
                translated = self._translate_event_name(ev.name)
                if translated and translated != ev.name:
                    ev.name = f"{ev.name} — {translated}"
                    ev.event_id = make_event_id(ev.name, ev.start_date)
                    fixed += 1
                    logger.warning(f"  [翻译后补] '{ev.name[:60]}' → 已补充中文")
        if fixed:
            logger.info(f"[Phase 4.1] 名称中文化后处理: 修正 {fixed} 个事件名称")

    # ── Phase 4.3: 跨源/跨语言事件去重 ──────────────────────

    def _deduplicate_events(self) -> None:
        """合并来自不同语言/来源的同一活动。

        策略（v1.5 — 基于 standard_name_en 精确去重）：
        1. 主策略：standard_name_en 完全相同 + 日期窗口 ±3 天 → 合并
           （这是 100% 多语种合并的关键：
             "EU ESG Summit" 和 "欧盟 ESG 峰会" 的 standard_name_en 都是 "EU ESG Summit"）
        2. 退避策略：standard_name_en 按去后缀后的关键词 Jaccard 匹配（阈值 0.70）
           （用于 LLM 未严格遵守 standard_name_en 输出格式的情况）
        3. 每个聚类保留信息最完整的一条，合并互补标签
        """
        if len(self._events) <= 1:
            return

        from datetime import date as date_type, timedelta

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

        # 预处理：每个事件的标准化 key
        def _std_key(ev: EventItem) -> str:
            """提取 standard_name_en 并做轻量标准化（小写、去前后空格）。"""
            s = (ev.standard_name_en or ev.name).strip().lower()
            # 移除常见后缀词（让 "EU ESG Summit 2026" 和 "EU ESG Summit" 匹配）
            for suffix in ["conference", "summit", "forum", "meeting", "event",
                           "workshop", "webinar", "symposium", "congress", "assembly",
                           "seminar", "expo", "roundtable", "dialogue",
                           "annual", "international", "global", "world",
                           "2024", "2025", "2026", "2027", "2028"]:
                s = re.sub(rf"\b{re.escape(suffix)}\b", "", s)
            s = re.sub(r"[^\w\s]", " ", s)
            s = re.sub(r"\s+", " ", s).strip()
            return s

        # ── 策略 1: standard_name_en 精确匹配 + 日期 ±3 天 ──
        for i in range(n):
            for j in range(i + 1, n):
                if find(i) == find(j):
                    continue
                ev_a, ev_b = self._events[i], self._events[j]

                # 日期窗口检查
                date_a = parsed_dates.get(i)
                date_b = parsed_dates.get(j)
                dates_close = False
                if date_a is not None and date_b is not None:
                    diff = abs((date_a - date_b).days)
                    dates_close = diff <= 3
                elif date_a is None and date_b is None:
                    dates_close = True
                else:
                    dates_close = False

                if not dates_close:
                    continue

                # 精确匹配 standard_name_en（标准化后）
                key_a = _std_key(ev_a)
                key_b = _std_key(ev_b)
                if key_a == key_b:
                    union(i, j)
                    logger.debug(
                        f"  [精确去重] std_en='{key_a}' | "
                        f"{ev_a.name[:50]} <-> {ev_b.name[:50]}"
                    )

        # ── 策略 2: 退避 — 关键词 Jaccard 相似度 ≥ 0.70 + 日期 ±7 天 ──
        for i in range(n):
            for j in range(i + 1, n):
                if find(i) == find(j):
                    continue
                ev_a, ev_b = self._events[i], self._events[j]
                date_a = parsed_dates.get(i)
                date_b = parsed_dates.get(j)
                if date_a is None or date_b is None:
                    continue
                diff = abs((date_a - date_b).days)
                if diff > 7:
                    continue

                # 用 normalized 关键词做 Jaccard
                words_a = set(_std_key(ev_a).split())
                words_b = set(_std_key(ev_b).split())
                if not words_a or not words_b:
                    continue
                jaccard = len(words_a & words_b) / len(words_a | words_b)
                if jaccard >= 0.70:
                    union(i, j)
                    logger.debug(
                        f"  [退避去重] jaccard={jaccard:.2f} diff={diff}d | "
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
        best.event_id = make_event_id(best.name, best.start_date)

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

    _NOTION_API = "https://api.notion.com/v1"
    _NOTION_VERSION = "2022-06-28"

    # 需要在数据库中创建的属性（title 属性"Name"由 Notion 自动创建）
    _NOTION_REQUIRED_PROPS: dict = {
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

    # Notion API 速率限制：3 req/s，主动流量整形 + tenacity 指数退避
    _NOTION_BASE_INTERVAL = 0.6        # 基础请求间隔（主动流量整形）
    _NOTION_MAX_RETRIES = 5            # 最大重试次数
    _NOTION_TRAFFIC_SHAPE_DELAY = 0.35 # 每次 API 调用后主动延迟（平滑请求曲线）

    class NotionAPIError(Exception):
        """Notion API 返回非 2xx 状态码时抛出。"""
        def __init__(self, status_code: int, message: str, response_text: str = ""):
            self.status_code = status_code
            self.message = message
            self.response_text = response_text
            super().__init__(f"HTTP {status_code}: {message}")

    @staticmethod
    def _retry_on_notion_rate_limit(func):
        """tenacity 装饰器：指数退避 + 随机抖动，专门处理 Notion 429/5xx。
        
        T_wait = min(10, base × 2^attempt + random_jitter)
        带 before_sleep 日志，停止条件：5 次尝试或非可重试错误。
        """
        def _is_retryable(exception: BaseException) -> bool:
            if isinstance(exception, EventRadarAgent.NotionAPIError):
                return exception.status_code in (429, 500, 502, 503, 504)
            if isinstance(exception, requests.RequestException):
                return True
            return False

        return retry(
            wait=wait_random_exponential(multiplier=0.6, max=10),
            stop=stop_after_attempt(EventRadarAgent._NOTION_MAX_RETRIES),
            retry=retry_if_exception_type((EventRadarAgent.NotionAPIError, requests.RequestException)),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )(func)

    def _notion_headers(self, token: str) -> dict:
        return {
            "Authorization": f"Bearer {token}",
            "Notion-Version": self._NOTION_VERSION,
            "Content-Type": "application/json",
        }

    def _ensure_notion_db_properties(self, db_id: str, headers: dict) -> Optional[str]:
        """检查并补全数据库属性；返回 title 属性名（用于后续 Upsert），失败返回 None。"""
        r = requests.get(f"{self._NOTION_API}/databases/{db_id}", headers=headers, timeout=15)
        if r.status_code != 200:
            logger.warning(
                f"[Phase 6] 读取数据库失败 HTTP {r.status_code}: {r.text[:300]}"
            )
            return None

        body = r.json()
        properties = body.get("properties", {})

        # 自动检测 title 列名（Notion 数据库的 title 属性名并非固定为 "Name"）
        title_prop_name = "Name"  # 默认值
        for prop_name, prop_info in properties.items():
            if prop_info.get("type") == "title":
                title_prop_name = prop_name
                logger.info(f"[Phase 6] 检测到 title 属性: '{title_prop_name}'")
                break

        existing = set(properties.keys())
        to_add = {k: v for k, v in self._NOTION_REQUIRED_PROPS.items() if k not in existing}
        if not to_add:
            return title_prop_name

        logger.info(f"[Phase 6] 补全缺失字段: {list(to_add.keys())}")
        pr = requests.patch(
            f"{self._NOTION_API}/databases/{db_id}",
            headers=headers,
            json={"properties": to_add},
            timeout=15,
        )
        if pr.status_code not in (200, 201):
            logger.warning(
                f"[Phase 6] 字段创建失败 HTTP {pr.status_code}: {pr.text[:300]}"
            )
            return None
        return title_prop_name

    @staticmethod
    def _build_notion_page_props(ev: "EventItem", title_prop_name: str = "Name") -> dict:
        """将 EventItem 映射到 Notion page properties dict。
        
        Args:
            ev: 事件对象
            title_prop_name: 数据库中 title 属性的实际名称（从数据库元数据检测获取）
        """

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

        # 优先使用 display_name_zh 作为 Notion 标题，确保中文显示
        notion_title = ev.display_name_zh or ev.name

        props: dict = {
            title_prop_name:        {"title": [{"text": {"content": notion_title[:200]}}]},
            "Event ID":             txt(ev.event_id),
            "Original Name":        txt(ev.original_name),
            "Organizer":            txt(ev.organizer),
            "Start Date":           dt(ev.start_date),
            "End Date":             dt(ev.end_date),
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
            "Discovered At":        dt(ev.discovered_at),
            "Registration URL":     safe_url(ev.registration_url),
            "Source URL":           safe_url(ev.source_url),
        }
        return props

    def _upsert_to_notion(self) -> None:
        notion_token = os.environ.get("NOTION_TOKEN", "")
        notion_db_id = os.environ.get("NOTION_DATABASE_ID", "")

        if not notion_token or not notion_db_id:
            logger.info("[Phase 6] Notion 密钥未就绪，跳过 Upsert。")
            logger.info(
                f"  NOTION_TOKEN={'已设置' if notion_token else '未设置'}, "
                f"NOTION_DATABASE_ID={'已设置' if notion_db_id else '未设置'}"
            )
            return

        headers = self._notion_headers(notion_token)
        logger.info(
            f"[Phase 6] Notion Upsert: {len(self._events)} 个事件 "
            f"(并发×3, 间隔 {self._NOTION_BASE_INTERVAL}s + 主动整形 {self._NOTION_TRAFFIC_SHAPE_DELAY}s)"
        )

        # 步骤 1: 确保数据库字段完整，并获取实际 title 属性名
        title_prop_name = self._ensure_notion_db_properties(notion_db_id, headers)
        if title_prop_name is None:
            logger.error("[Phase 6] 数据库初始化失败，跳过 Upsert。")
            return

        # ── 线程安全的主动流量整形锁 ──────────────────────
        import threading
        rate_lock = threading.Lock()
        last_request_time = [0.0]

        def _notion_api_request(method: str, url: str, **kwargs) -> requests.Response:
            """底层 Notion API 请求 — 由 tenacity 装饰器管理重试。

            404 等不可重试错误直接抛出 NotionAPIError（不被 retry 捕获）。
            429/5xx/网络异常由 tenacity wait_random_exponential 管理退避。
            """
            # 主动流量整形：确保每次请求间隔 ≥ _NOTION_BASE_INTERVAL（含抖动）
            with rate_lock:
                jitter = self._NOTION_BASE_INTERVAL * 0.15 * (random.random() * 2 - 1)
                interval = max(0.1, self._NOTION_BASE_INTERVAL + jitter)
                elapsed = time.monotonic() - last_request_time[0]
                if elapsed < interval:
                    time.sleep(interval - elapsed)
                last_request_time[0] = time.monotonic()

            resp = requests.request(method, url, headers=headers, timeout=15, **kwargs)

            # 可重试错误：抛出 NotionAPIError → tenacity 捕获并退避
            if resp.status_code == 429:
                raise self.NotionAPIError(
                    429,
                    f"Rate limited",
                    resp.text[:300],
                )
            if resp.status_code >= 500:
                raise self.NotionAPIError(
                    resp.status_code,
                    f"Server error",
                    resp.text[:300],
                )

            # 其他非 2xx（如 400/401/403/404）不可重试，直接返回
            return resp

        # 应用 tenacity 装饰器：指数退避 + 随机抖动
        _notion_api_request = self._retry_on_notion_rate_limit(_notion_api_request)

        def _upsert_one(ev: EventItem) -> str:
            """返回 "created" / "updated" / "failed"

            每次操作包含 1-2 个 API 调用（查询 + 创建/更新）。
            每次 API 调用后主动 time.sleep(_NOTION_TRAFFIC_SHAPE_DELAY) 平滑曲线。
            """
            try:
                # 查重：查询数据库
                qr = _notion_api_request(
                    "POST",
                    f"{self._NOTION_API}/databases/{notion_db_id}/query",
                    json={"filter": {"property": "Event ID", "rich_text": {"equals": ev.event_id}}},
                )
                time.sleep(self._NOTION_TRAFFIC_SHAPE_DELAY)  # 主动流量整形

                if qr.status_code != 200:
                    logger.warning(
                        f"  Notion 查询失败 HTTP {qr.status_code}: "
                        f"{qr.text[:200]} | Event: {ev.name[:40]}"
                    )
                    return "failed"

                results = qr.json().get("results", [])
                page_props = self._build_notion_page_props(ev, title_prop_name)

                if results:
                    page_id = results[0]["id"]
                    ur = _notion_api_request(
                        "PATCH",
                        f"{self._NOTION_API}/pages/{page_id}",
                        json={"properties": page_props},
                    )
                    time.sleep(self._NOTION_TRAFFIC_SHAPE_DELAY)  # 主动流量整形

                    if ur.status_code not in (200, 201):
                        logger.warning(
                            f"  Notion 更新失败 HTTP {ur.status_code}: "
                            f"{ur.text[:200]} | Event: {ev.name[:40]}"
                        )
                        return "failed"
                    logger.debug(f"  更新: {ev.name}")
                    return "updated"
                else:
                    cr = _notion_api_request(
                        "POST",
                        f"{self._NOTION_API}/pages",
                        json={"parent": {"database_id": notion_db_id}, "properties": page_props},
                    )
                    time.sleep(self._NOTION_TRAFFIC_SHAPE_DELAY)  # 主动流量整形

                    if cr.status_code not in (200, 201):
                        logger.warning(
                            f"  Notion 创建失败 HTTP {cr.status_code}: "
                            f"{cr.text[:200]} | Event: {ev.name[:40]}"
                        )
                        return "failed"
                    logger.debug(f"  新建: {ev.name}")
                    return "created"
            except self.NotionAPIError as exc:
                # tenacity 重试耗尽后抛出 → 标记失败
                logger.warning(
                    f"  Upsert 重试耗尽 [HTTP {exc.status_code}]: "
                    f"{exc.message} | Event: {ev.name[:40]}"
                )
                return "failed"
            except Exception as exc:
                logger.warning(f"  Upsert 异常 [{ev.name[:40]}]: {exc}")
                return "failed"

        created = updated = failed = 0
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(_upsert_one, ev): ev for ev in self._events}
            for fut in as_completed(futures):
                try:
                    result = fut.result()
                    if result == "created":
                        created += 1
                    elif result == "updated":
                        updated += 1
                    else:
                        failed += 1
                except Exception as exc:
                    failed += 1
                    logger.debug(f"  Upsert 线程异常: {exc}")

        logger.info(f"[Phase 6] 完成: 新建={created} 更新={updated} 失败={failed}")
        if failed > 0:
            logger.warning(
                f"[Phase 6] {failed}/{len(self._events)} 个事件同步失败，"
                f"请检查 Notion API 密钥和数据库权限"
            )

    # ── Phase 7: .ics 日历生成 ───────────────────────────

    def _generate_ics(self) -> Optional[str]:
        """生成 ICS 日历文件，返回文件路径。失败或空事件时返回 None。"""
        if not self._events:
            return None

        cal = Calendar()
        cal.add("prodid", "-//ESG Event Radar//esg_event_radar//EN")
        cal.add("version", "2.0")
        cal.add("calscale", "GREGORIAN")
        cal.add("method", "PUBLISH")
        cal.add("x-wr-calname", "ESG 全球会议日历")
        cal.add("x-wr-caldesc", "自动扫描全球 ESG 与可持续发展会议，高管参会决策参考")

        added = 0
        for ev in self._events:
            try:
                ical = ICalEvent()
                ical.add("summary", ev.name)
                ical.add("uid", f"{ev.event_id}@esg-event-radar")

                # 解析日期
                dt_start = self._parse_ics_date(ev.start_date)
                dt_end = self._parse_ics_date(ev.end_date) if ev.end_date != ev.start_date else dt_start

                if dt_start is None:
                    # 日期未知的活动仍写入，但不设 DTSTART
                    ical.add("description", self._build_ics_description(ev))
                    cal.add_component(ical)
                    added += 1
                    continue

                ical.add("dtstart", dt_start)
                ical.add("dtend", dt_end)

                # 地点
                location_parts = [p for p in [ev.city, ev.country, ev.venue] if p]
                if location_parts:
                    ical.add("location", ", ".join(location_parts))

                # 描述
                ical.add("description", self._build_ics_description(ev))

                # 报名链接
                if ev.registration_url and "news.google.com" not in ev.registration_url:
                    ical.add("url", ev.registration_url)

                # 分类
                if ev.topics:
                    ical.add("categories", [t[:50] for t in ev.topics[:5]])

                cal.add_component(ical)
                added += 1
            except Exception as exc:
                logger.debug(f"[Phase 7] 事件 {ev.name[:40]} 写入失败: {exc}")

        try:
            OUTPUT_ICS_PATH.write_bytes(cal.to_ical())
            logger.info(f"[Phase 7] .ics 生成完成: {added}/{len(self._events)} 个事件 → {OUTPUT_ICS_PATH}")
            return str(OUTPUT_ICS_PATH)
        except Exception as exc:
            logger.error(f"[Phase 7] .ics 写入失败: {exc}")
            return None

    @staticmethod
    def _parse_ics_date(date_str: str) -> Optional[date]:
        """将 YYYY-MM-DD 字符串转为 date 对象。"""
        if not date_str or date_str in ("unknown", ""):
            return None
        try:
            return date.fromisoformat(date_str)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _build_ics_description(ev: "EventItem") -> str:
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

    # ── Phase 8: 钉钉精简卡片推送 ────────────────────────

    def push_to_dingtalk(self) -> None:
        webhook = os.environ.get("DINGTALK_WEBHOOK_RADAR", "")
        if not webhook:
            logger.info("[Phase 8] DINGTALK_WEBHOOK_RADAR 未配置，跳过推送。")
            return
        if not self._events:
            logger.info("[Phase 8] 无事件，静默阻断推送。")
            return

        # 按 exec_value_score 降序，最多推送 Top 10
        top_events = sorted(self._events, key=lambda e: e.exec_value_score, reverse=True)[:10]

        # 此时才做 Google News URL 解密（仅针对入选事件，控制请求量）
        for ev in top_events:
            if ev.registration_url and "news.google.com" in ev.registration_url:
                ev.registration_url = resolve_news_url(ev.registration_url)
            if "news.google.com" in ev.source_url:
                ev.source_url = resolve_news_url(ev.source_url)

        lines = [f"## 🌍 ESG 全球会议预警周报 ({self._run_ts[:10]})\n"]
        lines.append(f"> 本周扫描 {len(self._events)} 场活动，以下为高管优先关注清单：\n")

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

        notion_db_id = os.environ.get("NOTION_DATABASE_ID", "")
        notion_link = (
            f"https://www.notion.so/{notion_db_id.replace('-', '')}"
            if notion_db_id else "#"
        )
        lines.append(f"\n> 📓 完整活动库: [Notion 数据库]({notion_link})")

        content = "\n".join(lines)
        # 钉钉 markdown 消息限 20000 字节
        if len(content.encode("utf-8")) > 19000:
            content = content[:6000] + "\n\n> ⚠️ 内容过长，已截断。完整列表见 Notion。"

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"ESG 会议预警 {self._run_ts[:10]}",
                "text": content,
            },
        }
        try:
            resp = requests.post(
                webhook,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=10,
            )
            logger.info(f"[Phase 8] 钉钉推送完成: {resp.text[:100]}")
        except Exception as exc:
            logger.error(f"[Phase 8] 钉钉推送失败: {exc}")

    # ── 主入口 ────────────────────────────────────────────

    def run(self, no_push: bool = False) -> None:
        t0 = time.monotonic()
        logger.info("══ ESG Event Radar v1.3 | 周频扫描启动 ══")

        # Phase 0 (新): 上游供料摄入层 — sourcing_engine 抓取 + 脱壳
        self._run_phase0_ingestion()

        # Phase 0 (原): 加载源清单
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