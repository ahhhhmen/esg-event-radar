#!/usr/bin/env python3
"""
全球 ESG 与可持续发展会议/活动动态预警系统 (Event Radar) v1.1
══════════════════════════════════════════════════════════════════════════════
架构层级
─────────
  EventRadarAgent.run()
    Phase 1  : 三轨 RSS 抓取（sources.yaml 硬编码源清单）
               轨道 B — 行业媒体 RSS（ESGToday / RI / CarbonBrief / Bloomberg）
               轨道 C — Google News 定向查询（type: google_news，含 URL 解密）
               轨道 A — html_calendar 爬虫（待实现，active: false）
    Phase 2  : 去重（URL + 标题双键）
    Phase 3  : 深度正文提取（ContentExtractor）
    Phase 4  : LLM 语义抽取 → EventItem JSON Array
               （基于 scoring_criteria.md 五维评分锚点）
    Phase 5  : 零数据熔断 / 无事件静默阻断
    Phase 6  : [STUB] Notion Database Upsert
    Phase 7  : [STUB] .ics 日历订阅文件生成
    Phase 8  : 钉钉群精简卡片推送

继承自 esg_intelligence_agent.py 的核心机制
─────────────────────────────────────────
  · NewsFetcher          — RSS 抓取引擎
  · resolve_news_url     — Google News 重定向解密（复用基座逻辑）
  · ContentExtractor     — 深度正文提取
  · _send_system_alert   — FATAL 级熔断钉钉报警
  · 零数据熔断           — 抓取量为 0 时触发 FATAL 并退出
  · 无事件静默阻断       — LLM 提取结果为空时静默退出，不推送
══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import hashlib
import html
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
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

load_dotenv()

from schemas.event import EventItem

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
OUTPUT_ICS_PATH = Path(__file__).parent / "esg_events.ics"

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

    TIMEOUT = 5
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

    # ── Phase 1: 垂直 RSS 抓取 ────────────────────────────

    def _fetch_all_sources(self) -> None:
        from datetime import timedelta
        cutoff_rss    = datetime.now(timezone.utc) - timedelta(days=FETCH_WINDOW_DAYS_RSS)
        cutoff_gnews  = datetime.now(timezone.utc) - timedelta(days=FETCH_WINDOW_DAYS_GNEWS)

        active = [s for s in self._sources if s.get("active", True)]
        rss_sources = [s for s in active if s.get("type") in ("rss", "google_news")]

        for idx, src in enumerate(rss_sources, 1):
            src_type = src.get("type", "rss")
            is_google = src_type == "google_news"
            url = src["url"]
            logger.info(
                f"[{idx:>2}/{len(rss_sources)}] [{src_type}] {src['name']} ({url[:65]})"
            )

            # Google News URL 解密在 Phase 1 会大幅拖慢速度（每条 HEAD 请求 ~1s）
            # 改为：Phase 1 保留原始 Google News URL，仅在输出层（Notion/DingTalk）按需解密
            raw_items = NewsFetcher.fetch(url, resolve_google=False)

            cutoff = cutoff_gnews if is_google else cutoff_rss

            injected = 0
            for item in raw_items:
                if item["url"] in self._seen_urls:
                    continue
                title_key = item["title"].strip().lower()
                if title_key in self._seen_titles:
                    continue

                parsed_date = parse_rss_date(item["date"])
                if parsed_date and parsed_date < cutoff:
                    continue

                self._seen_urls.add(item["url"])
                self._seen_titles.add(title_key)
                self._raw_items.append({
                    **item,
                    "source_org": src.get("org", src["name"]),
                    "source_tags": src.get("tags", []),
                    "source_tier": src.get("tier", 3),
                    "parsed_date": parsed_date,
                })
                injected += 1

            logger.info(f"  → 注入 {injected} 条（本源共 {len(raw_items)} 条）")
            # Google News 查询间隔稍长，避免限流
            time.sleep(1.5 if is_google else 0.8)

        logger.info(f"[Phase 1] 完成。共收集 {len(self._raw_items)} 条原始条目")

    # ── Phase 3: 深度正文提取（并发） ────────────────────────

    def _enrich_content(self) -> None:
        logger.info(f"[Phase 3] 正文并发提取: {len(self._raw_items)} 条（max_workers=10）")

        def _fetch_one(item: dict) -> None:
            body = ContentExtractor.extract(item["url"])
            if body:
                item["body"] = body
                return
            # ── 回退策略 ──
            # Google News RSS 的 description 只含链接，正文提取必然失败。
            # 层次回退: 正文 → RSS description → source_href 域名线索
            rss_desc = item.get("description", "")
            if rss_desc and len(rss_desc) > 30:
                item["body"] = f"[来源:RSS摘要] {rss_desc}"[:300]
            elif item.get("source_href"):
                item["body"] = f"[来源站点: {item['source_href']}] 标题: {item.get('title', '')}"[:300]
            else:
                item["body"] = item.get("title", "")[:300]

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(_fetch_one, it): it for it in self._raw_items}
            done = 0
            for fut in as_completed(futures):
                done += 1
                if done % 20 == 0:
                    logger.info(f"  进度: {done}/{len(self._raw_items)}")
                try:
                    fut.result()
                except Exception as exc:
                    logger.debug(f"  正文提取异常: {exc}")

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
  "event_id": "用 name + start_date 生成的唯一标识（占位，系统会覆盖）",
  "name": "活动全称（英文优先）",
  "organizer": "主办方",
  "start_date": "YYYY-MM-DD（无法确定时填 unknown）",
  "end_date": "YYYY-MM-DD",
  "timezone": "IANA timezone 或 UTC",
  "is_recurring": true/false,
  "format": "in-person|online|hybrid|unknown",
  "city": "城市或 null",
  "country": "ISO alpha-2 或 null",
  "venue": "场馆或 null",
  "registration_url": "报名链接或 null",
  "registration_deadline": "YYYY-MM-DD 或 null",
  "fee_tier": "free|paid|invite-only|unknown",
  "topics": ["主题标签列表"],
  "audience": ["受众标签列表"],
  "agenda_highlights": "议程亮点≤120字",
  "exec_value_score": 1-5,
  "exec_value_rationale": "评分依据≤80字，引用维度代码如 D1/D2",
  "source_url": "原始 RSS 链接",
  "source_org": "来源机构",
  "discovered_at": "{self._run_ts}",
  "raw_snippet": "原始摘要前100字"
}}

## 待处理条目
{items_text}

## 语言处理
- 条目可能为英语、中文或印尼语（Bahasa Indonesia）
- 无论原文语言，输出 JSON 中 name/organizer/city 等字段均使用英文或中英双语
- 印尼语关键词参考：konferensi=conference, forum=forum, pertemuan=meeting, keberlanjutan=sustainability

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
        logger.info(f"[Phase 4] LLM 提取: {len(self._raw_items)} 条 → {len(batches)} 批")

        for batch_idx, batch in enumerate(batches, 1):
            logger.info(f"  批次 {batch_idx}/{len(batches)} ({len(batch)} 条)...")
            prompt = self._build_extraction_prompt(batch)
            try:
                resp = self._llm.chat.completions.create(
                    model=DEEPSEEK_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=8192,
                )
                raw_json = resp.choices[0].message.content.strip()
                # 剥除可能的 markdown 代码块包装
                raw_json = re.sub(r"^```(?:json)?\s*", "", raw_json)
                raw_json = re.sub(r"\s*```$", "", raw_json)

                parsed: list[dict] = json.loads(raw_json)
                for obj in parsed:
                    # 用真实算法覆盖 LLM 生成的 event_id
                    obj["event_id"] = make_event_id(
                        obj.get("name", ""), obj.get("start_date", "")
                    )
                    # 容错：确保必填 int 字段有效
                    score = obj.get("exec_value_score", 3)
                    obj["exec_value_score"] = max(1, min(5, int(score)))
                    # 容错：确保必填 str 字段不为 None（LLM 偶尔返回 null）
                    obj["source_url"] = obj.get("source_url") or obj.get("registration_url") or ""
                    obj["source_org"] = obj.get("source_org") or "Unknown"
                    obj["organizer"] = obj.get("organizer") or obj.get("source_org") or "Unknown"

                    try:
                        event = EventItem(**obj)
                        if event.exec_value_score <= 2:
                            logger.debug(f"  低分过滤（{event.exec_value_score}分）: {event.name}")
                            continue
                        self._events.append(event)
                    except Exception as val_err:
                        logger.warning(f"  EventItem 校验失败（跳过）: {val_err} | {obj.get('name')}")

                logger.info(f"  批次 {batch_idx} 提取 {len(parsed)} 个事件")

            except json.JSONDecodeError as e:
                logger.error(f"  批次 {batch_idx} JSON 解析失败: {e}")
            except Exception as e:
                logger.error(f"  批次 {batch_idx} LLM 调用失败: {e}")
                time.sleep(5)

        logger.info(f"[Phase 4] 完成。共提取 {len(self._events)} 个有效事件")

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
        "Organizer":           {"rich_text": {}},
        "Start Date":          {"date": {}},
        "End Date":            {"date": {}},
        "Format":              {"select": {}},
        "City":                {"rich_text": {}},
        "Country":             {"rich_text": {}},
        "Registration URL":    {"url": {}},
        "Fee Tier":            {"select": {}},
        "Topics":              {"multi_select": {}},
        "Audience":            {"multi_select": {}},
        "Exec Value Score":    {"number": {"format": "number"}},
        "Exec Value Rationale":{"rich_text": {}},
        "Agenda Highlights":   {"rich_text": {}},
        "Source URL":          {"url": {}},
        "Source Org":          {"rich_text": {}},
        "Discovered At":       {"date": {}},
    }

    def _notion_headers(self, token: str) -> dict:
        return {
            "Authorization": f"Bearer {token}",
            "Notion-Version": self._NOTION_VERSION,
            "Content-Type": "application/json",
        }

    def _ensure_notion_db_properties(self, db_id: str, headers: dict) -> bool:
        """检查并补全数据库属性；返回 True 表示可继续操作。"""
        r = requests.get(f"{self._NOTION_API}/databases/{db_id}", headers=headers, timeout=15)
        if r.status_code != 200:
            logger.warning(f"[Phase 6] 读取数据库失败 {r.status_code}: {r.text[:200]}")
            return False

        existing = set(r.json().get("properties", {}).keys())
        to_add = {k: v for k, v in self._NOTION_REQUIRED_PROPS.items() if k not in existing}
        if not to_add:
            return True

        logger.info(f"[Phase 6] 补全缺失字段: {list(to_add.keys())}")
        pr = requests.patch(
            f"{self._NOTION_API}/databases/{db_id}",
            headers=headers,
            json={"properties": to_add},
            timeout=15,
        )
        if pr.status_code not in (200, 201):
            logger.warning(f"[Phase 6] 字段创建失败 {pr.status_code}: {pr.text[:200]}")
            return False
        return True

    @staticmethod
    def _build_notion_page_props(ev: "EventItem") -> dict:
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

        props: dict = {
            "Name":                 {"title": [{"text": {"content": ev.name[:200]}}]},
            "Event ID":             txt(ev.event_id),
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
            return

        headers = self._notion_headers(notion_token)
        logger.info(f"[Phase 6] Notion Upsert: {len(self._events)} 个事件")

        # 步骤 1: 确保数据库字段完整
        if not self._ensure_notion_db_properties(notion_db_id, headers):
            logger.error("[Phase 6] 数据库初始化失败，跳过 Upsert。")
            return

        created = updated = failed = 0
        for ev in self._events:
            try:
                # 步骤 2: 按 event_id 查重
                qr = requests.post(
                    f"{self._NOTION_API}/databases/{notion_db_id}/query",
                    headers=headers,
                    json={"filter": {"property": "Event ID", "rich_text": {"equals": ev.event_id}}},
                    timeout=15,
                )
                results = qr.json().get("results", [])
                page_props = self._build_notion_page_props(ev)

                if results:
                    # 步骤 3a: 更新已有页面
                    page_id = results[0]["id"]
                    requests.patch(
                        f"{self._NOTION_API}/pages/{page_id}",
                        headers=headers,
                        json={"properties": page_props},
                        timeout=15,
                    )
                    updated += 1
                    logger.debug(f"  更新: {ev.name}")
                else:
                    # 步骤 3b: 新建页面
                    requests.post(
                        f"{self._NOTION_API}/pages",
                        headers=headers,
                        json={"parent": {"database_id": notion_db_id}, "properties": page_props},
                        timeout=15,
                    )
                    created += 1
                    logger.debug(f"  新建: {ev.name}")

            except Exception as exc:
                logger.warning(f"  Upsert 失败 [{ev.name[:40]}]: {exc}")
                failed += 1

        logger.info(f"[Phase 6] 完成: 新建={created} 更新={updated} 失败={failed}")

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
        logger.info("══ ESG Event Radar v1.0 | 周频扫描启动 ══")

        # Phase 0: 加载源清单
        self._load_sources()

        # Phase 1: RSS 抓取
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
