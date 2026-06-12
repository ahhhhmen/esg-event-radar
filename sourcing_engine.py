"""
sourcing_engine.py
Phase 0 — 上游供料摄入层 (Ingestion Layer)
轻量级动态矩阵供料：YAML 配置驱动，高信噪比纯文本输出。

核心技术栈:
  - YAML   → 配置驱动，无硬编码爬虫
  - requests + feedparser → RSS 抓取
  - BeautifulSoup → HTML 深度脱壳
  - 时间窗口过滤 + limit 截断 → 控制算力开销
"""

import sys
import os
import logging
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import List, Dict, Optional
from urllib.parse import quote

import yaml
import requests
import feedparser
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ───────────────────────── 配置常量 ─────────────────────────

CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "config", "sources.yaml"
)
GOOGLE_NEWS_RSS_BASE = "https://news.google.com/rss/search?q="
USER_AGENT = (
    "Mozilla/5.0 (compatible; ESG-EventRadar/1.0; "
    "+https://github.com/ahhhhmen/esg_event_radar)"
)
REQUEST_TIMEOUT = 30  # 秒

# 需要完全分解（含内容）的标签
DECOMPOSE_TAGS = [
    "script", "style", "img", "figure", "figcaption",
    "nav", "footer", "header", "iframe", "noscript",
    "form", "input", "button", "select", "textarea",
    "svg", "canvas", "video", "audio", "source",
]
# 仅解除标签保留文本（如超链接文字）
UNWRAP_TAGS = ["a", "span", "b", "strong", "i", "em", "u", "code", "pre"]


# ───────────────────────── 核心 API ─────────────────────────


def load_sources(config_path: str = CONFIG_PATH) -> List[Dict]:
    """
    读取 config/sources.yaml，仅返回 enabled: true 的数据源。
    """
    with open(config_path, "r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh)

    all_sources = config.get("sources", [])
    enabled = [s for s in all_sources if s.get("enabled", False)]

    logger.info(
        "加载 sources.yaml：共 %d 源，激活 %d 个",
        len(all_sources), len(enabled),
    )
    return enabled


def fetch_rss(source: Dict) -> List[Dict]:
    """
    针对 google_news_rss 类型源：
    1. 用 urllib.parse.quote 对 query 做 URL-encode
    2. 构建 Google News RSS URL 并发起 GET
    3. 调用 clean_and_strip 做深度脱壳
    4. 返回标准化 List[Dict]
    """
    source_id = source["id"]
    raw_query = source["query"]
    time_window = source.get("time_window", 14)
    limit = source.get("limit", 50)

    # 压缩空白字符后做 URL-encode
    query_compact = " ".join(raw_query.split())
    query_encoded = quote(query_compact, safe="")

    # 拼接完整 RSS URL（hl/gl/ceid 固定为英文美国）
    url = (
        f"{GOOGLE_NEWS_RSS_BASE}{query_encoded}"
        f"&hl=en-US&gl=US&ceid=US:en"
    )

    headers = {"User-Agent": USER_AGENT}
    logger.info("[%s] GET %s…", source_id, url[:180])

    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("[%s] 网络请求失败: %s", source_id, exc)
        return []

    return clean_and_strip(resp.text, source_id, time_window, limit)


def clean_and_strip(
    rss_xml: str, source_id: str, time_window: int, limit: int
) -> List[Dict]:
    """
    深度脱壳清洗 —— 防幻觉核心屏障。

    处理管线:
      1. feedparser 解析 RSS/XML
      2. 逐 entry 提取 title + description → HTML 脱壳
      3. 剪除所有非文本标签，保留纯净文本
      4. 时间窗口过滤 & limit 截断
      5. 输出标准化 List[Dict]

    返回字段:
      source_id      — 来源标识
      title          — 脱壳后标题纯文本
      link           — Google News 原始链接
      real_url       — 穿透重定向后的真实目标 URL
      published_date — ISO 8601 UTC 时间
      clean_text     — 高信噪比纯文本 (title + description 脱壳拼接)
    """
    feed = feedparser.parse(rss_xml)

    # "bozo" 为真但仍有条目时继续处理（部分解析成功）
    if not feed.entries:
        bozo_msg = str(feed.bozo_exception) if feed.bozo else "无条目"
        logger.warning("[%s] RSS 无有效条目: %s", source_id, bozo_msg)
        return []

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=time_window)

    results: List[Dict] = []
    for entry in feed.entries:
        # ── 时间解析 ──
        published_dt = _parse_published(entry)
        if published_dt is None:
            continue
        if published_dt < cutoff:
            continue

        # ── HTML 脱壳 ──
        title_clean = _strip_html(getattr(entry, "title", "") or "")
        desc_clean = _strip_html(getattr(entry, "description", "") or "")

        # 拼接为纯文本（title 权重高，放置在前）
        clean_text = f"{title_clean} {desc_clean}".strip()
        if not clean_text:
            continue

        link = getattr(entry, "link", "") or ""
        # 穿透 Google News 重定向，提取真实目标 URL
        real_url = _resolve_real_url(link) if link else ""

        results.append({
            "source_id": source_id,
            "title": title_clean,
            "link": link,
            "real_url": real_url,
            "published_date": published_dt.isoformat(),
            "clean_text": clean_text,
        })

        if len(results) >= limit:
            break

    logger.info(
        "[%s] 脱壳完成：保留 %d 条（窗口 %dd · limit %d）",
        source_id, len(results), time_window, limit,
    )
    return results


# ───────────────────────── 内部工具函数 ─────────────────────────


def _strip_html(html_text: str) -> str:
    """
    对 HTML 片段进行严格脱壳：
      - COMPOSE_TAGS: 连同内容一并销毁
      - UNWRAP_TAGS: 仅移除标签保留内部文本（如超链接文字）
      - 其余标签默认保留结构但 get_text 仅取文本
    """
    if not html_text:
        return ""

    soup = BeautifulSoup(html_text, "html.parser")

    # 1️⃣ 完全分解（含内部内容）
    for tag_name in DECOMPOSE_TAGS:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    # 2️⃣ 仅解壳（保留内部文本）
    for tag_name in UNWRAP_TAGS:
        for tag in soup.find_all(tag_name):
            tag.unwrap()

    # 3️⃣ 提取纯文本
    return soup.get_text(separator=" ", strip=True)


def _resolve_real_url(google_url: str) -> str:
    """
    穿透 Google News 重定向链接，获取目标网站的真实 URL。

    策略:
      - 发起 GET 请求并跟随重定向 (allow_redirects=True)
      - 返回最终目标 URL (response.url)
      - 任何异常（超时、连接拒绝等）均 fallback 返回原始 google_url
    """
    if not google_url:
        return ""

    headers = {"User-Agent": USER_AGENT}

    try:
        resp = requests.get(
            google_url,
            headers=headers,
            allow_redirects=True,
            timeout=10,
        )
        # 如果最终 URL 仍是 Google News 域（未穿透成功），返回原始链接
        if "news.google.com" in resp.url:
            logger.warning(
                "链接未穿透 Google 重定向，回退原始链接: %s…",
                google_url[:100],
            )
            return google_url
        return resp.url

    except requests.RequestException as exc:
        logger.warning(
            "链接解析失败 (%s)，Falling back to original link: %s…",
            exc, google_url[:100],
        )
        return google_url


def _parse_published(entry) -> Optional[datetime]:
    """
    从 feedparser entry 提取 UTC datetime。
    优先级: published_parsed (struct_time) → published (RFC 2822)
    """
    # 路径 A: feedparser 已解析的结构化时间
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        except (TypeError, ValueError, OverflowError):
            pass

    # 路径 B: 原始 published 字符串 → RFC 2822 解析
    if hasattr(entry, "published") and entry.published:
        try:
            return parsedate_to_datetime(entry.published)
        except (ValueError, TypeError):
            pass

    # 路径 C: pubDate（部分 RSS 2.0 源使用此字段）
    if hasattr(entry, "pubDate") and entry.pubDate:
        try:
            return parsedate_to_datetime(entry.pubDate)
        except (ValueError, TypeError):
            pass

    return None


# ───────────────────────── 调试入口 ─────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    sources = load_sources()
    target = next((s for s in sources if s["id"] == "global_esg_summits"), None)
    if target is None:
        print("❌ 未找到 global_esg_summits 源（请确认 config/sources.yaml 中存在且 enabled: true）")
        sys.exit(1)

    print(f"🔍 正在抓取: {target['id']}")
    results = fetch_rss(target)

    if not results:
        print("⚠️  未获取到任何条目（可能网络问题或时间窗口内暂无符合条件的结果）")
        sys.exit(0)

    # ── 对第一条数据做链接穿透解析展示 ──
    first = results[0]

    print(f"\n✅ 共获取 {len(results)} 条\n")
    print("=" * 72)
    print("📰  第一条数据 · clean_text（纯文本输出）：")
    print("=" * 72)
    print(first["clean_text"])
    print("-" * 72)
    print(f"标题         : {first['title']}")
    print(f"发布日期     : {first['published_date']}")
    print(f"来源 ID      : {first['source_id']}")
    print(f"原始 link    : {first['link']}")
    print(f"穿透 real_url: {first.get('real_url', '（无）')}")
    print("=" * 72)