#!/usr/bin/env python3
"""
极简 Notion API 连通性测试
- 插入一条测试活动到 Notion 数据库
- 打印完整的 HTTP 错误信息
"""
import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
NOTION_DB_ID = os.environ.get("NOTION_DATABASE_ID", "")
NOTION_API = "https://api.notion.com/v1"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

print("=" * 60)
print("Notion API 连通性测试")
print("=" * 60)
print(f"  TOKEN 已设置: {bool(NOTION_TOKEN)} (长度={len(NOTION_TOKEN)})")
print(f"  DATABASE_ID 已设置: {bool(NOTION_DB_ID)} (值={NOTION_DB_ID})")
print()

if not NOTION_TOKEN or not NOTION_DB_ID:
    print("❌ NOTION_TOKEN 或 NOTION_DATABASE_ID 未设置，无法测试。")
    exit(1)

# ── Step 1: 验证数据库可访问 ──────────────────────────────
print("[Step 1] 查询数据库元数据...")
try:
    r = requests.get(
        f"{NOTION_API}/databases/{NOTION_DB_ID}",
        headers=HEADERS,
        timeout=15,
    )
    print(f"  HTTP {r.status_code}")
    body = r.json()
    if r.status_code == 200:
        props = list(body.get("properties", {}).keys())
        print(f"  ✅ 数据库可访问，现有字段: {props}")
        print(f"  数据库标题: {body.get('title', [{}])[0].get('plain_text', 'N/A')}")
    else:
        print(f"  ❌ 数据库访问失败！")
        print(f"  完整响应: {json.dumps(body, indent=2, ensure_ascii=False)}")
        exit(1)
except Exception as e:
    print(f"  ❌ 请求异常: {e}")
    exit(1)

print()

# ── Step 2: 插入一条测试数据 ──────────────────────────────
print("[Step 2] 插入测试页面 (活动名='API 测试会议')...")

test_props = {
    "会议/活动": {
        "title": [{"text": {"content": "API 测试会议"}}]
    },
    "Event ID": {
        "rich_text": [{"text": {"content": "test-conn-001"}}]
    },
    "Organizer": {
        "rich_text": [{"text": {"content": "连通性测试"}}]
    },
    "Start Date": {
        "date": {"start": "2026-12-01"}
    },
    "End Date": {
        "date": {"start": "2026-12-02"}
    },
    "Format": {
        "select": {"name": "in-person"}
    },
    "Exec Value Score": {
        "number": 3
    },
}

payload = {
    "parent": {"database_id": NOTION_DB_ID},
    "properties": test_props,
}

try:
    r = requests.post(
        f"{NOTION_API}/pages",
        headers=HEADERS,
        json=payload,
        timeout=15,
    )
    print(f"  HTTP {r.status_code}")
    body = r.json()
    print(f"  完整响应: {json.dumps(body, indent=2, ensure_ascii=False)[:2000]}")

    if r.status_code in (200, 201):
        page_id = body.get("id", "N/A")
        page_url = body.get("url", "N/A")
        print(f"\n  ✅ 创建成功！")
        print(f"  Page ID: {page_id}")
        print(f"  Page URL: {page_url}")
    elif r.status_code == 400:
        print(f"\n  ❌ 400 Bad Request — 字段不匹配或参数错误")
        print(f"  请检查上方完整响应中的 'message' 和 'code' 字段")
    elif r.status_code == 401:
        print(f"\n  ❌ 401 Unauthorized — Token 无效或过期")
    elif r.status_code == 403:
        print(f"\n  ❌ 403 Forbidden — Token 无权访问该数据库（需在 Notion 连接设置中添加集成）")
    elif r.status_code == 404:
        print(f"\n  ❌ 404 Not Found — 数据库不存在或 ID 错误")
    else:
        print(f"\n  ❌ 未预期的状态码: {r.status_code}")

except Exception as e:
    print(f"  ❌ 请求异常: {type(e).__name__}: {e}")

print()
print("=" * 60)
print("测试完成")
print("=" * 60)