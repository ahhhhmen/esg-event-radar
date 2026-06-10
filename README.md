# ESG Event Radar · 全球 ESG 活动雷达

[![Event Radar Daily Sync](https://github.com/ahhhhmen/esg_event_radar/actions/workflows/run.yml/badge.svg)](https://github.com/ahhhhmen/esg_event_radar/actions/workflows/run.yml)

A multilingual intelligence agent that monitors global ESG, sustainability, and responsible-sourcing events. Scrapes news feeds and organizational calendars, extracts structured event data via LLM, scores each event by executive relevance, and syncs results to [Notion](https://www.notion.so/) with a calendar subscription file.

## Features

- **Multi-source ingestion** — 40+ sources across three tracks:
  - Track A: HTML calendar scraping (Tier 1 orgs: UNGC, WBCSD, CDP, SBTi, GRI, RBA, BSR, PRI)
  - Track B: Vertical media RSS (ESG Today, Responsible Investor, Carbon Brief, Bloomberg Green)
  - Track C: Google News targeted queries (Tier 1 initiatives, summit brands, mining ESG, Chinese/French media)
- **LLM extraction** — DeepSeek API extracts structured `EventItem` JSON from raw news articles
- **5-dimension executive-value scoring** — Authority, C-suite attendance, regulatory exposure, competitive intel, network access
- **Cross-source deduplication** — URL + title + semantic similarity (supports English, Chinese, Indonesian, French)
- **Notion database upsert** — Full-field sync with auto-created Notion properties
- **ICS calendar generation** — `esg_events.ics` subscription file for calendar apps
- **DingTalk alerts** — Top-10 event push notifications (optional)
- **Automated weekly sync** — GitHub Actions cron runs every Monday 06:00 Beijing time, with manual trigger support

## Architecture

```
sources.yaml   ←  feeds & calender URLs
    │
    ▼
┌─────────────────────────────────────────────┐
│  Phase 0: Load active sources               │
│  Phase 1a: HTML Calendar crawl (Track A)    │
│  Phase 1:  RSS & Google News fetch (B + C) │
│  Phase 3:  Content extraction (GET + fallback)│
│  Phase 4:  LLM extraction → EventItem[]     │
│  Phase 4.3: Cross-source dedup              │
│  Phase 6:  Notion database upsert           │
│  Phase 7:  .ics calendar file generation    │
│  Phase 8:  DingTalk top-10 push             │
└─────────────────────────────────────────────┘
    │
    ▼
  Notion DB  +  esg_events.ics  +  DingTalk
```

## Quick Start

### Prerequisites

- Python 3.10+
- A [DeepSeek API key](https://platform.deepseek.com/)
- (Optional) A Notion integration token and database ID

### 1. Clone & configure

```bash
git clone https://github.com/ahhhhmen/esg_event_radar.git
cd esg_event_radar
cp .env.example .env
```

Edit `.env` and fill in at minimum:

```ini
DEEPSEEK_API_KEY=sk-your-deepseek-api-key-here
```

For Notion sync, also set:

```ini
NOTION_TOKEN=secret_your_notion_integration_token
NOTION_DATABASE_ID=your_notion_database_id_here
```

### 2. Install & run

```bash
pip install -r requirements.txt
python main.py
```

The agent will scrape sources, extract events, score them, and (if configured) upsert to Notion. The calendar file `esg_events.ics` is written to the project root.

## GitHub Actions

The workflow `.github/workflows/run.yml` runs **once a week** on a cron schedule:

| UTC | Beijing (UTC+8) |
|-----|-----------------|
| Sun 22:00 | Mon 06:00 |

Each run performs three steps:

1. Installs dependencies
2. Runs `python main.py`
3. Commits updated `esg_events.ics` back to the repository

### Where results go

| Output | Destination | Details |
|--------|-------------|---------|
| **Notion database** | Your Notion workspace | Full-field upsert to the database specified by `NOTION_DATABASE_ID`. All events are synced with dedup keys, so re-runs are idempotent. |
| **ICS calendar file** | `esg_events.ics` (repo root) + `output/esg_events.ics` | Auto-committed & pushed by the workflow after each run. Subscribe your calendar app to the raw file URL on `main` branch for live updates. |
| **DingTalk push** (optional) | DingTalk group chat | Top 10 highest-scored events are pushed when `DINGTALK_WEBHOOK_RADAR` is configured. |
| **Run logs** | GitHub Actions → workflow run page | Full pipeline logs (phase timings, event counts, errors) visible in the Actions tab. |

### Required secrets

Set these in **GitHub → Settings → Secrets and variables → Actions**:

| Secret | Description |
|--------|-------------|
| `DEEPSEEK_API_KEY` | DeepSeek API key |
| `NOTION_TOKEN` | Notion integration token |
| `NOTION_DATABASE_ID` | Target Notion database ID |

## Scoring Model

Each event is scored 1–5 on five dimensions:

| Dimension | What it measures |
|-----------|-----------------|
| **D1** Organizational Authority | Hosted by UNGC/GRI/ISSB/SBTi vs. regional organizer |
| **D2** C-Suite Attendance | CEO/CSO/Board Chair speakers vs. analyst-level audience |
| **D3** Regulatory Exposure | Linked to CSRD/CBAM/ISSB vs. voluntary disclosure only |
| **D4** Competitive Intelligence | Fortune 500 peers attending vs. niche audience |
| **D5** Network Access | Invite-only elite network vs. mass-expo format |

See [`scoring_criteria.md`](scoring_criteria.md) for the full rubric the LLM uses at inference time.

## Supported Languages

The agent handles news articles in:

- English 🇬🇧
- Chinese 🇨🇳 (中文)
- Indonesian 🇮🇩 (Bahasa Indonesia)
- French 🇫🇷 (Français)

All extracted event names are normalized to Chinese display names with English standard names for deduplication.

## Testing

```bash
# End-to-end mini test (skips Notion writes; requires DeepSeek key)
python test_e2e_mini.py --no-notion

# Notion API connectivity test
python test_notion_connection.py

# Deduplication pipeline backtest
python test_dedup_backtest.py
```

## Configuration

Edit [`sources.yaml`](sources.yaml) to add, remove, or disable feeds. Each source has:

```yaml
- name: "ESG Today"
  url: "https://www.esgtoday.com/feed/"
  type: rss              # rss | google_news | html_calendar
  tier: 3                # 1=Tier1 org, 2=major operator, 3=media/think tank
  region: global         # global | apac | emea | americas | cn
  org: "ESG Today"
  tags: ["ESG-news", "conference-announcement"]
  active: true           # set false to disable
```

## Project Structure

```
├── main.py                 # CLI entry point
├── event_radar_agent.py    # Core agent (all phases)
├── sources.yaml            # Feed & calendar source config
├── scoring_criteria.md     # LLM scoring rubric (prompt anchor)
├── schemas/
│   ├── __init__.py
│   └── event.py            # Pydantic EventItem model
├── test_e2e_mini.py        # Integration test
├── test_dedup_backtest.py  # Dedup pipeline benchmark
├── test_notion_connection.py # Notion API connectivity test
├── requirements.txt
├── .env.example            # Environment template
├── .gitignore
└── .github/workflows/run.yml  # GitHub Actions workflow
```

## License

MIT