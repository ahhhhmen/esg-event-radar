#!/usr/bin/env python3
"""
ESG Event Radar — 定时调度器
═══════════════════════════════════════════════════════
使用 APScheduler 在容器内实现定时扫描，启动时立即执行一次。

环境变量:
  SCHEDULE_INTERVAL_HOURS  扫描间隔（默认 12 小时）
  LOG_LEVEL                日志级别（默认 INFO）
"""
import os
import sys
import logging
from dotenv import load_dotenv

load_dotenv()

# ── 日志：输出到 stdout，docker logs 可直接捕获 ──────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("EventRadarScheduler")

# ── 定时任务 -------------------------------------------------
def run_event_radar_job():
    """扫描一次全球 ESG 活动。"""
    logger.info("═══ Event Radar 定时扫描开始 ═══")

    try:
        from event_radar_agent import EventRadarAgent
        agent = EventRadarAgent()
        agent.run(no_push=False)  # 生产模式：推送到 Notion + 钉钉
        logger.info("═══ Event Radar 扫描完成 ═══")
    except SystemExit as e:
        if e.code == 0:
            logger.info("═══ Event Radar 扫描完成（正常退出） ═══")
        else:
            logger.error(f"扫描异常退出，exit_code={e.code}")
    except Exception:
        logger.exception("定时任务执行过程中发生未捕获异常")

# ── 主入口 ---------------------------------------------------
def main():
    from apscheduler.schedulers.blocking import BlockingScheduler

    interval_hours = int(os.getenv("SCHEDULE_INTERVAL_HOURS", "12"))
    logger.info(f"调度器初始化，扫描间隔: 每 {interval_hours} 小时")

    # 1. 启动立即执行一次
    logger.info("启动时执行首次扫描...")
    run_event_radar_job()

    # 2. 注册定时任务
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_event_radar_job,
        "interval",
        hours=interval_hours,
        id="event_radar_sync",
    )

    logger.info(f"调度器已启动，下次扫描将在 {interval_hours} 小时后")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("调度器已安全关闭")

if __name__ == "__main__":
    main()