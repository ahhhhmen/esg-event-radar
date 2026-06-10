#!/usr/bin/env python3
"""ESG Event Radar — CLI 入口（供 GitHub Action / Docker 调度调用）"""

import sys
import argparse
from dotenv import load_dotenv

load_dotenv()

from event_radar_agent import EventRadarAgent


def main():
    parser = argparse.ArgumentParser(description="ESG Event Radar")
    parser.add_argument("--no-push", action="store_true",
                        help="跳过钉钉推送")
    args = parser.parse_args()

    agent = EventRadarAgent()
    try:
        agent.run(no_push=args.no_push)
    except SystemExit:
        pass
    except Exception:
        import logging
        logging.getLogger(__name__).exception("Agent.run() 未捕获异常")
        sys.exit(1)


if __name__ == "__main__":
    main()