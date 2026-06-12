"""
ESG Event Radar — EventItem Schema
Pydantic v2 数据模型，作为 Notion Upsert 和 .ics 生成的主键数据结构。
"""

from __future__ import annotations

from typing import Literal, Optional
from pydantic import BaseModel, Field


class EventItem(BaseModel):
    """单条 ESG 会议/活动结构化条目。

    Upsert 主键: event_id = SHA256[:12](name + start_date)
    """

    # ── 核心身份 ──────────────────────────────────────────
    event_id: str = Field(description="SHA256[:12](standard_name_en+start_date)，Notion Upsert 去重主键")
    name: str = Field(description="活动展示名称，推荐格式 'English Name — 中文名称'")
    original_name: str = Field(
        default="",
        description="原始语言名称，保留源语言（英文/印尼文/法文/中文原文），用于 Notion 原文名称列",
    )
    standard_name_en: str = Field(
        default="",
        description=(
            "标准英文名，用于跨语言精确去重。无论原文什么语言，统一翻译为英文。"
            "【实体消解约束 v2 — Few-Shot 精细规则】"
            "规则1（强制剥离）：移除年份（2024-2028）、频次修饰词（Annual/Biannual/Biennial）、"
            "届数（8th/2nd/第X届/annuel）及所有标点。"
            "规则2（领域白名单保护）：以下核心机构简称是品牌不可分割部分，严禁剥离——"
            "UN、UNGC、SBTi、CDP、COP、WBCSD、GRI、PRI、RBA、IRMA、ISSB、IUCN、UNEP、ILO、WRI、TNC、RMI。"
            "规则3（媒体/主办方前缀剔除）：Reuters、Bloomberg、GreenBiz、S&P Global 等媒体/主办方前缀必须移除。"
            "规则4（反过度泛化）：若剥离后剩余名称仅由通用名词构成"
            "（如 'Annual Summit'、'Leaders Forum'、'Global Conference'、'Council Meeting'），"
            "则必须将核心机构简称加回以保持实体唯一性。"
            "Few-Shot 示例："
            "'Reuters Responsible Business Europe 2026' → 'Responsible Business Europe'；"
            "'SBTi Annual Summit 2026' → 'SBTi Summit'（Annual 强制剥离，SBTi 是白名单）；"
            "'WBCSD Council Meeting 2025' → 'WBCSD Council Meeting'（WBCSD 白名单，保留）；"
            "'UN Global Compact Leaders Summit 2026' → 'UN Global Compact Leaders Summit'（UNGC 白名单保护）；"
            "'Annual Summit' 这种纯泛化名称若出现，必须加回机构 → 'SBTi Summit'（而非 'Annual Summit'）"
        ),
    )
    display_name_zh: str = Field(
        default="",
        description="中文展示名，用于 Notion 显示。非中文活动名翻译为中文",
    )
    organizer: str = Field(description="主办方全称")

    # ── 时间 ─────────────────────────────────────────────
    start_date: str = Field(description="ISO 8601 YYYY-MM-DD")
    end_date: str = Field(description="ISO 8601 YYYY-MM-DD，单日活动与 start_date 相同")
    timezone: str = Field(default="UTC", description="IANA timezone，如 Asia/Shanghai")
    is_recurring: bool = Field(default=False, description="是否为年度例会")

    # ── 地点与形式 ────────────────────────────────────────
    format: Literal["in-person", "online", "hybrid", "unknown"] = Field(
        description="活动形式"
    )
    city: Optional[str] = Field(default=None, description="城市")
    country: Optional[str] = Field(default=None, description="国家/地区，ISO 3166-1 alpha-2")
    venue: Optional[str] = Field(default=None, description="场馆名称")

    # ── 参与信息 ─────────────────────────────────────────
    registration_url: Optional[str] = Field(default=None, description="报名/注册直链")
    registration_deadline: Optional[str] = Field(
        default=None, description="ISO 8601 报名截止日期"
    )
    fee_tier: Literal["free", "paid", "invite-only", "unknown"] = Field(
        default="unknown", description="费用层级"
    )

    # ── 内容与受众 ────────────────────────────────────────
    topics: list[str] = Field(
        default_factory=list,
        description="主题标签，如 ['net-zero', 'CSRD', 'carbon-market']",
    )
    audience: list[str] = Field(
        default_factory=list,
        description="受众标签，如 ['CSO', 'CFO', 'sustainability-team', 'investor']",
    )
    agenda_highlights: str = Field(
        default="", description="议程关键亮点摘要，≤120字"
    )

    # ── 高管参会价值评分 ──────────────────────────────────
    exec_value_score: int = Field(
        ge=1, le=5, description="高管参会价值 1–5，评分准则见 scoring_criteria.md"
    )
    exec_value_rationale: str = Field(
        default="", description="评分依据，≤80字，引用具体维度"
    )

    # ── 元数据 ────────────────────────────────────────────
    source_url: str = Field(description="原始来源 URL")
    source_org: str = Field(description="来源组织，对应 sources.yaml 中的 org 字段")
    discovered_at: str = Field(description="ISO 8601 发现时间（本次跑批时刻）")
    raw_snippet: str = Field(
        default="", description="原始抓取摘要，调试用，Notion 不展示"
    )

    # ── 退化标记（降级链路） ───────────────────────────────
    is_degraded: bool = Field(
        default=False,
        description="是否经由降级链路生成（LLM 解析失败后的次优数据）",
    )
    degrade_reason: str = Field(
        default="",
        description="降级原因，如 'LLM_parsing_failed'、'field_coercion'",
    )

    # ── 字段级宽容校验（防止 LLM 幻觉值导致整批中断）────
    @classmethod
    def _coerce_format(cls, v: str) -> str:
        allowed = {"in-person", "online", "hybrid", "unknown"}
        if v not in allowed:
            # 模糊匹配常见变体
            v_lower = v.lower().strip()
            mapping = {
                "virtual": "online", "remote": "online", "offline": "in-person",
                "physical": "in-person", "mixed": "hybrid", "blended": "hybrid",
                "onsite": "in-person", "on-site": "in-person", "live": "in-person",
                "webinar": "online", "zoom": "online", "teams": "online",
                "线下": "in-person", "线上": "online", "混合": "hybrid",
            }
            v_lower = mapping.get(v_lower, "unknown")
            return v_lower
        return v

    @classmethod
    def _coerce_fee_tier(cls, v: str) -> str:
        allowed = {"free", "paid", "invite-only", "unknown"}
        if v not in allowed:
            v_lower = v.lower().strip()
            mapping = {
                "complimentary": "free", "no cost": "free", "gratis": "free",
                "ticketed": "paid", "paid event": "paid", "fee": "paid",
            }
            return mapping.get(v_lower, "unknown")
        return v

    @classmethod
    def _clamp_score(cls, v: int) -> int:
        return max(1, min(5, int(v)))
