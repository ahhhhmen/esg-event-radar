# ESG 高管参会价值评分准则 v1.0

本文件是 LLM prompt 锚点，供 `event_radar_agent.py` 在提取 `exec_value_score` 时引用。
评分范围：1–5，整数。

---

## 评分维度（五项，各自独立计分后取综合判断）

### D1 — 组织权威度（Organizational Authority）
| 分档 | 描述 |
|------|------|
| 高 | 主办方为国际倡议组织（UNGC / GRI / ISSB / SBTi / UNFCCC 等）或 G20/G7 官方附属机构 |
| 中 | 主办方为行业头部媒体、专业协会（RBA / BSR / WEF / Reuters Events 等） |
| 低 | 主办方为商业展会公司、区域性机构，无全球倡议背书 |

### D2 — CEO/CSO 出席密度（C-Suite Participation）
| 分档 | 描述 |
|------|------|
| 高 | 往届或本届明确标注 CEO / CSO / Board Chair 演讲或出席（年度大会、Davos 级别） |
| 中 | 受众定位含高级管理层或 VP/Director 级别，但无 CEO 硬承诺 |
| 低 | 技术/操作层活动，主要受众为分析师、合规专员、学术研究人员 |

### D3 — 监管曝光度（Regulatory Exposure）
| 分档 | 描述 |
|------|------|
| 高 | 与 ISSB / SEC / EU CSRD / CBAM / CSDDD 等监管议程直接挂钩，政策制定者参与 |
| 中 | 涉及 TCFD / GRI / CDP 等自愿性披露标准，间接影响监管合规路径 |
| 低 | 纯社会责任或品牌传播活动，与强制性监管无直接关联 |

### D4 — 同业竞争情报价值（Competitive Intelligence）
| 分档 | 描述 |
|------|------|
| 高 | 主要同业头部企业（Fortune 500 / 行业前五）有明确参会记录或为主要赞助商 |
| 中 | 行业一般参与，可捕捉趋势信号，但无明确竞争对手锚点 |
| 低 | 细分小众领域，行业覆盖面窄，竞争情报价值有限 |

### D5 — 网络接入密度（Network Access）
| 分档 | 描述 |
|------|------|
| 高 | 封闭受邀制、限额参会，参会名单即为行业核心决策网络（如 BSR Annual Conference） |
| 中 | 付费开放注册，参会人数 500–3000，具备定向社交机会 |
| 低 | 完全开放，大型博览会（展商 > 展览受众），高管社交效率低 |

---

## 综合评分对照表

| 分值 | 标准 | 典型示例 |
|------|------|---------|
| **5** | D1 高 + D2 高 + D3 高，且 D4/D5 至少一项高 | UN Global Compact Leaders Summit、COP 官方会议周、WEF Davos Annual Meeting |
| **4** | D1 高/中 + D2 中/高 + D3 中/高，三维度均衡强 | BSR Conference、GreenBiz Forum、Reuters Events Net Zero Festival |
| **3** | 两个维度为"高"，其余为"中"，或三维度均为"中" | WBCSD Year-in-Review Webinar、CDP Supply Chain Forum |
| **2** | 一个维度为"高"，其余以"中/低"为主；或特定领域深度活动 | 区域性可持续发展峰会、单一议题技术研讨会 |
| **1** | 各维度均为"低"；主要为品牌曝光或学术型活动，高管 ROI 极低 | 展览型博览会、大众公开活动 |

---

## LLM 输出规范

在 `exec_value_rationale` 字段中，必须：
1. 明确引用至少两个维度（如"D1-权威度高，D2-CEO出席明确"）
2. 不超过 80 字
3. 如信息不足以判断某维度，注明"D3-监管关联信息不足，默认中档"

### 示例
```
D1-UNGC为1级倡议组织，D2-往届含60+国CEO出席，D3-与SDGs监管路径挂钩，D5-受邀制精英网络。综合评5分。
```
