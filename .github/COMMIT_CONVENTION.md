# Commit Convention

本项目采用 [Conventional Commits](https://www.conventionalcommits.org/zh-hans/v1.0.0/) 规范，标题可用中文或英文，建议不超过 72 字符。

## 类型前缀

| 前缀 | 含义 |
|------|------|
| `feat:` | 新功能（如 `feat: 新增 Greenpeace 源覆盖`） |
| `fix:` | 缺陷修复（如 `fix: 三重防御修复 — 哈希年级降维`） |
| `docs:` | 文档变更（README / 注释） |
| `refactor:` | 重构（不改变行为） |
| `perf:` | 性能优化 |
| `test:` | 测试相关 |
| `ci:` | CI / 自动化（如 `ci: 增加 unittest 步骤`） |
| `chore:` | 杂项（依赖升级、构建脚本、`.ics` 自动回传等） |
| `style:` | 纯格式调整（空白等，不影响逻辑） |

## 示例

```
feat: 新增 13 条关键矿产与中文媒体 GNews 源
fix: hash desensitization + in-memory upsert cache
ci: add timeout-minutes 45 to prevent infinite hang
chore: auto-update calendar file [skip ci]
doi: 补充 COMMIT_CONVENTION.md
```

## 注意事项

- 自动提交的 ICS 文件更新必须使用 `chore: auto-update calendar file [skip ci]` 格式（已在 workflow 中硬编码）
- 不建议使用 `Update esg_events.ics` 等无前缀提交覆盖自动提交
