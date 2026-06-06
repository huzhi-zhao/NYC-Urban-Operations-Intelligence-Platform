---
name: feedback
description: User wants AI to read project docs first, then build .claude memory
metadata:
  type: feedback
---

## 规则
- **每次会话开始时**：先阅读 README.md、CLAUDE.md、AGENTS.md 和 docs/ 目录下的架构/结构文档
- **写代码前**：先理解项目架构，不要盲目开始
- **长期记忆**：将必要内容提炼到 `.claude/` 目录下，作为不同 AI 模型/会话的共享记忆

## 为什么
用户习惯先用文档整理思路，再开始实现。要求 AI 在动手前先理解项目全貌。

## 如何应用
- 新会话启动时：读取 `.claude/MEMORY.md`，按需加载 `project.md` 和 `directory-structure.md`
- 开始新任务时：先问自己"这个任务需要理解项目的哪些部分？"
- 任何关键项目事实（架构决策、数据源、业务逻辑）都值得沉淀到 `.claude/`