![modern_data_eng_docs_structure.svg](modern_project_structure.svg)


```text

docs/
│
├── 00-requirements/
│   ├── PRD-{id}-{feature}.md
│   ├── domain-knowledge.md          # 业务状态机、计算模型、枚举值定义
│   ├── metric-glossary.md           # 指标口径（GMV/DAU 等精确定义）
│   └── stakeholder-map.md           # 数据消费方清单（BI/ML/产品）
│
├── 01-architecture/
│   ├── system-architecture.md
│   ├── data-pipeline-flow.md
│   ├── environments.md              # Dev/Stg/Prod 差异、网络策略
│   ├── tech-stack.md                # 工具选型及版本锁定
│   └── decisions/                   # ★ ADR（Architecture Decision Records）
│       ├── ADR-001-why-iceberg.md   # 格式：背景 → 决策 → 否决方案 → 后果
│       └── ADR-002-why-dagster.md
│
├── 02-data-contracts/               # ★ 2025 新增：ODCS v3 (Bitol/Linux Foundation)
│   ├── README.md                    # 说明契约标准及工具链（datacontract-cli）
│   ├── provider-contracts/          # 本团队作为数据提供方对外承诺
│   │   └── orders.datacontract.yaml # ODCS v3 格式：schema + SLA + quality rules
│   ├── consumer-contracts/          # 本团队消费上游的期望（Great Expectations 可生成）
│   │   └── crm-users.datacontract.yaml
│   └── sla-matrix.md                # 数据产品 SLA 汇总（延迟/可用性/质量）
│
├── 03-data-sources/
│   ├── source-registry.md           # 数据源总览（负责人/网络/限速）
│   ├── api-contracts/
│   │   ├── crm-api.yaml             # OpenAPI/Swagger
│   │   └── payment-gateway.md
│   ├── webhooks/
│   │   └── user-behavior-webhook.md
│   ├── cdc-specs/                   # ★ 新增：Change Data Capture 规范
│   │   └── mysql-binlog-config.md   # Debezium/Flink CDC 配置说明
│   └── ingestion-specs.md
│
├── 04-data-models/
│   ├── schema-registry/
│   │   ├── ods_events.yaml
│   │   └── dwd_user_di.sql
│   ├── data-dictionary.md
│   ├── lineage.md
│   ├── data-quality-contracts.md    # ★ 已知脏数据模式、NULL 的业务含义、枚举边界
│   └── impact-matrix.md             # ★ 表 → 下游任务/报表/API 影响矩阵
│
├── 05-testing/                      # ★ 新增独立目录（原散落各处）
│   ├── test-strategy.md             # 单元/集成/契约/E2E 测试分层策略
│   ├── dbt-tests/
│   │   └── schema-test-conventions.md
│   ├── great-expectations/          # 或 soda/
│   │   └── expectation-suites.md
│   ├── sample-data/                 # 测试用脱敏数据集及生成脚本
│   └── ci-validation.md             # PR 门禁规则（数据质量卡点）
│
├── 06-operations/
│   ├── deployment-guide.md          # CI/CD + Airflow/Dagster 调度配置
│   ├── orchestration/
│   │   ├── dag-conventions.md       # DAG 命名、重试策略、SLA 配置
│   │   └── backfill-procedures.md
│   ├── troubleshooting/             # ★ 结构化 Playbook
│   │   ├── _template.md             # 症状 → 原因排序 → 诊断命令 → 恢复 → 预防
│   │   ├── pipeline-lag.md
│   │   └── schema-drift.md
│   ├── monitoring-alerts.md
│   └── incident-postmortems/        # ★ 事故复盘（防止 AI 重踩同类坑）
│       └── 2025-12-data-loss-rca.md
│
├── 07-governance/                   # ★ 新增独立目录（监管压力 + 数据产品化）
│   ├── data-catalog-integration.md  # Datahub/Amundsen/Alation 集成说明
│   ├── access-control.md            # RBAC 策略、列级权限
│   ├── pii-tagging.md               # 字段敏感级别标注（支持自动化扫描）
│   ├── compliance/
│   │   ├── gdpr-ccpa-mapping.md
│   │   └── data-retention-policy.md
│   └── data-products/               # ★ Data Mesh：数据产品定义
│       └── user-profile-product.md  # 遵循 ODPS 规范
│
└── 08-ai-context/                   # ★ AI 专属上下文（现代开发核心差异化目录）
    ├── CLAUDE.md → ../../CLAUDE.md  # symlink，保持单一来源
    ├── rules/                       # ★ 作用域规则（Cursor .mdc 格式）
    │   ├── sql.mdc                  # SQL 规范（禁止 collect()、分区策略等）
    │   ├── python.mdc               # Python pipeline 规范
    │   ├── dag.mdc                  # Airflow/Dagster DAG 编写规范
    │   └── dbt.mdc                  # dbt model 命名与测试规范
    ├── prompt-templates/
    │   ├── pipeline-scaffold.md     # 生成新 pipeline 的标准 prompt
    │   ├── sql-transform.md
    │   └── test-generation.md
    ├── business-logic-faq.md        # 复杂边界条件（防止 AI 幻觉的知识锚点）
    ├── anti-hallucination-guardrails.md  # ★ 禁止模型使用的已废弃函数/表/接口
    └── context-snapshots/           # ★ 阶段性上下文快照（长项目保持 AI 认知连续性）
        └── 2026-Q2-sprint-context.md
```


## DOCKER Deployment
```shell
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler

```