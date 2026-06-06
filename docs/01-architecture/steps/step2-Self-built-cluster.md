
## 项目阶段二：自建集群完成完整需求 (Production-Ready Lakehouse Phase)

**目标：** 在自建集群上，利用 Trino, MinIO, Iceberg, Spark, Airflow 构建一个生产级别的、满足所有需求的 NYC-UOIP Lakehouse 平台。

**核心策略：**

- **生产级可靠性：** 考虑 HA (High Availability), 容错性, 扩展性。
- **完整数据：** 拉取所有历史数据，实现鲁棒的增量/全量数据同步。
- **高级功能：** 充分利用 Iceberg 的 ACID 特性、Schema Evolution，Trino 的高性能查询，Spark 的强大处理能力。
- **数据质量提升：** 详细的数据质量检查、监控和告警机制。

### 2.1 自建集群系统架构

**核心组件：**

1. **数据源:** 保持不变。
2. **Orchestration (Apache Airflow - Self-Hosted):**
    - 部署在 Docker/Kubernetes 集群中，实现 HA 和扩展性。
    - 可与 LDAP/AD 集成进行权限管理。
    - 监控和告警系统（Prometheus + Grafana）。
3. **Object Storage (MinIO):**
    - 部署在裸机或 Kubernetes 上，提供 S3 兼容的 API。
    - 作为 Bronze 和 Silver/Gold 数据的底层存储。
    - 配置数据冗余和备份策略。
4. **Metadata Catalog (Apache Hive Metastore / Nessie):**
    - **Hive Metastore (Hadoop-compatible):** 为 Spark 和 Trino 提供 Iceberg 表的元数据管理。可选择部署为 HA 模式。
    - **Alternative (更现代): Nessie:** 如果希望更 Git-like 的表版本控制，可考虑 Nessie。
5. **Data Processing (Apache Spark):**
    - **Spark Standalone / YARN / Kubernetes:** 部署 Spark 集群，用于执行 ETL 任务。
    - **PySpark with Iceberg connector:** 用于读取 MinIO Bronze 数据，并写入/操作 MinIO 上的 Iceberg 表 (Silver/Gold)。
    - **高级 ETL：**
        - **Schema Evolution:** 利用 Iceberg 处理上游 API 变更。
        - **Merge/Upsert:** 针对 Socrata API 的更新和去重，使用 Iceberg `MERGE INTO` 或 Spark 的 `upsert` 模式。
        - **Data Quality Framework:** 集成 Deequ/Great Expectations 等工具进行数据质量验证。
6. **Table Format (Apache Iceberg):**
    - 作为 Silver 和 Gold 层的数据格式。
    - 提供 ACID 事务、Schema Evolution、Hidden Partitioning、Time Travel 等高级功能。
7. **Query Engine (Trino / PrestoSQL):**
    - 部署 Trino 集群 (coordinator + workers)，连接 Hive Metastore 和 MinIO。
    - 提供高性能的交互式 SQL 查询能力，直接查询 Iceberg 表。
    - 可用于 Gold 层的业务逻辑查询、运营智能引擎和报表连接。
8. **Operational Intelligence & Recommendation Engine:** Trino SQL 或 Spark SQL 查询 Iceberg Gold 表。
9. **Reporting & Visualization (Metabase / Superset / Looker Studio):**
    - 连接到 Trino 进行数据可视化。
    - Metabase 和 Superset 都是开源方案，更适合自建集群。如果网络可达，Looker Studio 也可以连接 Trino。
10. **Monitoring & Logging:**
    - Prometheus + Grafana：监控集群资源 (CPU, 内存, 磁盘, 网络), Spark Jobs, Airflow DAGs。
    - ELK Stack (Elasticsearch, Logstash, Kibana) / Loki + Grafana：集中日志管理。

### 2.2 自建集群详细计划

**Phase 1: 基础设施部署与基础集成 (3-4周)**

- **集群环境搭建：**
    - 规划硬件/VMs，部署 Linux OS。
    - 安装 Docker/Kubernetes (推荐)。
- **MinIO 部署：** 配置高可用 MinIO 集群，设置存储策略、备份。
- **Hive Metastore 部署：** 安装 Hive Metastore，配置数据库 (PostgreSQL 推荐)，确保 Spark 和 Trino 可以访问。
- **Airflow 部署：** 部署生产级 Airflow 环境 (基于 Docker Compose 或 Kubernetes)。
- **Spark 部署：** 安装 Spark Standalone/YARN/Kubernetes 集群。
- **Trino 部署：** 安装 Trino coordinator 和 workers，配置连接 Hive Metastore 和 MinIO。
- **基础集成验证：** 验证 Spark/Trino 能否连接 Hive Metastore 和 MinIO。
- **监控与日志：** 部署 Prometheus/Grafana 和 ELK/Loki。 **交付物：** 可运行的自建 Lakehouse 基础设施，所有核心组件部署完成并相互集成。

**Phase 2: 数据摄取与 Bronze 层建设 (2-3周)**

- **Python API 客户端：** 复用 GCP 阶段的 Python 脚本，并进行优化。
    - 实现鲁棒的增量/全量同步策略。
    - 处理 API 速率限制、重试、错误通知。
    - 实现 Socrata API 增量拉取（`$where` 子句结合 `created_date` 和 `last_updated_date`）。
- **Airflow DAGs：** 部署到自建 Airflow。
- **MinIO Bronze Layer：** 数据保存到 MinIO。
- **数据质量检查：** 在 Bronze 层进行基本数据格式验证。 **交付物：** 完整的 Airflow DAGs，MinIO Bronze 层存储所有历史和增量数据。

**Phase 3: Spark Iceberg ETL (Bronze -> Silver) (4-5周)**

- **PySpark ETL 脚本：**
    - 从 MinIO Bronze 读取原始数据。
    - **Iceberg Table Creation:** 使用 Spark SQL/PySpark 创建 Iceberg 表 (Silver 层)。
    - **Schema Enforcement & Evolution:** 定义严格的 Silver 层 Schema，利用 Iceberg 的 Schema Evolution 处理上游变更。
    - **高级去重与合并：** 使用 Iceberg 的 `MERGE INTO` 语句实现生产级的去重和更新（处理晚到数据、更新记录）。
    - **数据类型转换与标准化：** 全面标准化所有关键字段。
    - **缺失值处理：** 更复杂的填充逻辑，例如使用外部查找表或更高级的空间填充（如果 BigQuery GIS 功能无法完全通过 Spark/Trino 替代）。
    - 将处理后的数据以 Iceberg 格式写入 MinIO Silver。
- **Airflow DAGs：** 部署到自建 Airflow，触发 Spark 作业。
- **数据质量框架：** 集成 Deequ/Great Expectations 到 ETL 流程，定义数据质量规则和检查点，异常时触发告警。 **交付物：** PySpark Iceberg ETL 脚本，MinIO Silver 层存储高质量的 Iceberg 表，具有 ACID 保证和 Schema Evolution 能力。

**Phase 4: Gold 层建设与业务逻辑实现 (3-4周)**

- **Iceberg Gold Schemas：** 在 Hive Metastore 中注册 Gold 层的 Iceberg 表，对应星型模型。
    - `fact_311_requests_iceberg`, `fact_vehicle_collisions_iceberg`, `dim_date_iceberg`, `dim_time_iceberg`, `dim_geography_iceberg`, `dim_weather_forecast_iceberg`。
    - `dim_geography_iceberg` 中的 `borough_geometry` 可以存储 GeoJSON 字符串或 WKT (Well-Known Text) 格式，Trino/Spark 可利用相应的 GIS 函数进行处理。
- **Trino / Spark SQL 视图/表：**
    - 通过 Trino SQL 或 Spark SQL 从 Silver 层 Iceberg 表创建 Gold 层的事实表和维度表。
    - **空间填充：** 如果缺失 `borough_id`，利用 Trino 或 Spark (结合 GeoSpark 或类似库) 的 GIS 函数进行空间连接和填充。
- **运营智能引擎：**
    - 编写 Trino SQL 或 Spark SQL 脚本，实现完整的 `Operational Load Score` 计算逻辑（含所有权重、复杂因子）。
    - 实现详细的 `Operational Drivers` 分析逻辑。
    - 将结果存储到 `fact_daily_operational_summary_iceberg` 表。
- **资源推荐引擎：**
    - 编写 Trino SQL 或 Spark SQL 脚本，应用生产级的推荐规则，生成详细的资源配置建议。
- **Airflow DAGs：** 部署到自建 Airflow，触发 Gold 层建设和运营智能计算。 **交付物：** MinIO Gold 层存储 Iceberg 表，包含所有业务指标、驱动因素和建议。Trino 可直接查询这些表。

**Phase 5: 报表、监控与运维 (2-3周)**

- **Visualization Dashboard：**
    - 部署 Metabase/Superset，连接到 Trino。
    - 创建全面且高性能的仪表板，展示所有运营负荷分数、驱动因素、建议和历史趋势。
- **生产监控：** 完善 Prometheus/Grafana 监控，设置关键指标告警。
- **日志管理：** 确保 ELK/Loki 收集所有组件的日志，并方便查询。
- **运维手册：** 编写详细的部署、运维、故障排查手册。
- **文档：** 完整的架构图、数据字典、技术设计文档。 **交付物：** 生产级 Lakehouse 平台，包含报表、全面的监控系统和完善的文档。

---

**总结：**

**GCP Demo阶段 (PoC/MVP)** 的核心是**快速验证**，不追求规模和极致优化，而是证明技术方案可行，打通核心业务流程。最重要的是**成本控制**，学会使用 GCP 的免费层级和暂停/删除资源的策略。

**自建集群阶段 (Production-Ready)** 则是为了**生产环境**，需要关注高可用、数据一致性、可扩展性、性能和运维的便捷性。Iceberg 是这个阶段的关键，它提供了传统数据仓库的 ACID 特性，同时保持了数据湖的灵活性。

这两个阶段的划分非常合理，GCP Demo 可以帮助你快速上手并验证技术方向，为第二阶段的复杂部署和开发积累经验。