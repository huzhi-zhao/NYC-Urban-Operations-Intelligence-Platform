## 项目阶段一：GCP Demo级测试 (PoC/MVP Phase)

**目标：** 利用 GCP 免费额度 ($414/3个月)，在最小数据集上打通 NYC-UOIP 的端到端流程。验证技术栈可行性、数据流转、核心业务逻辑（运营负荷分计算、驱动因素识别、资源建议生成）。

**核心策略：**

- **极致成本控制：** 严格控制计算资源使用时间，能用免费层级服务的绝不升配。
- **最小数据量：** 仅拉取几天甚至一天的数据，或设置极小的 `limit` 参数。
- **简化逻辑：** 运营负荷分计算公式、驱动因素识别、资源建议规则可以简化。
- **焦点：** 验证**全流程的连贯性**，而非数据量或复杂性。

### 1.1 GCP Demo阶段系统架构 (Cost-Optimized)

**核心组件：**

- **数据源:** 保持不变 (Socrata, Open-Meteo, NYC Open Data)。
- **Ingestion (Airflow/Cloud Composer - Minimal):**
    - **Cloud Composer:** 这是最昂贵的组件。**必须选择最小的 Composer 环境 (small env size, low worker count)**，并在不使用时**暂停/删除**，需要时再重建。这是节省预算的关键。
    - **Python Scripts:** 用于API拉取。
- **Raw Data Lake (GCS Bronze - Free Tier):**
    - GCS Standard 存储，1GB 免费额度足够 demo。
- **Data Processing (Dataproc - Ephemeral, Micro):**
    - **Dataproc:** 仅在 Spark 作业运行时创建**最小的临时集群 (e.g., 1 master, 0-1 worker, `n1-standard-1` 或 `e2-micro` 实例类型)**，作业完成后立即删除。
    - **PySpark:** 用于清洗、标准化和去重。
- **Cleaned Data Lake (GCS Silver - Free Tier):**
    - GCS Standard 存储。
- **Data Warehouse (BigQuery Gold - Free Tier for storage/query):**
    - **BigQuery:** 存储（前10GB免费）和查询（前1TB免费/月）都非常经济。但仍需注意查询优化，避免大量全表扫描。
    - **SQL:** 用于构建 Gold 层和运营智能引擎。
- **Operational Intelligence & Recommendation Engine:** BigQuery SQL。
- **Reporting (Looker Studio - Free):** 连接 BigQuery。
- **IAM & Networking:** 最小权限原则。

### 1.2 GCP Demo阶段详细计划

**Phase 0: 基础设施与成本控制设置 (1-3天)**

- **GCP项目设置：** 创建新项目，绑定免费额度。
- **IAM设置：** 创建必要的Service Account (SA) 和最小权限角色，例如：
    - `composer_sa` (用于 Airflow, 需 GCS/BigQuery/Dataproc 访问权限)
    - `dataproc_sa` (用于 Spark, 需 GCS/BigQuery 访问权限)
- **GCS Buckets：** 创建 `your-project-bronze`, `your-project-silver`。
- **BigQuery Datasets：** 创建 `nyc_uoip_gold`。
- **Cloud Composer (关键成本项):**
    - **创建最小环境** (small environment size, 1 scheduler, 1 webserver, 1 worker, e.g., `n1-standard-2`).
    - **在不使用时，立即暂停或删除环境！** 这是最省钱的方式。每次需要测试时再重建（虽然重建耗时，但对 demo 而言是可接受的妥协）。
- **Dataproc 集群模板：** 定义一个包含 `n1-standard-1` 或 `e2-micro` 实例的最小集群模板。
- **Looker Studio:** 准备好连接 BigQuery 的数据源。
- **代码仓库:** 准备好 Git 仓库。 **交付物：** 配置好的GCP环境，最小的Composer环境（或删除状态），空的存储和数据库。

**Phase 1: 最小数据集摄取 (3-5天)**

- **数据范围裁剪：**
    - **Socrata (311 & NYPD):** 仅拉取**过去24-48小时**的数据，并将 `offset` 和 `limit` 参数设置为**极小值**（例如，`$limit=100` 或 `500`），只拉取几页数据，确保能看到数据流即可。
    - **Open-Meteo:** 仅获取纽约市**一个中心点**的未来24小时天气预报。
    - **Borough Boundaries:** 完整 GeoJSON，一次性上传。
- **Python API 客户端：** 编写 Python 脚本，实现增量（基于日期）和分页拉取逻辑，并处理 API 错误。
- **GCS Bronze Layer：** 将拉取到的原始 JSON/GeoJSON 文件保存到 GCS Bronze，按摄取日期分区。
- **Airflow DAGs：**
    - 为每个数据源编写独立的 DAG，负责 API 调用和数据上传到 GCS Bronze。
    - DAG 需包含适当的重试机制和错误处理。
- **成本控制：** 运行 DAG 后，**立即关闭 Dataproc 集群 (如果误创建)，暂停或删除 Cloud Composer**。 **交付物：** Airflow DAGs，GCS Bronze 层有少量原始数据文件。

**Phase 2: Spark ETL (Bronze -> Silver) (5-7天)**

- **PySpark ETL 脚本：**
    - 编写 Spark 脚本，从 GCS Bronze 读取 JSON 数据。
    - 执行基本的 Schema Enforcement (明确字段类型)。
    - **简化去重：** 对于 Demo，可以仅基于 `_id` 或 `(created_date, complaint_type)` 简单去重，不追求生产级的晚到数据处理。
    - **简化缺失值处理：** 仅处理 `latitude` 和 `longitude` 的非空判断。`borough` 字段可以先跳过空间填充，在 Gold 层再进行。
    - 数据类型转换：将日期字符串转为 `TIMESTAMP`。
    - 将处理后的数据以 Parquet 格式写入 GCS Silver，按日期分区。
- **Airflow DAGs：** 扩展 Airflow DAG，在 Bronze 数据就绪后，触发 Dataproc 上的 Spark 作业。确保 Spark 集群是**短暂的**。
- **Dataproc 配置：** 使用 Phase 0 定义的最小集群模板。
- **成本控制：** Spark 作业完成后，Dataproc 集群自动删除。**暂停或删除 Cloud Composer**。 **交付物：** PySpark ETL 脚本，GCS Silver 层有少量清洗后的 Parquet 数据。

**Phase 3: BigQuery Gold 层建模与数据加载 (5-7天)**

- **BigQuery Schemas：** 在 `nyc_uoip_gold` 数据集中创建 `fact_311_requests`, `fact_vehicle_collisions`, `dim_geography`, `dim_weather_forecast` 表。
    - `fact_` 表分区和聚簇设置可以简化，主要关注 `created_at_utc` 和 `crash_at_utc` 上的分区。
    - `dim_geography.borough_geometry` 使用 `GEOGRAPHY` 类型。
- **数据加载：**
    - `dim_geography`：将 GCS Bronze 中的 GeoJSON 文件加载到 BigQuery。
    - `fact_` 表：
        - 创建 BigQuery External Tables 指向 GCS Silver 的 Parquet 文件。
        - 编写 SQL (或 Python 脚本，使用 `google-cloud-bigquery` 库) 从 External Tables 读取，插入到管理表。
        - **关键：空间填充：** 使用 BigQuery SQL 的 `ST_GEOGPOINT` 和 `ST_CONTAINS` 函数与 `dim_geography` 进行连接，填充缺失的 `borough_id`。这是 Demo 的一个亮点。
- **Airflow DAGs：** 扩展 Airflow DAG，在 Silver 数据就绪后，触发 BigQuery 加载任务。
- **成本控制：** BigQuery 存储和查询相对便宜，但仍需注意查询优化。**暂停或删除 Cloud Composer**。 **交付物：** BigQuery Gold 层，包含少量填充数据的事实表和维度表，并验证了空间填充逻辑。

**Phase 4: 运营智能与推荐引擎 (BigQuery SQL) (3-5天)**

- **简化运营负荷分：**
    - `Operational Load Score` 简化为 `(count(311_requests) * 0.4 + count(collisions) * 0.4 + (case when snowfall > 0 or temperature < 0 then 1 else 0 end) * 0.2)`。
    - 计算每天每个 Borough 的这个分数。
- **简化驱动因素：**
    - 编写 BigQuery SQL 逻辑：
        - IF `snowfall > 0` THEN 'Severe Weather: Snow'
        - IF `temperature < 0` THEN 'Severe Weather: Cold'
        - ELSE 'General Demand'
- **简化资源建议：**
    - 基于简单的 `IF Load_Score > X THEN 'Increase Agents'` 等规则。
- **结果存储：** 将计算结果存储到 `fact_daily_operational_summary` 表中。
- **Airflow DAGs：** 扩展 Airflow DAG，每日触发此计算任务。
- **成本控制：** 纯 BigQuery SQL 成本较低。**暂停或删除 Cloud Composer**。 **交付物：** BigQuery SQL 脚本，`fact_daily_operational_summary` 表，包含每日运营负荷分、驱动因素和建议。

**Phase 5: 可视化与Demo (1-2天)**

- **Looker Studio Dashboard：**
    - 连接到 BigQuery 的 `fact_daily_operational_summary` 表。
    - 创建仪表板：展示运营负荷分排名、驱动因素文本、资源建议文本。
    - 可简单可视化地图（颜色深浅），列表。
- **Demo准备：** 准备好演示流程，解释每个环节的商业价值和技术实现。
- **成本监控：** 持续监控 GCP 账单，确保在免费额度内。 **交付物：** 可运行的 Looker Studio 仪表板，一份 Demo 演示稿。

---
