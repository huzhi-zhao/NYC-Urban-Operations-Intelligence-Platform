## 项目概述与核心目标重申

**项目名称：** NYC Urban Operations Intelligence Platform (NYC-UOIP) **项目定位：** 真实商业场景驱动的数据平台项目。 **核心目标：** 模拟 NYC 城市运营团队，构建 Lakehouse 平台，整合多源数据，预测未来24小时运营负荷，并提供资源配置决策建议。

**关键输出：**

1. **Operational Load Score (运营负荷分)：** 区域负荷排名。
2. **Operational Drivers (驱动因素)：** 高负荷来源解释。
3. **Resource Allocation Recommendation (资源配置建议)：** 跨部门资源调配建议。

**技术栈：** GCP (GCS, Dataproc, BigQuery, Cloud Composer/Airflow), Spark, Python, SQL.

---

## 1. 系统架构总览 (System Architecture Overview)

![NYC-UOIP Architecture Diagram](https://i.imgur.com/example_architecture.png)

 _(Note: As an AI, I cannot generate actual images directly. Please imagine a standard Lakehouse architecture diagram here, showing data flow from left to right as described below.)_

**核心组件和数据流：**

1. **数据源 (Data Sources):**
    
    - **Socrata APIs:** NYC 311 Service Requests, NYPD Motor Vehicle Collisions (JSON)
    - **Open-Meteo API:** Weather Forecast (JSON)
    - **NYC Open Data:** NYC Borough Boundaries (GeoJSON)
2. **数据摄取层 (Ingestion Layer - Airflow / Cloud Composer):**
    
    - **功能:** 定时触发，增量拉取（处理分页和Socrata API的`$OFFSET`/$LIMIT`), API错误处理, 原始数据加载到 GCS Bronze。
    - **技术:** Python (requests库), Airflow DAGs。
3. **原始数据湖 (Raw Data Lake - GCS Bronze Layer):**
    
    - **存储:** GCS bucket。
    - **格式:** 原始 JSON (Socrata, Open-Meteo), GeoJSON (Borough Boundaries)。
    - **分区:** `gs://your-project-bucket/bronze/<dataset_name>/year=<YYYY>/month=<MM>/day=<DD>/`
    - **特点:** 不可变，保留完整历史快照。
4. **数据处理层 (Data Processing Layer - Spark / Dataproc):**
    
    - **功能:** 从 GCS Bronze 读取，执行 ETL (Schema Enforcement, 数据类型转换, 去重, 处理脏数据/缺失值, 标准化)。
    - **技术:** PySpark, Dataproc (集群管理)。
    - **输出:** 干净、结构化的数据。
5. **清洗数据湖 (Cleaned Data Lake - GCS Silver Layer):**
    
    - **存储:** GCS bucket。
    - **格式:** Parquet (优化列式存储，便于Spark/BigQuery读取)。
    - **分区:** `gs://your-project-bucket/silver/<dataset_name>/year=<YYYY>/month=<MM>/day=<DD>/`
    - **特点:** 数据质量高，易于查询。
6. **数据仓库 (Data Warehouse - BigQuery Gold Layer):**
    
    - **功能:** 从 GCS Silver 加载数据，构建星型模型，实现业务逻辑，进行空间分析。
    - **技术:** BigQuery (外部表, 管理表, SQL, GIS functions)。
    - **核心:** 事实表、维度表、聚合表。
7. **运营智能与推荐引擎 (Operational Intelligence & Recommendation Engine):**
    
    - **功能:** 基于 BigQuery Gold 层数据，使用 SQL 规则引擎计算 Operational Load Score，分析驱动因素，并生成资源配置建议。
    - **技术:** BigQuery SQL。
8. **报表与可视化 (Reporting & Visualization):**
    
    - **功能:** 展示运营负荷分数、驱动因素和资源建议。
    - **技术:** Looker Studio / Streamlit (或两者结合)。
9. **编排与调度 (Orchestration & Scheduling):**
    
    - **功能:** 统一管理数据摄取、Spark ETL、BigQuery加载和规则引擎的执行顺序与依赖。
    - **技术:** Airflow / Cloud Composer。
10. **监控与告警 (Monitoring & Alerting):**
    
    - **功能:** 监控 pipeline 健康状况、数据质量、资源使用情况。
    - **技术:** Google Cloud Monitoring / Logging, Airflow alerts。

---

## 2. 数据分层 (Data Lakehouse Layers) 详细设计

### 2.1 Bronze Layer (GCS)

- **目的:** 存储原始、不变、完整的历史数据快照。作为数据湖的“单一真相来源”。
- **格式:**
    - Socrata APIs: 原始 JSON 文件。每个文件包含一个或多个 API 调用返回的记录。
    - Open-Meteo API: 原始 JSON 文件。
    - NYC Borough Boundaries: 原始 GeoJSON 文件（可能是一次性加载）。
- **结构:**
    - `gs://<bucket_name>/bronze/nyc_311_requests/raw_YYYY-MM-DD_HH-MM-SS.json`
    - `gs://<bucket_name>/bronze/nypd_collisions/raw_YYYY-MM-DD_HH-MM-SS.json`
    - `gs://<bucket_name>/bronze/open_meteo_weather/forecast_YYYY-MM-DD_HH-MM-SS.json`
    - `gs://<bucket_name>/bronze/nyc_borough_boundaries/boroughs.geojson` (静态数据)
- **分区:** 考虑到 Socrata API 的分页机制，每个 Airflow Task 实例会拉取当天的所有增量数据，并可以打包成一个或多个 JSON 文件。文件命名中包含摄取的时间戳，以保留快照。
- **数据保留:** 根据成本和合规性要求设置 GCS 生命周期策略（例如，30天后转为 Nearline/Coldline，或删除更旧的数据）。

### 2.2 Silver Layer (GCS)

- **目的:** 存储经过清洗、标准化、去重并具有定义模式的数据。为下游分析和数据仓库提供高质量输入。
- **格式:** Parquet (列式存储，高效查询和压缩)。
- **结构:**
    - `gs://<bucket_name>/silver/nyc_311_requests/year=<YYYY>/month=<MM>/day=<DD>/data.parquet`
    - `gs://<bucket_name>/silver/nypd_collisions/year=<YYYY>/month=<MM>/day=<DD>/data.parquet`
    - `gs://<bucket_name>/silver/open_meteo_weather/forecast_date=<YYYY-MM-DD>/data.parquet`
- **分区:** 按日期分区，便于 Spark 和 BigQuery 进行时间序列查询和优化。
- **主要处理：**
    - **Schema Enforcement:** 明确每个字段的数据类型。
    - **数据类型转换:** 将字符串日期转换为 `TIMESTAMP`，字符串数字转换为 `INT`/`FLOAT`。
    - **去重 (Deduplication):** 针对 Socrata API 可能返回重复记录的情况，基于 `unique_key` (例如 `(created_date, complaint_type, latitude, longitude)` 的哈希值或 Socrata ID) 进行去重。
    - **缺失值处理:** 针对关键字段（如 `latitude`, `longitude`, `borough`），采用策略填充（例如，对于 `incident_zip` 可以查找 `zip_code` 到 `borough` 的映射，或使用 BigQuery 的 `ST_CONTAINS`）。
    - **标准化:** `complaint_type`, `descriptor` 等字段的统一大小写，去除不必要的空格。
    - **时间戳处理:** 统一所有时间戳到 UTC。
    - **增量处理:** Spark ETL 任务只会处理当天在 Bronze 层新到达的数据，并将其追加到 Silver 层对应的日期分区。

### 2.3 Gold Layer (BigQuery)

- **目的:** 存储面向业务的、高度聚合和优化的数据。支持快速查询、报告和运营智能引擎。
- **格式:** BigQuery Native Tables。
- **数据模型:** 采用星型模型。
    - **Fact Tables (事实表):**
        - `fact_311_requests`: 311 服务请求的核心事实。
            - 字段: `request_id` (如果 Socrata API 提供), `date_key`, `time_key`, `borough_id`, `latitude`, `longitude`, `complaint_type`, `descriptor`, `status`, `incident_zip`, `created_at_utc`。
            - BigQuery 特性: 按 `created_at_utc` 分区, 按 `borough_id`, `complaint_type` 聚簇。
        - `fact_vehicle_collisions`: 车辆碰撞的核心事实。
            - 字段: `collision_id`, `date_key`, `time_key`, `borough_id`, `latitude`, `longitude`, `persons_injured`, `contributing_factor`, `crash_at_utc`。
            - BigQuery 特性: 按 `crash_at_utc` 分区, 按 `borough_id` 聚簇。
        - `fact_daily_operational_summary`: **新增的聚合事实表**。包含每天每个 Borough 的汇总负荷指标。
            - 字段: `date_key`, `borough_id`, `total_311_requests`, `total_collisions`, `avg_temp`, `total_snowfall`, `total_precipitation`, `operational_load_score`, `main_drivers`, `recommendations`。
            - BigQuery 特性: 按 `date_key` 分区, 按 `borough_id` 聚簇。
    - **Dimension Tables (维度表):**
        - `dim_date`: 日期维度表。
            - 字段: `date_key`, `full_date`, `day_of_week`, `month`, `year`, `is_weekend`, `holiday_name` 等。
        - `dim_time`: 时间维度表。
            - 字段: `time_key`, `full_time`, `hour`, `minute`, `period_of_day` 等。
        - `dim_geography`: 地理维度表。
            - 字段: `borough_id`, `borough_name`, `borough_geometry` (GEOGRAPHY 类型)。
            - 来源: NYC Borough Boundaries GeoJSON 导入。这将是 **空间分析的关键**。
        - `dim_weather_forecast`: 天气预报维度表（未来24小时）。
            - 字段: `forecast_date_key`, `forecast_hour_of_day`, `borough_id` (或 `location_key` 如果未来支持多点), `temperature_2m`, `snowfall`, `precipitation`, `windspeed_10m`, `weather_condition_code` (从Open-Meteo映射)。
            - BigQuery 特性: 按 `forecast_date_key` 分区, 按 `borough_id` 聚簇。
- **Spatial Data Handling:**
    - `dim_geography.borough_geometry` 将存储 GeoJSON 的多边形数据作为 BigQuery `GEOGRAPHY` 类型。
    - 对于 `fact_311_requests` 和 `fact_vehicle_collisions` 中缺失 `borough` 信息的记录：
        - 在 Silver 层，如果 `incident_zip` 存在，可以尝试通过查找表进行填充。
        - 在 Gold 层，可以使用 BigQuery 的 `ST_GEOGPOINT(longitude, latitude)` 创建地理点，然后利用 `ST_CONTAINS(dim_geography.borough_geometry, fact_table.incident_point)` 函数进行空间连接，填充缺失的 `borough_id`。这是一个关键且高效的步骤。

---

## 3. 敏捷开发路线 (Agile Development Roadmap)

我们将把项目分解为多个短迭代的 Phase，每个 Phase 有明确的交付物。

### Phase 0: 基础设施与项目启动 (Foundation & Project Setup) - (约 1 周)

**目标:** 建立基础 GCP 环境、IAM 和 CI/CD 流程，确保开发环境就绪。 **工作内容:**

- 创建 GCP 项目，配置计费。
- 设置 IAM 角色和权限，遵循最小权限原则（例如：服务账号用于 Airflow 和 Dataproc）。
- 创建 GCS buckets (Bronze, Silver)。
- 配置 BigQuery datasets (Raw, Staging, Gold)。
- 建立 Git 代码库，并设置 CI/CD 管道（例如：Cloud Build）用于 Airflow DAGs 部署到 Cloud Composer。
- 配置 Cloud Composer 环境。
- 定义编码标准和 PR 审查流程。 **交付物:**
- 就绪的 GCP 项目与 IAM 配置。
- 空的 GCS buckets 和 BigQuery datasets。
- 运行中的 Cloud Composer 环境。
- Git 仓库和 CI/CD 管道。
- 基础项目文档（架构图、数据字典模板）。

### Phase 1: 数据摄取基础 (Data Ingestion Foundation) - (约 2-3 周)

**目标:** 建立从所有数据源到 GCS Bronze 层的可靠、增量、自动化的数据流。 **工作内容:**

- **311 Requests Ingestion:**
    - 开发 Python 脚本，使用 `requests` 库调用 Socrata API `erm2-nwe9.json`。
    - 实现分页逻辑 (`$OFFSET`, `$LIMIT`)。
    - 实现增量拉取逻辑（例如，通过查询 `created_date > last_successful_run_date`，并处理 Socrata `limit` / `offset` 组合）。
    - 实现健壮的错误处理和重试机制。
    - 将原始 JSON 数据写入 GCS Bronze，按摄取时间戳命名。
- **NYPD Collisions Ingestion:**
    - 类似 311 请求，开发 Python 脚本拉取 `h9gi-nx95.json`，实现分页和增量拉ake。
    - 将原始 JSON 数据写入 GCS Bronze。
- **Open-Meteo Weather Ingestion:**
    - 开发 Python 脚本调用 `open-meteo.com/v1/forecast`。
    - 设计为每日拉取一次，获取未来24小时的预报。
    - 考虑如何将 NYC 几个大区的经纬度作为参数进行调用（例如，可以取每个 Borough 的中心点）。
    - 将原始 JSON 数据写入 GCS Bronze。
- **NYC Borough Boundaries Ingestion:**
    - 手动或编写脚本，将 GeoJSON 文件一次性上传到 GCS Bronze。
- **Airflow DAGs:** 为每个数据源创建独立的 Airflow DAG，负责其摄取任务。
- **数据验证:** 对 Bronze 层数据进行基本检查，确保数据到达。 **交付物:**
- Airflow DAGs (311, NYPD, Open-Meteo, Borough Boundaries)。
- GCS Bronze 层成功存储了各数据源的原始 JSON/GeoJSON 文件，并验证了增量拉取功能。
- Ingestion Pipeline 的日志与监控配置。

### Phase 2: Spark ETL 层 (Bronze -> Silver) - (约 3-4 周)

**目标:** 构建健壮的 Spark ETL 管道，将原始数据清洗、标准化并去重，写入 GCS Silver 层。 **工作内容:**

- **Spark环境设置:**
    - 创建 Dataproc 集群模板，用于 Spark 作业执行。
    - 编写 PySpark 脚本，读取 GCS Bronze 层的 JSON 文件。
- **311 Requests ETL:**
    - 定义精确的 `fact_311_requests` schema。
    - 执行数据类型转换 (e.g., `created_date` to `timestamp`, `latitude`/`longitude` to `double`).
    - 处理 `complaint_type`, `descriptor` 的标准化（小写，去除空格）。
    - 根据 `(created_date, complaint_type, latitude, longitude)` 或 Socrata ID 生成哈希键进行去重。
    - 处理 `borough` 字段的填充逻辑 (如果 `incident_zip` 存在，可以先做初步映射)。
    - 将处理后的数据以 Parquet 格式写入 GCS Silver。
- **NYPD Collisions ETL:**
    - 类似 311 请求，定义 `fact_vehicle_collisions` schema。
    - 数据类型转换 (`crash_date`/`crash_time` 组合成 `timestamp`, `persons_injured` to `integer`).
    - `contributing_factor` 标准化。
    - 去重逻辑。
    - 将处理后的数据以 Parquet 格式写入 GCS Silver。
- **Open-Meteo Weather ETL:**
    - 定义 `dim_weather_forecast` schema。
    - 处理时间戳、温度、降雪量等字段的转换。
    - 将处理后的数据以 Parquet 格式写入 GCS Silver。
- **Airflow DAGs:** 集成 Spark ETL 作业到 Airflow DAG 中，使其在数据摄取完成后自动触发。
- **数据质量检查:** 对 Silver 层数据进行初步的数据质量验证（例如，检查关键字段非空、数值范围等）。 **交付物:**
- PySpark ETL 脚本，可从 GCS Bronze 读取并写入 GCS Silver。
- 更新的 Airflow DAGs，包含 Spark ETL 步骤。
- GCS Silver 层有清洗、去重和标准化的 Parquet 数据。
- 数据质量监控和告警机制（例如，如果某天 Silver 层数据量异常）。

### Phase 3: 数据仓库建模 (Silver -> Gold) - (约 3-4 周)

**目标:** 构建 BigQuery Gold 层，实现星型模型，并填充数据，支持空间分析。 **工作内容:**

- **创建 BigQuery Gold Schemas:**
    - 定义 `fact_311_requests`, `fact_vehicle_collisions`, `dim_date`, `dim_time`, `dim_geography`, `dim_weather_forecast` 表结构。
    - 为事实表配置分区和聚簇。
    - 为 `dim_geography.borough_geometry` 使用 `GEOGRAPHY` 类型。
- **加载维度表:**
    - `dim_date` / `dim_time`: 可以通过 SQL 生成或从预设 CSV 加载。
    - `dim_geography`: 从 GCS Bronze 的 GeoJSON 文件加载到 BigQuery (例如使用 `bq load` 命令或 Python 客户端库)。确保 `geometry` 字段正确转换为 `GEOGRAPHY` 类型。
- **加载事实表:**
    - 创建 BigQuery External Tables 指向 GCS Silver 的 Parquet 文件。
    - 编写 SQL (或 PySpark 脚本)，从 External Tables 读取数据，进行必要的JOIN（例如，与 `dim_date`, `dim_time`），并插入到 BigQuery 事实管理表。
    - **关键空间填充:** 对 `fact_311_requests` 和 `fact_vehicle_collisions` 中 `borough_id` 缺失的记录，利用 BigQuery SQL 的 `ST_GEOGPOINT` 和 `ST_CONTAINS` 函数与 `dim_geography` 进行空间连接，填充 `borough_id`。
- **Airflow DAGs:** 扩展 Airflow DAGs，在 Spark ETL 任务完成后触发 BigQuery 加载和转换任务。 **交付物:**
- 完整 BigQuery Gold 层，包含所有事实表和维度表。
- 所有表都已填充数据，并通过 SQL 查询验证数据准确性。
- 实现了 `borough_id` 的空间填充逻辑。

### Phase 4: 运营智能引擎 (Operational Intelligence Engine) - (约 2-3 周)

**目标:** 在 BigQuery Gold 层实现 Operational Load Score 的计算和驱动因素分析。 **工作内容:**

- **Operational Load Score 计算逻辑:**
    - 编写 BigQuery SQL 脚本，基于以下规则计算每日每个 Borough 的得分：
        - `311 Request Volume Factor`: 计算每个 Borough 在过去24小时内311请求的数量，并将其标准化为0-100的因子。考虑不同 `complaint_type` 的权重（例如，供暖投诉权重更高）。
        - `Vehicle Collision Factor`: 计算过去24小时内每个 Borough 的交通事故数量和受伤人数，标准化为0-100因子。
        - `Weather Factor`: 基于 `dim_weather_forecast`，分析未来24小时的恶劣天气（例如，降雪量、低温、大风）。定义规则将其标准化为0-100因子（例如，暴雪=100，大雨=50）。
    - 将三个因子按权重 `0.4 / 0.4 / 0.2` 加权求和，得到 `Operational Load Score`。
- **Operational Drivers 分析:**
    - 编写 BigQuery SQL 逻辑，识别导致高负荷的主要原因。例如：
        - 如果 `Weather Factor` 高，则驱动因素为 "Severe Weather Impact"。
        - 如果特定 `complaint_type` 数量激增，则驱动因素为 "High Volume [Complaint Type] Complaints"。
        - 可以结合历史模式（例如，某个 Borough 某个时间段交通事故高发）。
- **创建聚合表:** 将计算结果存储到 `fact_daily_operational_summary` 表。
- **Airflow DAGs:** 创建新的 Airflow DAG 或扩展现有 DAG，每日触发此计算任务。 **交付物:**
- BigQuery SQL 脚本，用于计算 `Operational Load Score` 和 `Operational Drivers`。
- `fact_daily_operational_summary` 表，每日更新。
- 验证了得分和驱动因素的逻辑准确性。

### Phase 5: 资源配置建议引擎 (Resource Recommendation Engine) - (约 2 周)

**目标:** 基于 Operational Load Score 和驱动因素，生成具体的资源配置建议。 **工作内容:**

- **定义推荐规则:** 与业务方合作，根据不同的 `Operational Load Score` 区间和 `Operational Drivers` 制定具体规则。
    - 示例规则:
        - IF `Borough_X` `Load Score` > 80 AND `Driver` LIKE 'Blizzard%' THEN "Increase +15% agents for Heating/Housing queue in Borough_X."
        - IF `Borough_Y` `Load Score` > 70 AND `Driver` LIKE 'Traffic Collision%' THEN "Deploy +2 standby ambulances in Borough_Y accident hotspots."
        - IF `Borough_Z` `Load Score` < 30 AND `Driver` NOT LIKE 'Severe Weather%' THEN "Reduce Parks Dept regular inspection teams by -2 in Borough_Z."
- **实现推荐逻辑:**
    - 在 BigQuery 中编写 SQL 脚本，应用这些规则，生成 `resource_recommendations` 字段，存储在 `fact_daily_operational_summary` 中。
    - 可以设计一个 `dim_recommendation_rules` 表来存储这些规则，使其更易于维护和更新，而不是硬编码在 SQL 中。
- **Airflow DAGs:** 整合推荐引擎到每日调度中。 **交付物:**
- BigQuery SQL 脚本，用于生成 `Resource Allocation Recommendation`。
- `fact_daily_operational_summary` 表现在包含 `main_drivers` 和 `recommendations` 字段。
- 验证了推荐逻辑的准确性和业务相关性。

### Phase 6: 报表与监控 (Reporting & Monitoring) - (约 2-3 周)

**目标:** 为运营团队提供可视化界面，并建立全面的系统监控。 **工作内容:**

- **Looker Studio / Streamlit Dashboard:**
    - 创建仪表板，展示 `fact_daily_operational_summary` 表中的核心信息。
    - 可视化：运营负荷地图 (按 Borough 颜色深浅表示分数)，排名列表，驱动因素饼图/柱状图，资源建议文本框。
    - 确保仪表板响应迅速，数据更新及时。
- **系统监控与告警:**
    - 配置 Cloud Monitoring 和 Stackdriver Logging，监控 Airflow DAG 状态、Dataproc 资源使用、BigQuery 查询性能。
    - 设置关键告警（例如：DAG 失败、数据摄取延迟、数据量异常波动、BigQuery 成本超预期）。
- **文档更新:** 最终项目文档、数据字典、操作手册。 **交付物:**
- 功能完整的 Looker Studio / Streamlit 仪表板。
- 配置好的 Cloud Monitoring 仪表板和告警规则。
- 项目最终文档。
- 用户培训和移交。

---

## 4. 关键设计考虑 (Key Design Considerations)

- **数据质量 (Data Quality):**
    - **Schema Enforcement:** 在 Spark ETL 阶段严格执行，防止脏数据流入 Silver 层。
    - **Data Validation:** 检查关键字段的范围、格式和完整性（例如，经纬度是否在NYC范围内）。
    - **Monitoring:** 对 Bronze 到 Silver，Silver 到 Gold 的数据量和关键指标进行监控，异常时告警。
- **错误处理与幂等性 (Error Handling & Idempotency):**
    - 所有 Airflow 任务应具备重试机制。
    - 摄取和 ETL 任务应设计为幂等，即重复运行不会产生额外副作用或重复数据。例如，使用 `INSERT OVERWRITE PARTITION` 或基于唯一键的 `MERGE` 操作。
- **可扩展性 (Scalability):**
    - GCP 托管服务（Cloud Composer, Dataproc, BigQuery, GCS）本身具有高可扩展性。
    - BigQuery 的分区和聚簇设计对于大规模数据查询性能至关重要。
    - Spark/Dataproc 集群大小应根据数据量和处理需求动态调整。
- **安全性 (Security):**
    - **IAM:** 严格控制对 GCS buckets、BigQuery tables 和 Cloud Composer/Dataproc 的访问权限。遵循最小权限原则。
    - **VPC Service Controls:** （可选，但推荐用于企业级）进一步限制数据外泄风险。
    - **加密:** GCS 和 BigQuery 数据默认静态加密，传输中数据加密。
- **成本优化 (Cost Optimization):**
    - **GCS:** 利用生命周期管理策略，将不常访问的 Bronze 数据转为冷存储或删除。
    - **Dataproc:** 使用短暂集群 (Ephemeral Clusters)，Spark 作业完成后自动关闭，避免长时间运行。选择合适的机器类型和大小。
    - **BigQuery:** 优化查询，避免全表扫描；利用分区和聚簇；对不常用的表设置合理的数据过期策略。
    - **Cloud Composer:** 选择适当的节点数和机器类型。
- **监控与告警 (Monitoring & Alerting):**
    - **数据管道:** 监控 Airflow DAGs 的成功/失败、运行时间、数据延迟。
    - **数据质量:** 监控数据量、关键指标的异常值、缺失率。
    - **资源利用:** 监控 Dataproc CPU/内存、BigQuery Slot 利用率、GCS 存储成本。
    - **告警:** 配置 Slack/Email 通知。
- **CI/CD (Continuous Integration/Continuous Deployment):**
    - 所有 Airflow DAGs、PySpark 脚本和 BigQuery SQL 定义都应通过 Git 版本控制。
    - 使用 Cloud Build 或其他 CI/CD 工具自动化测试和部署代码到 Cloud Composer 和 BigQuery。
- **文档 (Documentation):**
    - **数据字典:** 详细记录所有表、字段的定义、来源、业务含义和数据质量规则。
    - **架构文档:** 维护最新的系统架构图和组件描述。
    - **操作手册:** 针对日常运维、故障排查提供指导。