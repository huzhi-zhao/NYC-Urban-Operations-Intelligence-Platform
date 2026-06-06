# Source Registry

> **用途**：所有上游数据源的权威清单。新接入数据源必须先在此登记，再开始开发。  
> **维护人**：数据平台团队 (Data Platform Team)  
> **更新频率**：每次新增/下线数据源时同步更新  
> **关联文档**：`03-data-sources/ingestion-specs.md` · `02-data-contracts/consumer-contracts/`

---

## 快速索引

|ID|数据源名称|类型|负责团队|状态|优先级|
|---|---|---|---|---|---|
|SRC-NYC-001|NYC 311 Service Requests|REST API (Socrata)|城市运营组|✅ 生产|P0|
|SRC-NYC-002|NYPD Motor Vehicle Collisions|REST API (Socrata)|交通警务组|✅ 生产|P0|
|SRC-NYC-003|Open-Meteo Weather API|REST API|气象数据组|✅ 生产|P1|
|SRC-NYC-004|NYC Spatial Boundaries|GeoJSON (Static)|GIS 分析组|✅ 生产|P2|

**状态说明**：✅ 生产 · 🚧 接入中 · ⏸ 暂停 · ❌ 已下线

---

## 详细登记表

---

### SRC-NYC-001 · NYC 311 Service Requests

**基本信息**

|字段|内容|
|---|---|
|数据源 ID|SRC-NYC-001|
|系统名称|NYC Open Data - 311 Requests|
|数据类型|市民服务请求（噪音、供暖、街道坑洼等）|
|接入方式|REST API（Socrata SoQL / 增量拉取）|
|数据格式|JSON|
|状态|✅ 生产|
|接入日期|2023-10-25|

**联系人**

|角色|姓名|联系方式|
|---|---|---|
|数据提供方|NYC Open Data Admin|[opendata@doitt.nyc.gov](mailto:opendata@doitt.nyc.gov)|
|数据工程对接人|数据平台部|#channel-data-eng|
|紧急联系（On-call）|Airflow 监控告警组|PagerDuty: `nyc-ingestion-alerts`|

**技术规格**

NYC Open Data (Socrata API)  
网页查看/手动下载 CSV 网址: https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9

Text

Endpoint:     https://data.cityofnewyork.us/resource/erm2-nwe9.json

Auth:         公开免密（如触发限流，需在 Socrata 申请 App Token）

凭证存储:      Vault: secret/data/socrata/app-token (备用)

网络策略:      公网访问，出向 NAT 需开放 443 端口

**拉取策略**

|参数|配置|
|---|---|
|拉取方式|增量（按 `created_date` 时间戳）|
|调度频率|每天 02:00 AM (EST)|
|分页限制|Socrata API 默认 `limit=1000`，需实现 `$limit` 和 `$offset` 分页|
|历史回溯|支持，数据最早可追溯至 2010 年|
|时区|EST (America/New_York)|

**数据量估算**

|表/端点|日增量|全量大小|备注|
|---|---|---|---|
|/erm2-nwe9|~8,000 - 12,000 行|~35M 行|冬季供暖期投诉量会激增|

**已知问题与注意事项**

- ⚠️ **数据延迟**：部分工单的 `closed_date` 会在创建数天后更新，Bronze 到 Silver 的 ETL 需要处理 Late-arriving Updates（UPSERT 逻辑）。
- ⚠️ **空间数据缺失**：约 5% 的记录 `latitude` 和 `longitude` 为空，需在下游用 `borough` 或 `incident_zip` 做兜底聚合。
- `status` 字段包含多种状态 ("Open", "Closed", "Pending")，计算负荷时需综合考虑。

**数据契约**

- API Schema：`03-data-sources/api-contracts/nyc-311-schema.json`

---

### SRC-NYC-002 · NYPD Motor Vehicle Collisions

**基本信息**

|字段|内容|
|---|---|
|数据源 ID|SRC-NYC-002|
|系统名称|NYC Open Data - NYPD Collisions|
|数据类型|交通事故、受伤/死亡人数、事故原因|
|接入方式|REST API（Socrata SoQL / 增量拉取）|
|数据格式|JSON|
|状态|✅ 生产|
|接入日期|2023-10-25|

**联系人**

|角色|姓名|联系方式|
|---|---|---|
|数据工程对接人|数据平台部|#channel-data-eng|

**技术规格**

网页查看/手动下载 CSV 网址: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95

Text

Endpoint:     https://data.cityofnewyork.us/resource/h9gi-nx95.json

Auth:         公开免密

**拉取策略**

|参数|配置|
|---|---|
|拉取方式|增量（按 `crash_date` 和 `crash_time`）|
|调度频率|每天 03:00 AM (EST)|
|限流限制|同 SRC-NYC-001|
|时区|EST|

**数据量估算**

|表/端点|日增量|全量大小|备注|
|---|---|---|---|
|/h9gi-nx95|~200 - 400 行|~2M 行|极端天气日增量翻倍|

**已知问题与注意事项**

- ⚠️ **滞后录入 (Late Arriving Facts)**：警局录入系统存在延迟，昨天的事故可能在 3 天后才出现在 API 中。Airflow 需配置 Lookback Window（每次拉取过去 7 天的数据并根据 `collision_id` 去重）。
- `contributing_factor_vehicle_1` 存在大量模糊文本 ("Unspecified")，需要在 Silver 层进行数据标准化。

---

### SRC-NYC-003 · Open-Meteo Weather API

**基本信息**

|字段|内容|
|---|---|
|数据源 ID|SRC-NYC-003|
|系统名称|Open-Meteo (开源气象局 API)|
|数据类型|纽约市逐小时历史天气、未来 7 天天气预报|
|接入方式|REST API（全量覆盖拉取）|
|数据格式|JSON|
|状态|✅ 生产|
|接入日期|2023-10-26|

**技术规格**

API 文档网址: https://open-meteo.com/

Text

Endpoint:     https://api.open-meteo.com/v1/forecast

获取纽约实时/预报 API Endpoint 示例: https://api.open-meteo.com/v1/forecast?latitude=40.7143&longitude=-74.006&hourly=temperature_2m,precipitation,snowfall,windspeed_

Auth:         无（无需 API Key）

限制:         非商业开源使用：< 10,000 req/day

请求参数:      latitude=40.7143, longitude=-74.006, hourly=temperature_2m,snowfall,precipitation

**拉取策略**

|参数|配置|
|---|---|
|拉取方式|Snapshot（每天拉取全量未来 7 天预测和过去 3 天实际历史，覆盖式写入）|
|调度频率|每天 06:00 AM (EST)|
|历史回溯|Open-Meteo Archive API 可获取几十年的历史数据|

**已知问题与注意事项**

- 天气预报是动态变化的（预测会随着时间推移而改变）。为了保证离线训练或复盘的数据一致性，GCS Bronze 层必须严格按照拉取的 `execution_date` 进行目录分区隔离，保留预测的历史快照。

---

### SRC-NYC-004 · NYC Spatial Boundaries

**基本信息**

|字段|内容|
|---|---|
|数据源 ID|SRC-NYC-004|
|系统名称|NYC Dept of City Planning|
|数据类型|Borough (行政区) 和 NTA (社区) 多边形边界|
|接入方式|静态文件下载|
|数据格式|GeoJSON|
|状态|✅ 生产|
|接入日期|2023-10-26|

**技术规格**

NYC 行政区 (Boroughs) GeoJSON 下载: https://data.cityofnewyork.us/City-Government/Borough-Boundaries/tqmj-j8zm (点击 Export -> GeoJSON)

Text

Borough 端点: https://data.cityofnewyork.us/City-Government/Borough-Boundaries/tqmj-j8zm (Export GeoJSON)

**拉取策略**

|参数|配置|
|---|---|
|拉取方式|静态（Static Load）|
|调度频率|仅在系统初始化，或城市行政区划变更时手动触发|

**已知问题与注意事项**

- ⚠️ BigQuery 的 `GEOGRAPHY` 数据类型要求多边形的坐标系必须是 WGS84。如果是其他 EPSG 投影，需要在 Spark ETL 层使用 GeoPandas/Sedona 进行 CRS (Coordinate Reference System) 转换后再落入 Gold 层。

---

## 新增数据源流程

Text

1. 在此文件添加登记表（状态标为"接入中"）

2. 在 data-contracts 中补充相应的 JSON Schema 定义

3. 确认网络连通性及 Socrata API 限制

4. 在 ingestion-specs.md 补充 Airflow DAG 的调度和分页重试策略

5. 开发并发布 Bronze 层代码后将状态更新为"生产"

---

## 变更日志

|日期|变更内容|操作人|
|---|---|---|
|2023-10-26|新增 SRC-NYC-003, SRC-NYC-004 天气与空间维度数据源|数据架构师|
|2023-10-25|确立项目基线，新增 SRC-NYC-001, SRC-NYC-002 核心事件流接入|数据架构师|