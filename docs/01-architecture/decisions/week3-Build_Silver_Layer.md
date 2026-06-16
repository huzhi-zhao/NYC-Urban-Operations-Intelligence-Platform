#  Build Silver Layer

## To Silver 前置操作

### 1. Bronze 数据质量审计（Data Profiling）

在写任何转换代码之前，先摸清楚原始数据的真实面貌：

- 每个字段的 null 率是多少？
- 时间戳字段有没有异常值（1970-01-01、9999-12-31）？- 
- Borough 字段有没有脏数据（"BRONX" vs "Bronx" vs "BX"）？- 
- 数值字段的分布（min/max/mean）是否在合理范围？- 
- 每天的记录数是否连续，有没有某几天数据量异常低（API 断档）？

工具：Great Expectations / dbt tests / 自己写 PySpark profiling job

---

### 2. Schema Contract 冻结

确认 Bronze 实际字段 与 `contracts/api-contracts/` 里记录的一致，**签字锁定**：

- 上游 API 有没有悄悄加字段、改字段名
- Pydantic 模型是否还准确反映现实
- Silver StructType 定义基于的是真实数据，不是文档假设

---

### 3. 采样验证（Sampling）

不跑全量，先抽一个月数据跑通 ETL 逻辑，验证：

- 空间 JOIN 命中率（ST_CONTAINS 覆盖了多少条记录）
- 时间戳标准化后有没有时区错位
- 输出行数与输入行数的比例是否合理

---

### 4. SLA 与数据量基线建立

记录每个源每天/每月的预期行数，用于后续告警阈值：

```
311:   ~8,000 条/天NYPD:  ~300 条/天Weather: 24 条/天（每小时一条）
```

Silver job 跑完后输出 0 行或低于基线 50% → 自动告警。