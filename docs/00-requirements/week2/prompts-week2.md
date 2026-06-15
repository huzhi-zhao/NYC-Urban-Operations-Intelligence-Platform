#### Daily Target - 1


1. GCP bucket存储路径 按source/dataset/文件名+月份.json的格式存储， source和dataset 由yaml配置文件维护，数据源如下 
2. 配置文件目录，参考标准数据开发项目工程， 是统一在./config/source
3. backfill层需要一个主入口，调用不同数据源的backfill脚本，而不同数据源的backfill脚本也需要使用统一的facade实现
   - backfill 需要有一个统一的封装，至少包括upload和fetch写和读两个上层入口，上层调用对不同数据的内部存储结构的不同实现无感，只管给定backfill参数，上传指定时间段的数据（不同数据源 可能有不同的时间段），fetch时也按参数拉取目标数据
4. tests目录下需求有对backfill各个脚本的调用分支测试，尽可能多地覆盖原代码 而不仅是为了打通链路
5. 数据上传和Fetch方式优化
   - 数据分批下载上传，而不是按月下载然后整体上传，提高IO+CPU的资源协调利用率
   - 不同的数据源制定不同的切分逻辑， 有的按月有的按天（根据数据模型而定）
   - facade 层原子api下载最小单位的raw_data + 单个raw_data文件上传
   - 业务层将入参的startdate到enddate时间区间 按天/月切割成n个切片任务，多线程并行执行facade原子上传接口




