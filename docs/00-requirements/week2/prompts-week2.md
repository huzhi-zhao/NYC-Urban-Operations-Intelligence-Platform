#### Daily Target - 1


  1. GCP bucket存储路径 按source/dataset/文件名+月份.json的格式存储， source和dataset 由yaml配置文件维护，数据源如下 
  2. 配置文件目录，参考标准数据开发项目工程， 是统一在.config 还是跟随目录 需要按北美主流行业标准
     - 数据分批下载上传，提高IO+CPU的资源协调利用率
  3. backfill层需要一个主入口，调用不同数据源的backfill脚本，而不同数据源的backfill脚本也需要使用统一的facade实现
  4. tests目录下需求有对backfill各个脚本的调用分支测试，尽可能多地覆盖原代码 而不仅是为了打通链路
