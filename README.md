# pg-flow
PostgreSQL WAL Processor/Listener

## 使用
1. 编写业务客户端, 注册并监听事件
2. 运行 NATS Streaming Server
3. 运行客户端
4. 运行 flow-listener 开始监听数据库变化, 如果使用 dumper 导出历史数据(非event模式), [3]客户端运行后退出, 在目标数据仓库导入数据后再运行[3], 以妨止导入数据期间客户端改变数据造成数据不一致


## 注意事项
- clickhouse 需要配置 date_time_input_format='best_effort'

## TODO
- clickhouse ddl
- clickhouse 批量导入
- dump/repository/repository.go 获取列信息 scheme参数动态获取
- dump 运行完开始事件监听
- ddl 引擎生成需要增加更多引擎判断
