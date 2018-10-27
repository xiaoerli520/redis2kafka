# Redis2kafka

使用方向redis指定key的list推送数据（rPush）(注意！！使用方必须使用rPush！！)，本服务会不断从list中读取数据（blpop）并推送指定kafka_topic

# 特性

- 多线程，快速消费RedisList

- 基于Redis的单线程模型，本中间件可以无限水平扩展，提升消费速度

- 优雅退出，保证消息完整传输

- 传输失败的消息会放回redisList，尽可能保证消息不丢失

- 自动监控RedisList，kafka状态，list过长自动报警

- kafka、redis连接丢失后自动重连，不存在长连接问题

- 完善日志以及通知功能

# 配置

当前支持以下redis\_key => kafka：


| 服务名 | redis_key|  对应kafka_topic |
| ------ |  ------ | ------ |
| test | test_r2k | test_kfk |

# 配置文件

```
settings:
  # redisList过长判定长度
  maxRedisList: 10
  # kafka最大消费阻塞时间ms
  maxBlock: 3000
  metaDataMaxAge: 10000
  requestTimeOut: 5000
  kafkaTimeOut: 2       # second
  redisPopTimeOut: 5    # second
  # 异常报警通知
  mailTo: guoqingzhe@hotmail.com
  debugListLenSleep: 5
  listLongSleep: 30
  listSleep: 60
  redisTimeoutSleep: 20
  kafkaTimeoutSleep: 20
  kafkaNoLeaderSleep: 20
  BaseExceptionSleep: 10
  # 每个redisList -> kafka 任务的线程数
  ThreadsNumberPerTask: 2

kafka:
  DevOps:
    # kafka servers
    servers:
      - local.cms.kafka.com:9092
    actionTopics:
      # 要同步的服务名称，以及对应的kafka topic 和 redis list key
      aspectLog:
        topic: test1
        username:
        password:
        apiVersion: "0.10"
        redis:
          dsn: 127.0.0.1:6379
          queue_key: devops_aspectlog
      test:
        topic: test1
        username:
        password:
        apiVersion: "0.10"
        redis:
          dsn: 127.0.0.1:6379
          queue_key: devops_test
```

