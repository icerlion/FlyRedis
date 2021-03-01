# FlyRedis
C++ Redis 客户端库，基于Boost.asio实现.
本项目依赖于*boost_1_72_0*, RedisServer 支持 *5.0+* 以上的版本. 同事，你也可以尝试使用更高版本的boost或者RedisServer.

[README in English](https://github.com/icerlion/FlyRedis/blob/master/README.md) 

## 2021-03-01: 支持发布订阅指令!
## 2020-11-18: 支持设置读取超时时间!
## 2020-07-09: 支持TLS!
## 2020-06-20: 支持RESP3! 


[![Build Status](https://travis-ci.com/icerlion/FlyRedis.svg?branch=master)](https://travis-ci.com/icerlion/FlyRedis)
[![license](https://img.shields.io/github/license/icerlion/FlyRedis.svg)](https://github.com/icerlion/FlyRedis/blob/master/LICENSE)

****

### 依赖
boost.asio

### 如何使用FlyRedis?

*Option1: 将 FlyRedis 作为一个静态库*  
*Option2: ___推荐___ 将源码包含到你的项目中，该库仅仅有两个文件，目录为{fly_redis_home}/include/FlyRedis/*, FlyRedis.h and FlyRedis.cpp  

### 如何构建静态库?
Windows: {fly_redis_home}/FlyRedis.vcxproj    
Linux: {fly_redis_home}/Makefile    

### 如何测试FlyRedis?
Windows: 
*{fly_redis_home}/example/full_sample/full_sample.vcxproj  
*{fly_redis_home}/example/pubsub_sample/pubsub_sample.vcxproj  
Linux: 
*{fly_redis_home}/example/full_sample/Makefile  
*{fly_redis_home}/example/pubsub_sample/Makefile  

### 如何在你的代码中使用FlyRedis

```
// 如果你想收集FlyuRedis的日志信息，可以为不同级别的日志设计对应的日志手机函数, 调用CFlyRedis::SetLoggerHandler即可
CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Notice, YourLoggerFunction);
CFlyRedisClient hFlyRedisClient;
hFlyRedisClient.SetRedisConfig(strRedisAddr, strPassword);
// 如果你想实现读写分离，需要调用CFlyRedisClient::SetFlyRedisReadWriteType, 
// 然后，所有的读取指令都将会被发送到slave执行。
// 确认模式是读写指令都会发送到主节点上执行
hFlyRedisClient.SetFlyRedisReadWriteType(FlyRedisReadWriteType::ReadOnSlaveWriteOnMaster);
hFlyRedisClient.Open();
std::string strResult;
int nResult = 0;
hFlyRedisClient.SET("key", "value", strResult);
hFlyRedisClient.GET("key", strResult);
hFlyRedisClient.DEL("key", nResult);
```

### 对Redis指令的支持

本项目并没有支持全部的Redis指令，未来我将会添加对更多指令的支持。于此同时，你也可以调整代码以添加对相应指令的支持，或者你也可以给我提issue或者发送邮件，我将会尽快完成对新指令的支持。

### 如何在FlyRedis中打开SSL/TSL

首先，构建Redis的时候你需要打开TLS，参考：https://redis.io/topics/encryption
然后，运行redis-server时打开TLS端口。
最后，在编译是添加预编译宏：FLY_REDIS_ENABLE_TLS
```
CFlyRedisClient* pFlyRedisClient = new CFlyRedisClient();
pFlyRedisClient->SetTLSContext("redis.crt", "redis.key", "ca.crt");
pFlyRedisClient->SetRedisConfig(CONFIG_REDIS_ADDR, CONFIG_REDIS_PASSWORD);
pFlyRedisClient->Open();
pFlyRedisClient->HELLO(CONFIG_RESP_VER);
```

### 如何使用Publish/Subscribe指令?

在调用了 SUBSCRIBE/PSUBSCRIBE之后，你需要调用 PollSubscribeMsg/PollPSubscribeMsg来轮询订阅消息
```
// Subscribe代码样例
CFlyRedisClient* pFlyRedisClient = new CFlyRedisClient();
pFlyRedisClient->SetRedisConfig(CONFIG_REDIS_ADDR, CONFIG_REDIS_PASSWORD);
pFlyRedisClient->Open();
pFlyRedisClient->HELLO(CONFIG_RESP_VER);
FlyRedisSubscribeResponse stFlyRedisSubscribeResponse;
pFlyRedisClient->SUBSCRIBE("ch1", stFlyRedisSubscribeResponse);
//pFlyRedisClient->PSUBSCRIBE("ch*", stFlyRedisSubscribeResponse);
//std::vector<std::string> vecChannel;
//vecChannel.push_back("c*");
//vecChannel.push_back("ch*");
//pFlyRedisClient->PSUBSCRIBE(vecChannel, vecFlyRedisSubscribeResponse);
while (true)
{
    std::vector<FlyRedisSubscribeResponse> vecSubscribeRst;
    pFlyRedisClient->PollSubscribeMsg(vecSubscribeRst, 10);
    for (auto& stResponse : vecSubscribeRst)
    {
        printf("%zu,Subscribe,%s,%s,%s\n", time(nullptr), stResponse.strCmd.c_str(), stResponse.strChannel.c_str(), stResponse.strMsg.c_str());
    }
}
```
下面的代码演示如何发布消息.
```
int nResult = 0;
CFlyRedisClient* pFlyRedisClient = new CFlyRedisClient();
pFlyRedisClient->SetRedisConfig(CONFIG_REDIS_ADDR, CONFIG_REDIS_PASSWORD);
pFlyRedisClient->Open();
pFlyRedisClient->HELLO(CONFIG_RESP_VER);
std::string strChannel = "ch_name";
std::string strMsg = "msg-content";
hFlyRedisClient.PUBLISH(strChannel, strMsg, nResult);
```
