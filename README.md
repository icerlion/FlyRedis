# FlyRedis
C++ Redis Client, base on Boost.asio.
This project depends on *boost_1_72_0*, and The RedisServer is *5.0+*. At the same time, you can try it with other version of boost and redis server.

[README in Chinese](https://github.com/icerlion/FlyRedis/blob/master/README.chs.md) 

## 2022-08-28: Upgrade Boost!
## 2021-03-01: Support PUB/SUB CMD!
## 2020-11-18: Enable SetReadTimeoutSeconds!
## 2020-07-09: TLS IS READY NOW!
## 2020-06-20: RESP3 IS READY NOW! 


[![Build Status](https://travis-ci.com/icerlion/FlyRedis.svg?branch=master)](https://travis-ci.com/icerlion/FlyRedis)
[![license](https://img.shields.io/github/license/icerlion/FlyRedis.svg)](https://github.com/icerlion/FlyRedis/blob/master/LICENSE)


****

### Dependency
boost.asio

### How to use FlyRedis?

*Option1: Use FlyRedis As Statistic library*  
*Option2: ___Recommand___ Include source code in your project, which is {fly_redis_home}/include/FlyRedis/*, there only two files, FlyRedis.h and FlyRedis.cpp  

### How to build FlyRedis as Library?
Windows: {fly_redis_home}/FlyRedis.vcxproj    
Linux: {fly_redis_home}/Makefile    

### How to test FlyRedis?
Windows: 
*{fly_redis_home}/example/full_sample/full_sample.vcxproj  
*{fly_redis_home}/example/pubsub_sample/pubsub_sample.vcxproj  
Linux: 
*{fly_redis_home}/example/full_sample/Makefile  
*{fly_redis_home}/example/pubsub_sample/Makefile  

### Use FlyRedis In Your Code

```
// If you want collect RedisLog, you can call CFlyRedis::SetLoggerHandler
CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Notice, YourLoggerFunction);
CFlyRedisClient hFlyRedisClient;
hFlyRedisClient.SetRedisConfig(strRedisAddr, strPassword);
// If you want split read and write, you can call CFlyRedisClient::SetFlyRedisReadWriteType, 
// then the read command will be sent to slave only.
// The default mode was FlyRedisReadWriteType::ReadWriteOnMaster
hFlyRedisClient.SetFlyRedisReadWriteType(FlyRedisReadWriteType::ReadOnSlaveWriteOnMaster);
hFlyRedisClient.Open();
std::string strResult;
int nResult = 0;
hFlyRedisClient.SET("key", "value", strResult);
hFlyRedisClient.GET("key", strResult);
hFlyRedisClient.DEL("key", nResult);
```

### Redis Command Support

This project did not implement all Redis command, And I will add support in the further, at the same time, You can add implementation yourself, or you can send email to me, and I will add it in 7 days.

### How To Enable SSL/TSL in FlyRedis

First of all, you need to build redis with TLS open, ref： https://redis.io/topics/encryption 
Then run redis-server witn TLS port opened.
Add macro: FLY_REDIS_ENABLE_TLS to enable TLS in FlyRedis.
```
CFlyRedisClient* pFlyRedisClient = new CFlyRedisClient();
pFlyRedisClient->SetTLSContext("redis.crt", "redis.key", "ca.crt");
pFlyRedisClient->SetRedisConfig(CONFIG_REDIS_ADDR, CONFIG_REDIS_PASSWORD);
pFlyRedisClient->Open();
pFlyRedisClient->HELLO(CONFIG_RESP_VER);
```

### How To Use Publish/Subscribe Cmd?

After call SUBSCRIBE/PSUBSCRIBE, you should call PollSubscribeMsg/PollPSubscribeMsg
```
// Subscribe code sample
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
The following code show how to publish msg.
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
