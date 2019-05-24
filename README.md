# FlyRedis
C++ Redis Client, base on Boost.asio.
This project depends on *boost_1_70_0*, and The RedisServer is *5.0+*. At the same time, you can try it with other version of boost and redis server.

[![Build Status](https://travis-ci.com/icerlion/FlyRedis.svg?branch=master)](https://travis-ci.com/icerlion/FlyRedis)
[![license](https://img.shields.io/github/license/icerlion/FlyRedis.svg)](https://github.com/icerlion/FlyRedis/blob/master/LICENSE)


****

### Dependency
boost.asio

### How to use FlyRedis?

*Option1: Use FlyRedis As Statistic library*  
*Option2: ___Recommand___ Include source code in your project, which is {fly_redis_home}/include/FlyRedis/*  

### How to build FlyRedis as Library?
Windows: {fly_redis_home}/build/win/FlyRedis.vcxproj    
Linux: {fly_redis_home}/build/linux/Makefile    

### How to test FlyRedis?
Windows: {fly_redis_home}/sample/sample.vcxproj  
Linux: {fly_redis_home}/sample/Makefile  

### Use FlyRedis In Code

```
CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Notice, YourLoggerFunction);
CFlyRedisClient hFlyRedisClient;
hFlyRedisClient.SetRedisConfig(strRedisAddr, strPassword);
hFlyRedisClient.Open();
std::string strResult;
int nResult = 0;
hFlyRedisClient.SET("key", "value", strResult);
hFlyRedisClient.GET("key", strResult);
hFlyRedisClient.DEL("key", nResult);
```
