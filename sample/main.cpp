#include "FlyRedis/FlyRedis.h"
#include "boost/thread.hpp"

void Logger(const char* pszMsg)
{
    printf("%s\n", pszMsg);
}

void ThreadTestFlyRedis(std::string strRedisAddr, std::string strPassword)
{
    CFlyRedisClient hFlyRedisClient;
    hFlyRedisClient.SetRedisConfig(strRedisAddr, strPassword);
    if (!hFlyRedisClient.Open())
    {
        return;
    }
    // You are free to run every redis cmd.
    std::string strResult;
    int nResult = 0;
    for (int i = 0; i < 10000; ++i)
    {
        if (!hFlyRedisClient.SET("key", "value"))
        {
            Logger("GET FAILED");
            continue;
        }
        if (!hFlyRedisClient.GET("key", strResult))
        {
            Logger("GET FAILED");
            continue;
        }
        if (!hFlyRedisClient.DEL("key", nResult))
        {
            Logger("GET FAILED");
            continue;
        }
    }
}

int main(int argc, char* argv[])
{
    if (argc != 3)
    {
        // Param: 127.0.0.1:8000 123456
        Logger("sample redis_ip:redis_port redis_password");
        return -1;
    }
    std::string strRedisAddr = argv[1];
    std::string strPassword = argv[2];
    // Config FlyRedis, but it's not not necessary
    //CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Debug, Logger);
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Notice, Logger);
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Error, Logger);
    //CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Warning, Logger);
    //CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Command, Logger);
    CFlyRedis::SetRedisReadTimeOutSeconds(10);
    boost::thread_group tg;
    for (int i = 0; i < 10; ++i)
    {
        tg.create_thread(boost::bind(ThreadTestFlyRedis, strRedisAddr, strPassword));
    }
    tg.join_all();
    Logger("Done Test");
    return 0;
}