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
    hFlyRedisClient.SetRedisReadTimeOutSeconds(10);
    hFlyRedisClient.SetRedisReadWriteType(FlyRedisReadWriteType::ReadWriteOnMaster);
    //hFlyRedisClient.SetRedisClusterDetectType(FlyRedisClusterDetectType::DisableCluster);
    if (!hFlyRedisClient.Open())
    {
        return;
    }
    // You are free to run every redis cmd.
    time_t nBeginTime = time(nullptr);
    std::string strResult;
    int nResult = 0;
    std::vector<std::string> vecResult;
    for (int i = 0; i < 10000; ++i)
    {
        std::string strKey = "key_" + std::to_string(i);
        if (!hFlyRedisClient.SET(strKey, "value"))
        {
            Logger("SET FAILED");
            continue;
        }
        if (!hFlyRedisClient.GET(strKey, strResult))
        {
            Logger("GET FAILED");
            continue;
        }
        if (!hFlyRedisClient.DEL(strKey, nResult))
        {
            Logger("DEL FAILED");
            continue;
        }
        if (!hFlyRedisClient.SCAN(0, "*", 10, nResult, vecResult))
        {
            Logger("SCAN FAILED");
            continue;
        }
        std::vector<std::string> vecRedisNode;
        hFlyRedisClient.FetchRedisNodeList(vecRedisNode);
        for (auto& strNode : vecRedisNode)
        {
            hFlyRedisClient.ChoseCurRedisNode(strNode);
            if (!hFlyRedisClient.SCAN(0, "*", 10, nResult, vecResult))
            {
                Logger("SCAN FAILED");
            }
        }
    }
    time_t nElapsedTime = time(nullptr) - nBeginTime;
    Logger(("TimeCost: " + std::to_string(nElapsedTime)).c_str());
}

int main(int argc, char* argv[])
{
    if (argc != 4)
    {
        // Param: 127.0.0.1:8000 123456 1
        Logger("sample redis_ip:redis_port redis_password thread_count");
        return -1;
    }
    std::string strRedisAddr = argv[1];
    std::string strPassword = argv[2];
    int nThreadCount = atoi(argv[3]);
    // Config FlyRedis, but it's not not necessary
    //CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Debug, Logger);
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Notice, Logger);
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Error, Logger);
    //CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Warning, Logger);
    //CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Command, Logger);
    boost::thread_group tg;
    for (int i = 0; i < nThreadCount; ++i)
    {
        tg.create_thread(boost::bind(ThreadTestFlyRedis, strRedisAddr, strPassword));
    }
    tg.join_all();
    Logger("Done Test");
    return 0;
}