#include "FlyRedis/FlyRedis.h"
#include "boost/thread.hpp"

void Logger(const char* pszLevel, const char* pszMsg)
{
    int nCurTime = (int)time(nullptr);
    printf("%d - %s - %s\n", nCurTime, pszLevel, pszMsg);
}

void LoggerDebug(const char* pszMsg)
{
    Logger("Debug", pszMsg);
}
void LoggerNotice(const char* pszMsg)
{
    Logger("Notice", pszMsg);
}
void LoggerError(const char* pszMsg)
{
    Logger("Error", pszMsg);
}
void LoggerWarning(const char* pszMsg)
{
    Logger("Warning", pszMsg);
}
void LoggerCommand(const char* pszMsg)
{
    Logger("Command", pszMsg);
}

void ThreadTestFlyRedis(std::string strRedisAddr, std::string strPassword, bool bUseTLSFlag)
{
    CFlyRedisClient hFlyRedisClient;
    hFlyRedisClient.SetRedisConfig(strRedisAddr, strPassword);
    hFlyRedisClient.SetRedisReadWriteType(FlyRedisReadWriteType::ReadWriteOnMaster);
    hFlyRedisClient.SetReadTimeoutSeconds(60);
#ifdef FLY_REDIS_ENABLE_TLS
    if (bUseTLSFlag && !hFlyRedisClient.SetTLSContext("./tls/redis.crt", "./tls/redis.key", "./tls/ca.crt", ""))
    {
        return;
    }
#else
    if (bUseTLSFlag)
    {
        LoggerError("TLS Not Enable, Please Define Macro FLY_REDIS_ENABLE_TLS When Compile");
        return;
    }
#endif // FLY_REDIS_ENABLE_TLS
    //hFlyRedisClient.SetRedisClusterDetectType(FlyRedisClusterDetectType::DisableCluster);
    if (!hFlyRedisClient.Open())
    {
        return;
    }
    hFlyRedisClient.HELLO(3);
    hFlyRedisClient.HELLO_AUTH_SETNAME(2, "default", "123456", "FlyRedis");
    hFlyRedisClient.HELLO_AUTH_SETNAME(3, "default", "123456", "FlyRedis");
    //////////////////////////////////////////////////////////////////////////
    RedisResponse stRedisResponse;
    double fResult = 0.0f;
    int nResult = 0;
    //////////////////////////////////////////////////////////////////////////
    hFlyRedisClient.SET("{mkey}:1", "val1");
    hFlyRedisClient.SET("{mkey}:2", "val2");
    std::vector<std::string> vecMKey;
    vecMKey.push_back("{mkey}:1");
    vecMKey.push_back("{mkey}:2");
    hFlyRedisClient.MGET(vecMKey, stRedisResponse.vecRedisResponse);
    // You are free to run every redis cmd.
    time_t nBeginTime = time(nullptr);
    //////////////////////////////////////////////////////////////////////////
    std::string strKey = "floatkey";
    hFlyRedisClient.SET(strKey, std::to_string(1.5f));
    hFlyRedisClient.INCRBYFLOAT(strKey, 0.1f, fResult);
    //////////////////////////////////////////////////////////////////////////
    for (int i = 0; i < 1000; ++i)
    {
        if (i % 100 == 0)
        {
            LoggerNotice(std::to_string(i).c_str());
        }
        strKey = "key_" + std::to_string(i);
        if (!hFlyRedisClient.SET(strKey, "value"))
        {
            LoggerError("SET FAILED");
            continue;
        }
        if (!hFlyRedisClient.GET(strKey, stRedisResponse.strRedisResponse))
        {
            LoggerError("GET FAILED");
            continue;
        }
        if (!hFlyRedisClient.DEL(strKey, nResult))
        {
            LoggerError("DEL FAILED");
            continue;
        }
        strKey = "hashkey_" + std::to_string(i);
        for (int j = 0; j < 5; ++j)
        {
            std::string strHashField = "field_" + std::to_string(j);
            if (!hFlyRedisClient.HSET(strKey, strHashField, "value", nResult))
            {
                LoggerError("HSET FAILED");
                continue;
            }
            if (!hFlyRedisClient.HGET(strKey, strHashField, stRedisResponse.strRedisResponse))
            {
                LoggerError("HGET FAILED");
                continue;
            }
            if (!hFlyRedisClient.HGETALL(strKey, stRedisResponse.mapRedisResponse))
            {
                LoggerError("HGET FAILED");
                continue;
            }
        }
        strKey = "setkey_" + std::to_string(i);
        for (int j = 0; j < 5; ++j)
        {
            std::string strValue = "sval_" + std::to_string(j);
            if (!hFlyRedisClient.SADD(strKey, strValue, nResult))
            {
                LoggerError("SADD FAILED");
                continue;
            }
            if (!hFlyRedisClient.SMEMBERS(strKey, stRedisResponse.setRedisResponse))
            {
                LoggerError("SMEMBERS FAILED");
                continue;
            }
        }
    }
    time_t nElapsedTime = time(nullptr) - nBeginTime;
    LoggerNotice(("TimeCost: " + std::to_string(nElapsedTime)).c_str());
}

int main(int argc, char* argv[])
{
    // ./sample 192.168.1.10:1000 123456 tls 1
    // Start Redis server enable tls
    // redis-server --tls-port 2000 --port 1000 --tls-cert-file ./tests/tls/redis.crt --tls-key-file ./tests/tls/redis.key --tls-ca-cert-file ./tests/tls/ca.crt --bind 192.168.1.10 --requirepass 123455
    if (argc != 5)
    {
        // Param: 127.0.0.1:8000 123456 tls 1
        printf("sample redis_ip:redis_port redis_password enable_tls thread_count\n");
        return -1;
    }
    std::string strRedisAddr = argv[1];
    std::string strPassword = argv[2];
    bool bUseTLSFlag = (0 == strcmp("tls", argv[3]));
    int nThreadCount = atoi(argv[4]);
    // Config FlyRedis, but it's not not necessary
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Debug, LoggerDebug);
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Notice, LoggerNotice);
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Error, LoggerError);
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Warning, LoggerWarning);
    //CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Command, LoggerCommand);
    boost::thread_group tg;
    for (int i = 0; i < nThreadCount; ++i)
    {
        tg.create_thread(boost::bind(ThreadTestFlyRedis, strRedisAddr, strPassword, bUseTLSFlag));
    }
    tg.join_all();
    LoggerNotice("Done Test");
    return 0;
}