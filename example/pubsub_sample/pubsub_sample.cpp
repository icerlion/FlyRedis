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

bool InitFlyRedisClient(CFlyRedisClient& hFlyRedisClient, const std::string& strIP, int nPort, const std::string& strPassword, bool bUseTLSFlag)
{
    hFlyRedisClient.SetRedisConfig(strIP, nPort, strPassword);
    hFlyRedisClient.SetRedisReadWriteType(FlyRedisReadWriteType::ReadWriteOnMaster);
    hFlyRedisClient.SetReadTimeoutSeconds(60);
#ifdef FLY_REDIS_ENABLE_TLS
    if (bUseTLSFlag && !hFlyRedisClient.SetTLSContext("./tls/redis.crt", "./tls/redis.key", "./tls/ca.crt", ""))
    {
        return false;
    }
#else
    if (bUseTLSFlag)
    {
        LoggerError("TLS Not Enable, Please Define Macro FLY_REDIS_ENABLE_TLS When Compile");
        return false;
    }
#endif // FLY_REDIS_ENABLE_TLS
    //hFlyRedisClient.SetRedisClusterDetectType(FlyRedisClusterDetectType::DisableCluster);
    if (!hFlyRedisClient.Open())
    {
        return false;
    }
    return true;
}

void TestPubSub(const std::string& strIP, int nPort, const std::string& strPassword, bool bUseTLSFlag)
{
    CFlyRedisClient hFlyRedisClient;
    if (!InitFlyRedisClient(hFlyRedisClient, strIP, nPort, strPassword, bUseTLSFlag))
    {
        return;
    }
    int nResult = 0;
    std::vector<std::string> vecRedisNodeList = hFlyRedisClient.FetchRedisNodeList();
    for (auto& strNode : vecRedisNodeList)
    {
        hFlyRedisClient.ChooseCurRedisNode(strNode);
        hFlyRedisClient.PUBSUB_NUMPAT(nResult);
        hFlyRedisClient.PUBSUB_NUMSUB("", nResult);
        hFlyRedisClient.PUBSUB_NUMSUB("ch1", nResult);
        std::map<std::string, int> mapKVP;
        std::vector<std::string> vecChannel;
        vecChannel.push_back("ch1");
        vecChannel.push_back("ch2");
        vecChannel.push_back("ch3");
        hFlyRedisClient.PUBSUB_NUMSUB(vecChannel, mapKVP);
        std::vector<std::string> vecResult;
        hFlyRedisClient.PUBSUB_CHANNELS("", vecResult);
        vecResult.clear();
        hFlyRedisClient.PUBSUB_CHANNELS("ch1", vecResult);
        vecResult.clear();
        hFlyRedisClient.PUBSUB_CHANNELS("ch*", vecResult);
    }
    for (int i = 0; i < 10; ++i)
    {
        std::string strMsg = "msg-" + std::to_string(time(nullptr)) + "-" + std::to_string(i);
        for (int j = 0; j < 10; ++j)
        {
            std::string strChannel = "ch" + std::to_string(j);
            hFlyRedisClient.PUBLISH(strChannel, strMsg, nResult);
            for (auto& strNode : vecRedisNodeList)
            {
                hFlyRedisClient.ChooseCurRedisNode(strNode);
                hFlyRedisClient.PUBSUB_NUMPAT(nResult);
            }
        }
    }
    for (int i = 1; i < 2; ++i)
    {
        std::string strChannel1 = "ch" + std::to_string(i);
        std::string strChannel2 = "ch" + std::to_string(i * 2);
        std::vector<std::string> vecChannel;
        vecChannel.push_back(strChannel1);
        vecChannel.push_back(strChannel2);
        for (auto& strNode : vecRedisNodeList)
        {
            hFlyRedisClient.ChooseCurRedisNode(strNode);
            std::vector<FlyRedisSubscribeResponse> vecResult;
            hFlyRedisClient.SUBSCRIBE(vecChannel, vecResult);
            time_t nCurTime = time(nullptr);
            time_t nBeginTime = nCurTime;
            while (nCurTime - nBeginTime < 10)
            {
                nCurTime = time(nullptr);
                std::vector<FlyRedisSubscribeResponse> vecSubscribeRst;
                hFlyRedisClient.PollSubscribeMsg(vecSubscribeRst, 100);
                for (auto& stRst : vecSubscribeRst)
                {
                    printf("%zu - %s - %s - %s\n", nCurTime, stRst.strCmd.c_str(), stRst.strChannel.c_str(), stRst.strMsg.c_str());
                    if (0 == stRst.strMsg.compare("un1"))
                    {
                        std::vector<std::string> vecUnSubResult;
                        hFlyRedisClient.UNSUBSCRIBE(strChannel1, vecUnSubResult);
                    }
                    else if (0 == stRst.strMsg.compare("un2"))
                    {
                        std::vector<std::string> vecUnSubResult;
                        hFlyRedisClient.UNSUBSCRIBE(strChannel2, vecUnSubResult);
                    }
                    else if (0 == stRst.strMsg.compare("un"))
                    {
                        std::vector<std::string> vecUnSubResult;
                        hFlyRedisClient.UNSUBSCRIBE("", vecUnSubResult);
                    }
                }
                if (nCurTime % 2 == 0)
                {
                    std::string strPingResponse;
                    hFlyRedisClient.PING(std::to_string(nCurTime), strPingResponse);
                    printf("PING %zu - %s, Node,%s\n", nCurTime, strPingResponse.c_str(), strNode.c_str());
                    boost::this_thread::sleep_for(boost::chrono::seconds(1));
                }
            }
        }
    }
    return;
}


void ThreadPublish(const std::string& strIP, int nPort, const std::string& strPassword, bool bUseTLSFlag)
{
    CFlyRedisClient hFlyRedisClient;
    if (!InitFlyRedisClient(hFlyRedisClient, strIP, nPort, strPassword, bUseTLSFlag))
    {
        return;
    }
    int nResult = 0;
    time_t nBeginTime = time(nullptr);
    while (time(nullptr) - nBeginTime < 10)
    {
        std::string strChannel = "ch1";
        std::string strMsg = "ch1-msg-" + std::to_string(time(nullptr)) + "-" + std::to_string(rand());
        hFlyRedisClient.PUBLISH(strChannel, strMsg, nResult);
        strChannel = "ch2";
        strMsg = "ch2-msg-" + std::to_string(time(nullptr)) + "-" + std::to_string(rand());
        hFlyRedisClient.PUBLISH(strChannel, strMsg, nResult);
        boost::this_thread::sleep_for(boost::chrono::milliseconds(100));
    }
}

void ThreadSubscribe(const std::string& strIP, int nPort, const std::string& strPassword, bool bUseTLSFlag)
{
    CFlyRedisClient hFlyRedisClient;
    if (!InitFlyRedisClient(hFlyRedisClient, strIP, nPort, strPassword, bUseTLSFlag))
    {
        return;
    }
    FlyRedisSubscribeResponse stFlyRedisSubscribeResponse;
    std::vector<FlyRedisSubscribeResponse> vecFlyRedisSubscribeResponse;
    //////////////////////////////////////////////////////////////////////////
    hFlyRedisClient.SUBSCRIBE("ch1", stFlyRedisSubscribeResponse);
    //std::vector<std::string> vecChannel;
    //vecChannel.push_back("ch1");
    //vecChannel.push_back("ch2");
    //hFlyRedisClient.SUBSCRIBE(vecChannel, vecFlyRedisSubscribeResponse);
    //////////////////////////////////////////////////////////////////////////
    time_t nBeginTime = time(nullptr);
    while (time(nullptr) - nBeginTime < 10)
    {
        std::vector<FlyRedisSubscribeResponse> vecSubscribeRst;
        hFlyRedisClient.PollSubscribeMsg(vecSubscribeRst, 10);
        for (auto& stResponse : vecSubscribeRst)
        {
            printf("%zu,Subscribe,%s,%s,%s\n", time(nullptr), stResponse.strCmd.c_str(), stResponse.strChannel.c_str(), stResponse.strMsg.c_str());
        }
    }
}

void ThreadPSubscribe(const std::string& strIP, int nPort, const std::string& strPassword, bool bUseTLSFlag)
{
    CFlyRedisClient hFlyRedisClient;
    if (!InitFlyRedisClient(hFlyRedisClient, strIP, nPort, strPassword, bUseTLSFlag))
    {
        return;
    }
    FlyRedisSubscribeResponse stFlyRedisSubscribeResponse;
    std::vector<FlyRedisSubscribeResponse> vecFlyRedisSubscribeResponse;
    //////////////////////////////////////////////////////////////////////////
    //hFlyRedisClient.PSUBSCRIBE("ch*", stFlyRedisSubscribeResponse);
    std::vector<std::string> vecChannel;
    vecChannel.push_back("c*");
    vecChannel.push_back("ch*");
    hFlyRedisClient.PSUBSCRIBE(vecChannel, vecFlyRedisSubscribeResponse);
    //////////////////////////////////////////////////////////////////////////
    time_t nBeginTime = time(nullptr);
    while (time(nullptr) - nBeginTime < 10)
    {
        std::vector<FlyRedisPMessageResponse> vecSubscribeRst;
        hFlyRedisClient.PollPSubscribeMsg(vecSubscribeRst, 10);
        for (FlyRedisPMessageResponse& stResponse : vecSubscribeRst)
        {
            printf("%zu,PSubscribe,%s,%s,%s,%s\n", time(nullptr), stResponse.strCmd.c_str(), stResponse.strPattern.c_str(), stResponse.strChannel.c_str(), stResponse.strMsg.c_str());
        }
    }
}


int main(int argc, char* argv[])
{
    // ./sample 192.168.1.10 1000 123456 tls 1
    // Start Redis server enable tls
    // redis-server --tls-port 2000 --port 1000 --tls-cert-file ./tests/tls/redis.crt --tls-key-file ./tests/tls/redis.key --tls-ca-cert-file ./tests/tls/ca.crt --bind 192.168.1.10 --requirepass 123455
    if (argc != 5)
    {
        // Param: 127.0.0.1 8000 123456 tls 1
        printf("sample redis_ip redis_port redis_password enable_tls\n");
        return -1;
    }
    std::string strIP = argv[1];
    int nPort = atoi(argv[2]);
    std::string strPassword = argv[3];
    bool bUseTLSFlag = (0 == strcmp("tls", argv[3]));
    // Config FlyRedis, but it's not not necessary
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Debug, LoggerDebug);
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Notice, LoggerNotice);
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Error, LoggerError);
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Warning, LoggerWarning);
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Command, LoggerCommand);
    TestPubSub(strIP, nPort, strPassword, bUseTLSFlag);
    //////////////////////////////////////////////////////////////////////////
    boost::thread_group tgPubSub;
    tgPubSub.create_thread(boost::bind(ThreadPublish, strIP, nPort, strPassword, bUseTLSFlag));
    tgPubSub.create_thread(boost::bind(ThreadSubscribe, strIP, nPort, strPassword, bUseTLSFlag));
    tgPubSub.create_thread(boost::bind(ThreadPSubscribe, strIP, nPort, strPassword, bUseTLSFlag));
    tgPubSub.join_all();
    //////////////////////////////////////////////////////////////////////////
    LoggerNotice("Done Test");
    return 0;
}