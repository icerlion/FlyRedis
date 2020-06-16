#include "FlyRedis/FlyRedis.h"
#include "boost/thread.hpp"

#define BOOST_TEST_MODULE UTFlyRedis
#include "boost/test/included/unit_test.hpp"

void Logger(const char* pszLog)
{
    printf("%s\n", pszLog);
}

const std::string CONST_REDIS_ADDR = "127.0.0.1:1000";
const std::string CONST_REDIS_PASSWORD = "123456";
const int CONST_RESP_VER = 3; // RESP should be 2 or 3

#define CREATE_REDIS_CLIENT() \
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Error, Logger); \
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Warning, Logger); \
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Notice, Logger); \
    CFlyRedis::SetLoggerHandler(FlyRedisLogLevel::Command, Logger); \
    CFlyRedisClient* pFlyRedisClient = new CFlyRedisClient(); \
    pFlyRedisClient->SetRedisConfig(CONST_REDIS_ADDR, CONST_REDIS_PASSWORD); \
    BOOST_CHECK(pFlyRedisClient->Open()); \
    pFlyRedisClient->HELLO(CONST_RESP_VER);

#define DESTROY_REDIS_CLIENT() delete pFlyRedisClient;

BOOST_AUTO_TEST_CASE(MISC)
{
    CREATE_REDIS_CLIENT();
    int nUnixTime = 0;
    int nMicroSeconds = 0;
    BOOST_CHECK(pFlyRedisClient->TIME(nUnixTime, nMicroSeconds));
    BOOST_CHECK_GT(nUnixTime, 0);

    int nResult = 0;
    BOOST_CHECK(pFlyRedisClient->LASTSAVE(nResult));
    BOOST_CHECK(pFlyRedisClient->DBSIZE(nResult));
    std::vector<std::string> vecResult;
    BOOST_CHECK(pFlyRedisClient->KEYS("*", vecResult));
    if (!pFlyRedisClient->GetClusterFlag())
    {
        BOOST_CHECK(pFlyRedisClient->SELECT(1));
    }
    DESTROY_REDIS_CLIENT();
}

BOOST_AUTO_TEST_CASE(KEY_STRING)
{
    CREATE_REDIS_CLIENT();
    std::string strKey = "key_" + std::to_string(time(nullptr));
    std::string strValue = "value_" + std::to_string(time(nullptr));
    int nResult = 0;
    std::string strResult;

    BOOST_CHECK(pFlyRedisClient->BITCOUNT(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->EXISTS(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->GETSET(strKey, strValue, strResult));
    BOOST_CHECK_EQUAL(strResult, "");

    BOOST_CHECK(pFlyRedisClient->GETSET(strKey, strValue, strResult));
    BOOST_CHECK_EQUAL(strResult, strValue);

    BOOST_CHECK(pFlyRedisClient->DEL(strKey, nResult));
    BOOST_CHECK(pFlyRedisClient->APPEND(strKey, strValue, nResult));
    BOOST_CHECK_EQUAL(nResult, strValue.length());

    BOOST_CHECK(pFlyRedisClient->TYPE(strKey, strResult));
    BOOST_CHECK_EQUAL(strResult, "string");

    BOOST_CHECK(pFlyRedisClient->TTL(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, -1);

    BOOST_CHECK(pFlyRedisClient->PTTL(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, -1);

    BOOST_CHECK(pFlyRedisClient->EXISTS(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->BITCOUNT(strKey, nResult));
    BOOST_CHECK_GT(nResult, 10);

    BOOST_CHECK(pFlyRedisClient->GETBIT(strKey, 1, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->SETBIT(strKey, 1, 0, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->GETBIT(strKey, 1, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->SETBIT(strKey, 1, 0, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->SET(strKey, strValue));
    BOOST_CHECK(pFlyRedisClient->GET(strKey, strResult));
    BOOST_CHECK_EQUAL(strResult, strValue);

    BOOST_CHECK(pFlyRedisClient->APPEND(strKey, strValue, nResult));
    BOOST_CHECK_EQUAL(nResult, strValue.length() * 2);

    BOOST_CHECK(pFlyRedisClient->STRLEN(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, strValue.length() * 2);

    BOOST_CHECK(pFlyRedisClient->SET(strKey, strValue));
    BOOST_CHECK_EQUAL(strResult, strValue);

    BOOST_CHECK(pFlyRedisClient->SETNX(strKey, strValue, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->DEL(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->DEL(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->SETNX(strKey, strValue, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->SETNX(strKey, strValue, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->SETEX(strKey, 1, strValue, strResult));
    BOOST_CHECK_EQUAL(strResult, "OK");

    BOOST_CHECK(pFlyRedisClient->TTL(strKey, nResult));
    BOOST_CHECK_LE(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->PTTL(strKey, nResult));
    BOOST_CHECK_LE(nResult, 1000);

    BOOST_CHECK(pFlyRedisClient->PSETEX(strKey, 1000, strValue, strResult));
    BOOST_CHECK_EQUAL(strResult, "OK");

    BOOST_CHECK(pFlyRedisClient->TTL(strKey, nResult));
    BOOST_CHECK_LE(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->PTTL(strKey, nResult));
    BOOST_CHECK_LE(nResult, 1000);

    BOOST_CHECK(pFlyRedisClient->GET(strKey, strResult));
    BOOST_CHECK_EQUAL(strResult, strValue);

    boost::this_thread::sleep_for(boost::chrono::milliseconds(1000));
    BOOST_CHECK(pFlyRedisClient->GET(strKey, strResult));
    BOOST_CHECK_EQUAL(strResult, "");

    DESTROY_REDIS_CLIENT();
}

BOOST_AUTO_TEST_CASE(KEY_STRING_BIT)
{
    CREATE_REDIS_CLIENT();
    std::string strKey = "key_bit_" + std::to_string(time(nullptr));
    std::string strValue = std::to_string(time(nullptr));
    int nResult = 0;
    std::string strResult;

    BOOST_CHECK(pFlyRedisClient->BITCOUNT(strKey, 0, 1, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->SET(strKey, strValue));
    BOOST_CHECK(pFlyRedisClient->BITCOUNT(strKey, 0, 1, nResult));
    BOOST_CHECK_GT(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->SET(strKey, strValue));
    BOOST_CHECK(pFlyRedisClient->BITOP_XOR(strKey, strKey, nResult));

    BOOST_CHECK(pFlyRedisClient->SET(strKey, strValue));
    BOOST_CHECK(pFlyRedisClient->BITOP_AND(strKey, strKey, nResult));

    BOOST_CHECK(pFlyRedisClient->SET(strKey, strValue));
    BOOST_CHECK(pFlyRedisClient->BITOP_OR(strKey, strKey, nResult));

    BOOST_CHECK(pFlyRedisClient->SET(strKey, strValue));
    BOOST_CHECK(pFlyRedisClient->BITOP_NOT(strKey, strKey, nResult));

    BOOST_CHECK(pFlyRedisClient->BITPOS(strKey, 1, nResult));
    BOOST_CHECK(pFlyRedisClient->BITPOS(strKey, 1, 1, 2, nResult));

    BOOST_CHECK(pFlyRedisClient->GETRANGE(strKey, 1, 2, strResult));
    BOOST_CHECK(pFlyRedisClient->SETRANGE(strKey, 1, strValue, nResult));

    DESTROY_REDIS_CLIENT();
}

BOOST_AUTO_TEST_CASE(KEY_STRING_MULTI)
{
    CREATE_REDIS_CLIENT();
    time_t nCurTime = time(nullptr);
    std::string strKey1 = "{key" + std::to_string(nCurTime) + "}";
    std::string strKey2 = "{key" + std::to_string(nCurTime) + "}";
    std::string strValue = std::to_string(time(nullptr));
    int nResult = 0;
    std::vector<std::string> vecResult;
    std::map<std::string, std::string> mapKVP;
    mapKVP.insert(std::make_pair(strKey1, strValue));
    mapKVP.insert(std::make_pair(strKey2, strValue));

    std::vector<std::string> vecMultiKey;
    vecMultiKey.push_back(strKey1);
    vecMultiKey.push_back(strKey2);

    BOOST_CHECK(pFlyRedisClient->MGET(vecMultiKey, vecResult));
    BOOST_CHECK_EQUAL(vecResult.size(), vecMultiKey.size());

    BOOST_CHECK(pFlyRedisClient->MSETNX(mapKVP, nResult));
    BOOST_CHECK(pFlyRedisClient->MSET(mapKVP));
    BOOST_CHECK(pFlyRedisClient->MSETNX(mapKVP, nResult));

    BOOST_CHECK(pFlyRedisClient->MGET(vecMultiKey, vecResult));
    DESTROY_REDIS_CLIENT();
}

BOOST_AUTO_TEST_CASE(KEY_STRING_INT)
{
    CREATE_REDIS_CLIENT();
    std::string strKey = "key_int_" + std::to_string(time(nullptr));
    std::string strValue = std::to_string(time(nullptr));
    int nResult = 0;
    double fResult = 0.0f;

    BOOST_CHECK(pFlyRedisClient->DECR(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, -1);

    BOOST_CHECK(pFlyRedisClient->DECRBY(strKey, 10, nResult));
    BOOST_CHECK_EQUAL(nResult, -11);

    BOOST_CHECK(pFlyRedisClient->INCR(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, -10);

    BOOST_CHECK(pFlyRedisClient->INCRBY(strKey, 11, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->INCRBYFLOAT(strKey, 0.1, fResult));
    BOOST_CHECK_EQUAL(fResult, 1.1);

    DESTROY_REDIS_CLIENT();
}

BOOST_AUTO_TEST_CASE(KEY_STRING_ERASE)
{
    CREATE_REDIS_CLIENT();
    std::string strKey = "key_erase_" + std::to_string(time(nullptr));
    std::string strValue = std::to_string(time(nullptr));
    int nResult = 0;
    std::string strResult;

    BOOST_CHECK(pFlyRedisClient->SET(strKey, strValue));
    BOOST_CHECK(pFlyRedisClient->DUMP(strKey, strResult));

    BOOST_CHECK(pFlyRedisClient->EXISTS(strKey, nResult));
    BOOST_CHECK_EQUAL(1, nResult);
    
    BOOST_CHECK(pFlyRedisClient->DEL(strKey, nResult));

    BOOST_CHECK(pFlyRedisClient->EXPIRE(strKey, 10, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);
    BOOST_CHECK(pFlyRedisClient->SET(strKey, strValue));
    BOOST_CHECK(pFlyRedisClient->EXPIRE(strKey, 10, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);
    BOOST_CHECK(pFlyRedisClient->TTL(strKey, nResult));
    BOOST_CHECK_LE(nResult, 10);
    BOOST_CHECK(pFlyRedisClient->PTTL(strKey, nResult));
    BOOST_CHECK_LE(nResult, 10 * 1000);

    BOOST_CHECK(pFlyRedisClient->SET(strKey, strValue));
    BOOST_CHECK(pFlyRedisClient->EXPIREAT(strKey, static_cast<int>(time(nullptr) + 60), nResult));
    BOOST_CHECK_EQUAL(nResult, 1);
    BOOST_CHECK(pFlyRedisClient->TTL(strKey, nResult));
    BOOST_CHECK_GE(nResult, 60);
    BOOST_CHECK(pFlyRedisClient->PTTL(strKey, nResult));
    BOOST_CHECK_GE(nResult, 60 * 1000);

    BOOST_CHECK(pFlyRedisClient->PEXPIRE(strKey, 10000, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);
    BOOST_CHECK(pFlyRedisClient->TTL(strKey, nResult));
    BOOST_CHECK_LE(nResult, 10);
    BOOST_CHECK(pFlyRedisClient->PTTL(strKey, nResult));
    BOOST_CHECK_LE(nResult, 10 * 1000);

    BOOST_CHECK(pFlyRedisClient->PEXPIREAT(strKey, static_cast<int>((time(nullptr) + 60) * 1000), nResult));
    BOOST_CHECK_EQUAL(nResult, 1);
    BOOST_CHECK(pFlyRedisClient->TTL(strKey, nResult));
    BOOST_CHECK_LE(nResult, 60);
    BOOST_CHECK(pFlyRedisClient->PTTL(strKey, nResult));
    BOOST_CHECK_LE(nResult, 60 * 1000);
    
    BOOST_CHECK(pFlyRedisClient->SET(strKey, strValue));
    BOOST_CHECK(pFlyRedisClient->UNLINK(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->TOUCH(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    std::string strFromKey = "{key_erase}_from_" + std::to_string(time(nullptr));
    std::string strToKey = "{key_erase}_to_" + std::to_string(time(nullptr));
    BOOST_CHECK(pFlyRedisClient->SET(strFromKey, strValue));
    BOOST_CHECK(pFlyRedisClient->RENAME(strFromKey, strToKey, strResult));
    BOOST_CHECK(pFlyRedisClient->RENAMENX(strToKey, strFromKey, strResult));
    DESTROY_REDIS_CLIENT();
}


BOOST_AUTO_TEST_CASE(KEY_HASH)
{
    CREATE_REDIS_CLIENT();
    std::string strKey = "key_hash_" + std::to_string(time(nullptr));
    std::string strField1 = "field1_" + std::to_string(time(nullptr));
    std::string strField2 = "field2_" + std::to_string(time(nullptr));
    std::string strValue = std::to_string(time(nullptr));
    int nResult = 0;
    double fResult = 0.0f;
    std::string strResult;
    std::vector<std::string> vecResult;
    std::map<std::string, std::string> mapResult;
    
    BOOST_CHECK(pFlyRedisClient->HEXISTS(strKey, strField1, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->HSET(strKey, strField1, strValue + "first", nResult));
    BOOST_CHECK_EQUAL(nResult, 1);
    BOOST_CHECK(pFlyRedisClient->HSET(strKey, strField1, strValue, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);
    BOOST_CHECK(pFlyRedisClient->HSET(strKey, strField2, strValue, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->HGET(strKey, strField1, strResult));
    BOOST_CHECK_EQUAL(strResult, strValue);

    BOOST_CHECK(pFlyRedisClient->HGETALL(strKey, mapResult));
    BOOST_CHECK_EQUAL(mapResult.size(), 2);

    BOOST_CHECK(pFlyRedisClient->HINCRBY(strKey, strField1, 1, nResult));
    BOOST_CHECK_EQUAL(nResult, atoi(strValue.c_str()) + 1);

    BOOST_CHECK(pFlyRedisClient->HINCRBYFLOAT(strKey, strField1, 0.1, fResult));
    BOOST_CHECK_EQUAL(fResult, atoi(strValue.c_str()) + 1.1);
    BOOST_CHECK(pFlyRedisClient->HSET(strKey, strField1, strValue, nResult));

    BOOST_CHECK(pFlyRedisClient->HKEYS(strKey, vecResult));
    BOOST_CHECK_EQUAL(vecResult.size(), 2);

    BOOST_CHECK(pFlyRedisClient->HLEN(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 2);

    BOOST_CHECK(pFlyRedisClient->HMGET(strKey, strField1, strResult));
    BOOST_CHECK_EQUAL(strResult, strValue);

    BOOST_CHECK(pFlyRedisClient->HMSET(strKey, strField1, strValue + "-" + strValue, strResult));
    BOOST_CHECK_EQUAL(strResult, "OK");

    BOOST_CHECK(pFlyRedisClient->HMSET(strKey, mapResult, strResult));
    BOOST_CHECK_EQUAL(strResult, "OK");

    BOOST_CHECK(pFlyRedisClient->HSET(strKey, strField1, strValue + "-" + strValue, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->HSETNX(strKey, strField1 + strField2, strValue + "-" + strValue, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->HSTRLEN(strKey, strField1, nResult));
    BOOST_CHECK_EQUAL(nResult, 21);

    BOOST_CHECK(pFlyRedisClient->HVALS(strKey, vecResult));
    BOOST_CHECK_EQUAL(vecResult.size(), 3);

    BOOST_CHECK(pFlyRedisClient->HEXISTS(strKey, strField1, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->HDEL(strKey, strField1, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->HDEL(strKey, strField1, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    DESTROY_REDIS_CLIENT();
}

BOOST_AUTO_TEST_CASE(KEY_ZSET)
{
    CREATE_REDIS_CLIENT();
    std::string strKey = "key_zset_" + std::to_string(time(nullptr));
    std::string strMember1 = "member1_" + std::to_string(time(nullptr));
    std::string strMember2 = "member2_" + std::to_string(time(nullptr));
    int nResult = 0;
    double fResult = 0.0f;
    std::string strResult;
    std::vector<std::string> vecResult;
    std::map<std::string, std::string> mapResult;
    std::vector<std::pair<std::string, double> > vecPairResult;

    BOOST_CHECK(pFlyRedisClient->ZADD(strKey, 1, strMember1, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->ZADD(strKey, 2, strMember2, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->ZSCORE(strKey, strMember1, fResult));
    BOOST_CHECK_EQUAL(fResult, 1);

    BOOST_CHECK(pFlyRedisClient->ZCARD(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 2);

    BOOST_CHECK(pFlyRedisClient->ZRANGE(strKey, 0, -1, vecResult));
    BOOST_CHECK_EQUAL(vecResult.size(), 2);

    BOOST_CHECK(pFlyRedisClient->ZRANGE_WITHSCORES(strKey, 0, -1, vecPairResult));
    BOOST_CHECK_EQUAL(vecResult.size(), 2);

    BOOST_CHECK(pFlyRedisClient->ZCOUNT(strKey, "0", "-1", nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->ZINCRBY(strKey, 1, strMember1, strResult));
    BOOST_CHECK_EQUAL(strResult, "2");

    BOOST_CHECK(pFlyRedisClient->ZRANK(strKey, strMember1, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->ZREM(strKey, strMember1, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->ZREMRANGEBYSCORE(strKey, -1, 1, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->ZREVRANGE(strKey, 0, -1, vecResult));
    BOOST_CHECK_EQUAL(vecResult.size(), 1);

    BOOST_CHECK(pFlyRedisClient->ZREVRANGE_WITHSCORES(strKey, 0, -1, vecPairResult));
    BOOST_CHECK_EQUAL(vecResult.size(), 1);

    DESTROY_REDIS_CLIENT();
}

BOOST_AUTO_TEST_CASE(KEY_LIST)
{
    CREATE_REDIS_CLIENT();
    std::string strKey = "key_list_" + std::to_string(time(nullptr));
    std::string strKeyDst = "key_dst_list_" + std::to_string(time(nullptr));
    std::string strValue1 = "value1_" + std::to_string(time(nullptr));
    std::string strValue2 = "value2_" + std::to_string(time(nullptr));
    std::string strValue3 = "value3_" + std::to_string(time(nullptr));
    int nResult = 0;
    std::string strResult;
    std::vector<std::string> vecResult;

    BOOST_CHECK(pFlyRedisClient->LLEN(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->LPOP(strKey, strResult));
    BOOST_CHECK_EQUAL(strResult, "");

    BOOST_CHECK(pFlyRedisClient->RPOP(strKey, strResult));
    BOOST_CHECK_EQUAL(strResult, "");

    BOOST_CHECK(pFlyRedisClient->LPUSHX(strKey, strValue1, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->RPUSHX(strKey, strValue1, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->LPUSH(strKey, strValue1, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->LPUSHX(strKey, strValue1, nResult));
    BOOST_CHECK_EQUAL(nResult, 2);

    BOOST_CHECK(pFlyRedisClient->LPUSH(strKey, strValue1, nResult));
    BOOST_CHECK_EQUAL(nResult, 3);

    BOOST_CHECK(pFlyRedisClient->LPUSH(strKey, strValue2, nResult));
    BOOST_CHECK_EQUAL(nResult, 4);

    BOOST_CHECK(pFlyRedisClient->LLEN(strKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 4);

    BOOST_CHECK(pFlyRedisClient->RPUSH(strKey, strValue1, nResult));
    BOOST_CHECK_EQUAL(nResult, 5);

    BOOST_CHECK(pFlyRedisClient->RPUSH(strKey, strValue2, nResult));
    BOOST_CHECK_EQUAL(nResult, 6);

    BOOST_CHECK(pFlyRedisClient->RPUSHX(strKey, strValue2, nResult));
    BOOST_CHECK_EQUAL(nResult, 7);

    BOOST_CHECK(pFlyRedisClient->BLPOP(strKey, 1, vecResult));
    BOOST_CHECK_EQUAL(vecResult.size(), 2);

    BOOST_CHECK(pFlyRedisClient->BRPOP(strKey, 1, vecResult));
    BOOST_CHECK_EQUAL(vecResult.size(), 2);

    BOOST_CHECK(pFlyRedisClient->LPOP(strKey, strResult));
    BOOST_CHECK_EQUAL(strResult, strValue1);

    BOOST_CHECK(pFlyRedisClient->RPOP(strKey, strResult));
    BOOST_CHECK_EQUAL(strResult, strValue2);

    BOOST_CHECK(pFlyRedisClient->BRPOPLPUSH(strKey, strKey, 1, strResult));
    BOOST_CHECK_EQUAL(strResult, strValue1);

    BOOST_CHECK(pFlyRedisClient->LINDEX(strKey, 1, strResult));
    BOOST_CHECK_EQUAL(strResult, strValue1);

    BOOST_CHECK(pFlyRedisClient->LINSERT_BEFORE(strKey, strValue1, strValue3, nResult));
    BOOST_CHECK_EQUAL(nResult, 4);

    BOOST_CHECK(pFlyRedisClient->LINSERT_AFTER(strKey, strValue1, strValue3, nResult));
    BOOST_CHECK_EQUAL(nResult, 5);

    BOOST_CHECK(pFlyRedisClient->RPOPLPUSH(strKey, strKey, vecResult));
    BOOST_CHECK_EQUAL(vecResult.size(), 1);

    BOOST_CHECK(pFlyRedisClient->LRANGE(strKey, 0, 100, vecResult));
    BOOST_CHECK_EQUAL(vecResult.size(), 5);

    BOOST_CHECK(pFlyRedisClient->LREM(strKey, -1, strValue1, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->LSET(strKey, -1, strValue3, strResult));
    BOOST_CHECK_EQUAL(strResult, "OK");

    BOOST_CHECK(pFlyRedisClient->LTRIM(strKey, 0, -1, strResult));
    BOOST_CHECK_EQUAL(strResult, "OK");

    DESTROY_REDIS_CLIENT();
}

BOOST_AUTO_TEST_CASE(KEY_SET)
{
    CREATE_REDIS_CLIENT();
    std::string strKey1 = "{key}set1_" + std::to_string(time(nullptr));
    std::string strKey2 = "{key}set2_" + std::to_string(time(nullptr));
    std::string strKey3 = "{key}set3_" + std::to_string(time(nullptr));
    std::string strMember1 = "member1_" + std::to_string(time(nullptr));
    std::string strMember2 = "member2_" + std::to_string(time(nullptr));
    std::string strMember3 = "member3_" + std::to_string(time(nullptr));
    std::string strMember4 = "member4_" + std::to_string(time(nullptr));
    std::string strMember5 = "member5_" + std::to_string(time(nullptr));
    int nResult = 0;
    std::string strResult;
    std::vector<std::string> vecResult;
    std::set<std::string> setResult;

    BOOST_CHECK(pFlyRedisClient->DEL(strKey1, nResult));
    BOOST_CHECK(pFlyRedisClient->DEL(strKey2, nResult));
    BOOST_CHECK(pFlyRedisClient->DEL(strKey3, nResult));

    BOOST_CHECK(pFlyRedisClient->SISMEMBER(strKey1, strMember1, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->SADD(strKey1, strMember1, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->SISMEMBER(strKey1, strMember1, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->SCARD(strKey1, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->SADD(strKey1, strMember1, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->SADD(strKey1, strMember2, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->SCARD(strKey1, nResult));
    BOOST_CHECK_EQUAL(nResult, 2);

    BOOST_CHECK(pFlyRedisClient->SMEMBERS(strKey1, setResult));
    BOOST_CHECK_EQUAL(setResult.size(), 2);

    BOOST_CHECK(pFlyRedisClient->DEL(strKey1, nResult));
    BOOST_CHECK(pFlyRedisClient->DEL(strKey2, nResult));
    BOOST_CHECK(pFlyRedisClient->DEL(strKey3, nResult));
    BOOST_CHECK(pFlyRedisClient->SADD(strKey1, strMember1, nResult));
    BOOST_CHECK(pFlyRedisClient->SADD(strKey1, strMember2, nResult));
    BOOST_CHECK(pFlyRedisClient->SADD(strKey1, strMember3, nResult));
    BOOST_CHECK(pFlyRedisClient->SADD(strKey2, strMember1, nResult));
    BOOST_CHECK(pFlyRedisClient->SADD(strKey2, strMember2, nResult));
    BOOST_CHECK(pFlyRedisClient->SADD(strKey2, strMember4, nResult));

    std::vector<std::string> vecSrcKey;
    vecSrcKey.push_back(strKey1);
    vecSrcKey.push_back(strKey2);
    BOOST_CHECK(pFlyRedisClient->SDIFF(strKey1, strKey2, vecResult));
    BOOST_CHECK(pFlyRedisClient->SDIFF(vecSrcKey, vecResult));
    BOOST_CHECK(pFlyRedisClient->SDIFFSTORE(strKey3, vecSrcKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->SINTER(strKey1, strKey2, vecResult));
    BOOST_CHECK(pFlyRedisClient->SINTER(vecSrcKey, vecResult));
    BOOST_CHECK(pFlyRedisClient->DEL(strKey3, nResult));
    BOOST_CHECK(pFlyRedisClient->SINTERSTORE(strKey3, vecSrcKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 2);

    BOOST_CHECK(pFlyRedisClient->DEL(strKey3, nResult));
    BOOST_CHECK(pFlyRedisClient->SUNION(vecSrcKey, vecResult));
    BOOST_CHECK(pFlyRedisClient->SUNION(vecSrcKey, vecResult));
    BOOST_CHECK(pFlyRedisClient->DEL(strKey3, nResult));
    BOOST_CHECK(pFlyRedisClient->SUNIONSTORE(strKey3, vecSrcKey, nResult));
    BOOST_CHECK_EQUAL(nResult, 4);

    BOOST_CHECK(pFlyRedisClient->SMOVE(strKey1, strKey2, strMember1, nResult));
    BOOST_CHECK_EQUAL(nResult, 1);

    BOOST_CHECK(pFlyRedisClient->SMOVE(strKey1, strKey2, strMember1, nResult));
    BOOST_CHECK_EQUAL(nResult, 0);

    BOOST_CHECK(pFlyRedisClient->SPOP(strKey3, 1, vecResult));
    BOOST_CHECK_EQUAL(vecResult.size(), 1);

    BOOST_CHECK(pFlyRedisClient->SRANDMEMBER(strKey3, 1, vecResult));
    BOOST_CHECK_EQUAL(vecResult.size(), 1);

    BOOST_CHECK(pFlyRedisClient->SREM(strKey3, strMember1, nResult));

    DESTROY_REDIS_CLIENT();
}

BOOST_AUTO_TEST_CASE(SCRIPT)
{
    CREATE_REDIS_CLIENT();
    std::string strResult;
    std::string strLuaScript = "return math.random(1000)";
    std::string strSHA;
    BOOST_CHECK(pFlyRedisClient->SCRIPT_LOAD(strLuaScript, strSHA));
    BOOST_CHECK(pFlyRedisClient->EVALSHA(strSHA, "a", strResult));
    BOOST_CHECK(pFlyRedisClient->EVALSHA(strSHA, "b", strResult));
    BOOST_CHECK(pFlyRedisClient->EVALSHA(strSHA, "c", strResult));
    BOOST_CHECK(pFlyRedisClient->EVAL(strLuaScript, "a", strResult));
    BOOST_CHECK(pFlyRedisClient->EVAL(strLuaScript, "b", strResult));
    BOOST_CHECK(pFlyRedisClient->EVAL(strLuaScript, "c", strResult));
    BOOST_CHECK(pFlyRedisClient->SCRIPT_EXISTS(strSHA));
    BOOST_CHECK(pFlyRedisClient->SCRIPT_FLUSH());
    BOOST_CHECK(!pFlyRedisClient->SCRIPT_EXISTS(strSHA + strSHA));
    DESTROY_REDIS_CLIENT();
}

BOOST_AUTO_TEST_CASE(ACL_CMD)
{
    CREATE_REDIS_CLIENT();
    std::vector<std::string> vecResult;
    BOOST_CHECK(pFlyRedisClient->ACL_CAT(vecResult));
    BOOST_CHECK(!vecResult.empty());
    std::vector<std::string> vecSubResult;
    for (auto& strCat : vecResult)
    {
        BOOST_CHECK(pFlyRedisClient->ACL_CAT(strCat, vecSubResult));
        BOOST_CHECK(!vecSubResult.empty());
    }
    BOOST_CHECK(pFlyRedisClient->ACL_HELP(vecResult));
    BOOST_CHECK(!vecResult.empty());

    BOOST_CHECK(pFlyRedisClient->ACL_LIST(vecResult));
    BOOST_CHECK(!vecResult.empty());

    BOOST_CHECK(pFlyRedisClient->ACL_GETUSER("default", vecResult));
    BOOST_CHECK(!vecResult.empty());

    std::string strResult;
    BOOST_CHECK(pFlyRedisClient->ACL_WHOAMI(strResult));
    BOOST_CHECK(0 == strResult.compare("default"));

    BOOST_CHECK(pFlyRedisClient->ACL_GENPASS(strResult));
    BOOST_CHECK(!strResult.empty());

    BOOST_CHECK(pFlyRedisClient->ACL_GENPASS(16, strResult));
    BOOST_CHECK(!strResult.empty());

    BOOST_CHECK(pFlyRedisClient->ACL_SETUSER("icerlion", "on allkeys +set", strResult));
    BOOST_CHECK(!strResult.empty());

    BOOST_CHECK(pFlyRedisClient->ACL_SETUSER("icerlion", "reset", strResult));
    BOOST_CHECK(!strResult.empty());

    BOOST_CHECK(pFlyRedisClient->ACL_SETUSER("icerlion", "+get", strResult));
    BOOST_CHECK(!strResult.empty());

    BOOST_CHECK(pFlyRedisClient->ACL_SETUSER("icerlion", "allkeys +@string +@set -SADD >password", strResult));
    BOOST_CHECK(!strResult.empty());

    BOOST_CHECK(pFlyRedisClient->ACL_USERS(vecResult));
    BOOST_CHECK(!vecResult.empty());

    int nResult = 0;
    BOOST_CHECK(pFlyRedisClient->ACL_DELUSER("icerlion", nResult));
    BOOST_CHECK(1 == nResult);

    BOOST_CHECK(pFlyRedisClient->ACL_LOG(vecResult));

    BOOST_CHECK(pFlyRedisClient->ACL_SAVE());

    BOOST_CHECK(pFlyRedisClient->ACL_LOAD());

    DESTROY_REDIS_CLIENT();
}