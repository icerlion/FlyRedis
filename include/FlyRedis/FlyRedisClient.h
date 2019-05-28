/*+==================================================================
* Copyright (C) 2019 FlyRedis. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* FileName: FlyRedisClient.h
*
* Purpose:  Define FlyRedisClient
*
* Author:   Jhon Frank(icerlion@163.com)
*
* Modify:   2019/5/23 15:24
===================================================================+*/
#ifndef _FLYREDISCLIENT_H_
#define _FLYREDISCLIENT_H_
#include "FlyRedisSession.h"
#include <set>

class CFlyRedisClient
{
public:
    // Constructor
    CFlyRedisClient();

    // Destructor
    ~CFlyRedisClient();

    // Set redis config, address as 127.0.0.1:6789
    void SetRedisConfig(const std::string& strRedisAddress, const std::string& strPassword);

    // Open this client
    bool Open();

    // Close this client
    void Close();

    //////////////////////////////////////////////////////////////////////////
    /// Begin of RedisCmd
    bool SCRIPT_LOAD(const std::string& strScript, std::string& strResult);

    bool APPEND(const std::string& strKey, const std::string& strValue, int& nResult);
    bool BITCOUNT(const std::string& strKey, int nStart, int nEnd, int& nResult);
    bool BITCOUNT(const std::string& strKey, int& nResult);
    bool BITOP_AND(const std::string& strDestKey, const std::string& strSrcKey, int& nResult);
    bool BITOP_OR(const std::string& strDestKey, const std::string& strSrcKey, int& nResult);
    bool BITOP_XOR(const std::string& strDestKey, const std::string& strSrcKey, int& nResult);
    bool BITOP_NOT(const std::string& strDestKey, const std::string& strSrcKey, int& nResult);
    bool BITPOS(const std::string& strKey, int nBit, int& nResult);
    bool BITPOS(const std::string& strKey, int nBit, int nStart, int nEnd, int& nResult);
    bool DECR(const std::string& strKey, int& nResult);
    bool DECRBY(const std::string& strKey, int nDecrement, int& nResult);
    bool GET(const std::string& strKey, std::string& strResult);
    bool GETBIT(const std::string& strKey, int nOffset, int& nResult);
    bool GETRANGE(const std::string& strKey, int nStart, int nEnd, std::string& strResult);
    bool GETSET(const std::string& strKey, const std::string& strValue, std::string& strResult);
    bool INCR(const std::string& strKey, int& nResult);
    bool INCRBY(const std::string& strKey, int nIncrement, int& nResult);
    bool INCRBYFLOAT(const std::string& strKey, double fIncrement, double& fResult);
    bool MGET(const std::vector<std::string>& vecKey, std::vector<std::string>& vecResult);
    bool MSET(const std::map<std::string, std::string>& mapKeyValue);
    bool MSETNX(const std::map<std::string, std::string>& mapKeyValue, int& nResult);
    bool PSETEX(const std::string& strKey, int nTimeOutMS, const std::string& strValue, std::string& strResult);
    bool SET(const std::string& strKey, const std::string& strValue);
    bool SETBIT(const std::string& strKey, int nOffset, int nValue, int& nResult);
    bool SETEX(const std::string& strKey, int nTimeOutSeconds, const std::string& strValue, std::string& strResult);
    bool SETNX(const std::string& strKey, const std::string& strValue, int& nResult);
    bool SETRANGE(const std::string& strKey, int nOffset, const std::string& strValue, int& nResult);
    bool STRLEN(const std::string& strKey, int& nValue);

    bool DEL(const std::string& strKey, int& nResult);
    bool DUMP(const std::string& strKey, std::string& strResult);
    bool EXISTS(const std::string& strKey, int& nResult);
    bool EXPIRE(const std::string& strKey, int nSeconds, int& nResult);
    bool EXPIREAT(const std::string& strKey, int nTimestamp, int& nResult);
    bool PERSIST(const std::string& strKey, int& nResult);
    bool PEXPIRE(const std::string& strKey, int nMS, int& nResult);
    bool PEXPIREAT(const std::string& strKey, int nMS, int& nResult);
    bool PTTL(const std::string& strKey, int& nResult);
    bool RENAME(const std::string& strFromKey, const std::string& strToKey, std::string& strResult);
    bool RENAMENX(const std::string& strFromKey, const std::string& strToKey, std::string& strResult);
    bool TOUCH(const std::string& strKey, int& nResult);
    bool TTL(const std::string& strKey, int& nResult);
    bool TYPE(const std::string& strKey, std::string& strResult);
    bool UNLINK(const std::string& strKey, int& nResult);

    bool HDEL(const std::string& strKey, const std::string& strField, int& nResult);
    bool HEXISTS(const std::string& strKey, const std::string& strField, int& nResult);
    bool HGET(const std::string& strKey, const std::string& strField, std::string& strResult);
    bool HGETALL(const std::string& strKey, std::map<std::string, std::string>& mapFieldValue);
    bool HINCRBY(const std::string& strKey, const std::string& strField, int nIncVal, int& nResult);
    bool HINCRBYFLOAT(const std::string& strKey, const std::string& strField, double fIncVal, double& fResult);
    bool HKEYS(const std::string& strKey, std::vector<std::string>& vecResult);
    bool HLEN(const std::string& strKey, int& nResult);
    bool HMGET(const std::string& strKey, const std::string& strField, std::string& strValue);
    bool HMGET(const std::string& strKey, const std::vector<std::string>& vecField, std::vector<std::string>& vecOutput);
    bool HMSET(const std::string& strKey, const std::map<std::string, std::string>& mapFieldValue, std::string& strResult);
    bool HMSET(const std::string& strKey, const std::string& strField, const std::string& strValue, std::string& strResult);
    bool HSET(const std::string& strKey, const std::string& strField, const std::string& strValue, int& nResult);
    bool HSETNX(const std::string& strKey, const std::string& strField, const std::string& strValue, int& nResult);
    bool HSTRLEN(const std::string& strKey, const std::string& strField, int& nResult);
    bool HVALS(const std::string& strKey, std::vector<std::string>& vecResult);

    bool ZADD(const std::string& strKey, double fScore, const std::string& strMember, int& nResult);
    bool ZCARD(const std::string& strKey, int& nResult);
    bool ZCOUNT(const std::string& strKey, const std::string& strMin, const std::string& strMax, int& nResult);
    bool ZINCRBY(const std::string& strKey, double fIncrement, const std::string& strMember, std::string& strResult);
    bool ZRANGE(const std::string& strKey, int nStart, int nStop, std::vector<std::string>& vecResult);
    bool ZRANGE_WITHSCORES(const std::string& strKey, int nStart, int nStop, std::vector<std::pair<std::string, double> >& vecResult);
    bool ZRANK(const std::string& strKey, const std::string& strMember, int& nResult);
    bool ZREM(const std::string& strKey, const std::string& strMember, int& nResult);
    bool ZREMRANGEBYSCORE(const std::string& strKey, double fFromScore, double fToScore, int& nResult);
    bool ZREVRANGE(const std::string& strKey, int nStart, int nStop, std::vector<std::string>& vecResult);
    bool ZREVRANGE_WITHSCORES(const std::string& strKey, int nStart, int nStop, std::vector<std::pair<std::string, double> >& vecResult);
    bool ZSCORE(const std::string& strKey, const std::string& strMember, double& fResult);

    bool BLPOP(const std::string& strKey, int nTimeout, std::vector<std::string>& vecResult);
    bool BRPOP(const std::string& strKey, int nTimeout, std::vector<std::string>& vecResult);
    bool BRPOPLPUSH(const std::string& strSrcKey, const std::string& strDstKey, int nTimeout, std::string& strResult);
    bool LINDEX(const std::string& strKey, int nIndex, std::string& strResult);
    bool LINSERT_BEFORE(const std::string& strKey, const std::string& strPivot, const std::string& strValue, int& nResult);
    bool LINSERT_AFTER(const std::string& strKey, const std::string& strPivot, const std::string& strValue, int& nResult);
    bool LLEN(const std::string& strKey, int& nResult);
    bool LPOP(const std::string& strKey, std::string& strResult);
    bool LPUSH(const std::string& strKey, const std::string& strValue, int& nResult);
    bool LPUSHX(const std::string& strKey, const std::string& strValue, int& nResult);
    bool LRANGE(const std::string& strKey, int nStart, int nStop, std::vector<std::string>& vecResult);
    bool LREM(const std::string& strKey, int nCount, const std::string& strValue, int& nResult);
    bool LSET(const std::string& strKey, int nIndex, const std::string& strValue, std::string& strResult);
    bool LTRIM(const std::string& strKey, int nStart, int nStop, std::string& strResult);
    bool RPOP(const std::string& strKey, std::string& strResult);
    bool RPOPLPUSH(const std::string& strSrcKey, const std::string& strDestKey, std::vector<std::string>& vecResult);
    bool RPUSH(const std::string& strKey, const std::string& strValue, int& nResult);
    bool RPUSHX(const std::string& strKey, const std::string& strValue, int& nResult);

    bool SADD(const std::string& strKey, const std::string& strValue, int& nResult);
    bool SCARD(const std::string& strKey, int& nResult);
    bool SDIFF(const std::vector<std::string>& vecKey, std::vector<std::string>& vecResult);
    bool SDIFF(const std::string& strFirstKey, const std::string& strSecondKey, std::vector<std::string>& vecResult);
    bool SDIFFSTORE(const std::string& strDestKey, const std::vector<std::string>& vecSrcKey, int& nResult);
    bool SINTER(const std::vector<std::string>& vecKey, std::vector<std::string>& vecResult);
    bool SINTER(const std::string& strFirstKey, const std::string& strSecondKey, std::vector<std::string>& vecResult);
    bool SINTERSTORE(const std::string& strDestKey, const std::vector<std::string>& vecSrcKey, int& nResult);
    bool SISMEMBER(const std::string& strKey, const std::string& strMember, int& nResult);
    bool SMEMBERS(const std::string& strKey, std::vector<std::string>& vecResult);
    bool SMOVE(const std::string& strSrcKey, const std::string& strDestKey, const std::string& strMember, int& nResult);
    bool SPOP(const std::string& strKey, int nCount, std::vector<std::string>& vecResult);
    bool SRANDMEMBER(const std::string& strKey, int nCount, std::vector<std::string>& vecResult);
    bool SREM(const std::string& strKey, const std::string& strValue, int& nResult);
    bool SUNION(const std::vector<std::string>& vecSrcKey, std::vector<std::string>& vecResult);
    bool SUNIONSTORE(const std::string& strDestKey, const std::vector<std::string>& vecSrcKey, int& nResult);
    /// End of RedisCmd
    //////////////////////////////////////////////////////////////////////////

private:
    bool VerifyRedisSessionList();

    bool ResolveRedisSession(const std::string& strKey);

    bool ConnectToEveryMasterRedisNode();

    CFlyRedisSession* CreateRedisSession(const std::string& strRedisAddress);

    void DestroyRedisSession(const std::string& strIPPort);
    void DestroyRedisSession(CFlyRedisSession* pRedisSession);

    void PingEveryRedisNode(std::vector<CFlyRedisSession*>& vecDeadRedisSession);

    bool DeliverRedisCmd(const std::string& strKey, const char* pszCaller);
    bool RunRedisCmdOnOneLineResponseInt(const std::string& strKey, int& nResult, const char* pszCaller);
    bool RunRedisCmdOnOneLineResponseDouble(const std::string& strKey, double& fResult, const char* pszCaller);
    bool RunRedisCmdOnOneLineResponseString(const std::string& strKey, std::string& strResult, const char* pszCaller);
    bool RunRedisCmdOnOneLineResponseVector(const std::string& strKey, std::vector<std::string>& vecResult, const char* pszCaller);
    bool RunRedisCmdOnResponseKVP(const std::string& strKey, std::map<std::string, std::string>& mapResult, const char* pszCaller);
    bool RunRedisCmdOnResponsePairList(const std::string& strKey, std::vector< std::pair<std::string, std::string> >& vecResult, const char* pszCaller);

    void ClearRedisCmdCache();

private:
    std::set<std::string> m_setRedisAddressSeed;
    std::string m_strRedisPasswod;
    bool m_bClusterFlag;
    CFlyRedisSession* m_pCurRedisSession;
    // Key: redis address, ip:port
    // Value: redis session
    std::map<std::string, CFlyRedisSession*> m_mapRedisSession;
    int m_nMasterNodeCount;
    // Flag of need verify redis session list
    bool m_bHasBadRedisSession;
    //////////////////////////////////////////////////////////////////////////
    // Redis Request 
    std::vector<std::string> m_vecRedisCmdParamList;
    std::string m_strRedisCmdRequest;
    std::vector<std::string> m_vecRedisResponseLine;
};

#endif // _FLYREDISCLIENT_H_
