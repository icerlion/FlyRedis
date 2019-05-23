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

    bool EXISTS(const std::string& strKey, int& nResult);
    bool GET(const std::string& strKey, std::string& strResult);
    bool SET(const std::string& strKey, const std::string& strValue, std::string& strResult);
    bool DEL(const std::string& strKey, int& nResult);
    bool SETEX(const std::string& strKey, int nTimeOutSeconds, const std::string& strValue, std::string& strResult);
    bool INCR(const std::string& strKey, int& nResult);

    bool HSET(const std::string& strKey, const std::string& strField, const std::string& strValue, int& nResult);
    bool HSETNX(const std::string& strKey, const std::string& strField, const std::string& strValue, int& nResult);
    bool HMSET(const std::string& strKey, const std::map<std::string, std::string>& mapFieldValue, std::string& strResult);
    bool HMSET(const std::string& strKey, const std::string& strField, const std::string& strValue, std::string& strResult);
    bool HGETALL(const std::string& strKey, std::map<std::string, std::string>& mapFieldValue);
    bool HDEL(const std::string& strKey, const std::string& strField, int& nResult);
    bool HGET(const std::string& strKey, const std::string& strField, std::string& strResult);
    bool HMGET(const std::string& strKey, const std::string& strField, std::string& strValue);
    bool HMGET(const std::string& strKey, const std::vector<std::string>& vField, std::vector<std::string>& vOutput);
    bool HINCRBY(const std::string& strKey, const std::string& strField, int nIncVal, int& nResult);

    bool ZADD(const std::string& strKey, int nScore, const std::string& strMember, int& nResult);
    bool ZCARD(const std::string& strKey, int& nResult);
    bool ZREVRANGE_WITHSCORES(const std::string& strKey, int nStart, int nStop, std::vector<std::pair<std::string, int> >& vResult);
    bool ZREMRANGEBYSCORE(const std::string& strKey, int nFromScore, int nToScore, int& nResult);
    bool ZSCORE(const std::string& strKey, const std::string& strMember, int& nResult);
    bool ZREM(const std::string& strKey, const std::string& strMember, int& nResult);

    bool LPUSH(const std::string& strKey, const std::string& strValue, int& nResult);
    bool RPOP(const std::string& strKey, std::string& strResult);

    bool SADD(const std::string& strKey, const std::string& strValue, int& nResult);
    bool SREM(const std::string& strKey, const std::string& strValue, int& nResult);
    bool SMEMBERS(const std::string& strKey, std::vector<std::string>& vResult);
    /// End of RedisCmd
    //////////////////////////////////////////////////////////////////////////

private:
    bool VerifyRedisSessionList();

    bool ResolveRedisSession(const std::string& strKey);

    bool ConnectToEveryMasterRedisNode();

    CFlyRedisSession* CreateRedisSession(const std::string& strRedisAddress);

    void DestroyRedisSession(const std::string& strIPPort);
    void DestroyRedisSession(CFlyRedisSession* pRedisSession);

    void PingEveryRedisNode(std::vector<CFlyRedisSession*>& vDeadRedisSession);

    bool PrepareRunRedisCmd(const std::string& strKey);
    bool RunRedisCmdOnOneLineResponseInt(int& nResult, const char* pszCaller);
    bool RunRedisCmdOnOneLineResponseString(std::string& strResult, const char* pszCaller);

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
    std::vector<std::string> m_vRedisCmdParamList;
    std::string m_strRedisCmdRequest;
    std::vector<std::string> m_vRedisResponseLine;
};

#endif // _FLYREDISCLIENT_H_
