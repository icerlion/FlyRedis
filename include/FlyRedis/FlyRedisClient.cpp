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
* FileName: FlyRedisClient.cpp
*
* Purpose:  Define FlyRedisClient, hold all redis session
*
* Author:   Jhon Frank(icerlion@163.com)
*
* Modify:   2019/5/23 15:24
===================================================================+*/
#include "FlyRedisClient.h"
#include "FlyRedis.h"

#define CHECK_CUR_REDIS_SESSION() if (nullptr == m_pCurRedisSession) { CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull"); m_bHasBadRedisSession = true; return false; }

CFlyRedisClient::CFlyRedisClient()
    :m_bClusterFlag(false),
    m_pCurRedisSession(nullptr),
    m_nMasterNodeCount(0),
    m_bHasBadRedisSession(false)
{
}

CFlyRedisClient::~CFlyRedisClient()
{
    Close();
}

void CFlyRedisClient::SetRedisConfig(const std::string& strRedisAddress, const std::string& strPassword)
{
    m_setRedisAddressSeed.insert(strRedisAddress);
    m_strRedisPasswod = strPassword;
}

bool CFlyRedisClient::Open()
{
    for (auto& strAddress : m_setRedisAddressSeed)
    {
        CFlyRedisSession* pRedisSession = CreateRedisSession(strAddress);
        if (nullptr != pRedisSession)
        {
            break;
        }
    }
    if (nullptr == m_pCurRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "No RedisNode Is Reachable");
        return false;
    }
    m_nMasterNodeCount = 1;
    if (!m_pCurRedisSession->INFO_CLUSTER(m_bClusterFlag))
    {
        return true;
    }
    if (!m_bClusterFlag)
    {
        return true;
    }
    return ConnectToEveryMasterRedisNode();
}

void CFlyRedisClient::Close()
{
    m_bClusterFlag = false;
    std::map<std::string, CFlyRedisSession *> mapRedisSessionCopy = m_mapRedisSession;
    for (auto& kvp : mapRedisSessionCopy)
    {
        DestroyRedisSession(kvp.second);
    }
    m_pCurRedisSession = nullptr;
    m_mapRedisSession.clear();
    m_nMasterNodeCount = 0;
    m_bHasBadRedisSession = false;
}

bool CFlyRedisClient::APPEND(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("APPEND");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITCOUNT(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BITCOUNT");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITCOUNT(const std::string& strKey, int nStart, int nEnd, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BITCOUNT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nEnd));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITOP_AND(const std::string& strDestKey, const std::string& strSrcKey, int& nResult)
{
    if (m_bClusterFlag && CFlyRedis::KeyHashSlot(strDestKey) != CFlyRedis::KeyHashSlot(strSrcKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BITOP");
    m_vecRedisCmdParamList.push_back("AND");
    m_vecRedisCmdParamList.push_back(strDestKey);
    m_vecRedisCmdParamList.push_back(strSrcKey);
    return RunRedisCmdOnOneLineResponseInt(strDestKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITOP_OR(const std::string& strDestKey, const std::string& strSrcKey, int& nResult)
{
    if (m_bClusterFlag && CFlyRedis::KeyHashSlot(strDestKey) != CFlyRedis::KeyHashSlot(strSrcKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BITOP");
    m_vecRedisCmdParamList.push_back("OR");
    m_vecRedisCmdParamList.push_back(strDestKey);
    m_vecRedisCmdParamList.push_back(strSrcKey);
    return RunRedisCmdOnOneLineResponseInt(strDestKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITOP_XOR(const std::string& strDestKey, const std::string& strSrcKey, int& nResult)
{
    if (m_bClusterFlag && CFlyRedis::KeyHashSlot(strDestKey) != CFlyRedis::KeyHashSlot(strSrcKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BITOP");
    m_vecRedisCmdParamList.push_back("XOR");
    m_vecRedisCmdParamList.push_back(strDestKey);
    m_vecRedisCmdParamList.push_back(strSrcKey);
    return RunRedisCmdOnOneLineResponseInt(strDestKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITOP_NOT(const std::string& strDestKey, const std::string& strSrcKey, int& nResult)
{
    if (m_bClusterFlag && CFlyRedis::KeyHashSlot(strDestKey) != CFlyRedis::KeyHashSlot(strSrcKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BITOP");
    m_vecRedisCmdParamList.push_back("NOT");
    m_vecRedisCmdParamList.push_back(strDestKey);
    m_vecRedisCmdParamList.push_back(strSrcKey);
    return RunRedisCmdOnOneLineResponseInt(strDestKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITPOS(const std::string& strKey, int nBit, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BITPOS");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nBit));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITPOS(const std::string& strKey, int nBit, int nStart, int nEnd, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BITPOS");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nBit));
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nEnd));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::DECR(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("DECR");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::DECRBY(const std::string& strKey, int nDecrement, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("DECRBY");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nDecrement));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::GET(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("GET");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::GETBIT(const std::string& strKey, int nOffset, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("GETBIT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nOffset));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::GETRANGE(const std::string& strKey, int nStart, int nEnd, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("GETRANGE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nEnd));
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::GETSET(const std::string& strKey, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("GETSET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::INCR(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("INCR");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::INCRBY(const std::string& strKey, int nIncrement, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("INCRBY");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nIncrement));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}


bool CFlyRedisClient::INCRBYFLOAT(const std::string& strKey, double fIncrement, double& fResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("INCRBYFLOAT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(fIncrement));
    return RunRedisCmdOnOneLineResponseDouble(strKey, fResult, __FUNCTION__);
}

bool CFlyRedisClient::MGET(const std::vector<std::string>& vecKey, std::vector<std::string>& vecResult)
{
    if (vecKey.empty())
    {
        return false;
    }
    if (m_bClusterFlag && !CFlyRedis::IsMlutiKeyOnTheSameSlot(vecKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("MGET");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecKey.begin(), vecKey.end());
    if (!DeliverRedisCmd(vecKey.front(), __FUNCTION__))
    {
        return false;
    }
    vecResult.swap(m_vecRedisResponseLine);
    return vecResult.size() == vecKey.size();
}

bool CFlyRedisClient::MSET(const std::map<std::string, std::string>& mapKeyValue)
{
    if (m_bClusterFlag && !CFlyRedis::IsMlutiKeyOnTheSameSlot(mapKeyValue))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    std::string strFirstKey;
    m_vecRedisCmdParamList.push_back("MSET");
    for (auto& kvp : mapKeyValue)
    {
        strFirstKey = kvp.first;
        m_vecRedisCmdParamList.push_back(kvp.first);
        m_vecRedisCmdParamList.push_back(kvp.second);
    }
    std::string strResult;
    return RunRedisCmdOnOneLineResponseString(strFirstKey, strResult, __FUNCTION__) && strResult == "OK";
}

bool CFlyRedisClient::MSETNX(const std::map<std::string, std::string>& mapKeyValue, int& nResult)
{
    if (m_bClusterFlag && !CFlyRedis::IsMlutiKeyOnTheSameSlot(mapKeyValue))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    std::string strFirstKey;
    m_vecRedisCmdParamList.push_back("MSETNX");
    for (auto& kvp : mapKeyValue)
    {
        strFirstKey = kvp.first;
        m_vecRedisCmdParamList.push_back(kvp.first);
        m_vecRedisCmdParamList.push_back(kvp.second);
    }
    return RunRedisCmdOnOneLineResponseInt(strFirstKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PSETEX(const std::string& strKey, int nTimeOutMS, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("PSETEX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nTimeOutMS));
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::VerifyRedisSessionList()
{
    if (!m_bHasBadRedisSession)
    {
        return true;
    }
    std::vector<CFlyRedisSession*> vBadSession;
    PingEveryRedisNode(vBadSession);
    if (vBadSession.empty() && (int)m_mapRedisSession.size() == m_nMasterNodeCount)
    {
        m_bHasBadRedisSession = false;
        return true;
    }
    CFlyRedis::Logger(FlyRedisLogLevel::Warning, "RedisClientHasBadSession, FixIt");
    for (CFlyRedisSession* pRedisSession : vBadSession)
    {
        DestroyRedisSession(pRedisSession);
    }
    if (Open())
    {
        m_bHasBadRedisSession = false;
        return true;
    }
    return false;
}

bool CFlyRedisClient::SCRIPT_LOAD(const std::string& strScript, std::string& strResult)
{
    bool bResult = true;
    for (auto& kvp : m_mapRedisSession)
    {
        CFlyRedisSession* pRedisSession = kvp.second;
        if (nullptr != pRedisSession)
        {
            if (!pRedisSession->SCRIPT_LOAD(strScript, strResult))
            {
                bResult = false;
            }
        }
    }
    return bResult;
}

bool CFlyRedisClient::EXISTS(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("EXISTS");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::EXPIRE(const std::string& strKey, int nSeconds, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("EXPIRE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nSeconds));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::EXPIREAT(const std::string& strKey, int nTimestamp, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("EXPIREAT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nTimestamp));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PERSIST(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("PERSIST");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PEXPIRE(const std::string& strKey, int nMS, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("PEXPIRE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nMS));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PEXPIREAT(const std::string& strKey, int nMS, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("PEXPIREAT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nMS));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SET(const std::string& strKey, const std::string& strValue)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    std::string strResult;
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__) && strResult.compare("OK") == 0;
}

bool CFlyRedisClient::SETBIT(const std::string& strKey, int nOffset, int nValue, int& nResult)
{
    ClearRedisCmdCache();
    if (nValue != 1 && nValue != 0)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ERR bit is not an integer or out of range");
        return false;
    }
    m_vecRedisCmdParamList.push_back("SETBIT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nOffset));
    m_vecRedisCmdParamList.push_back(std::to_string(nValue));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::DEL(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("DEL");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::DUMP(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("DUMP");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::TTL(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("TTL");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PTTL(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("PTTL");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::RENAME(const std::string& strFromKey, const std::string& strToKey, std::string& strResult)
{
    if (m_bClusterFlag && CFlyRedis::KeyHashSlot(strFromKey) != CFlyRedis::KeyHashSlot(strToKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("RENAME");
    m_vecRedisCmdParamList.push_back(strFromKey);
    m_vecRedisCmdParamList.push_back(strToKey);
    return RunRedisCmdOnOneLineResponseString(strFromKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::RENAMENX(const std::string& strFromKey, const std::string& strToKey, std::string& strResult)
{
    if (m_bClusterFlag && CFlyRedis::KeyHashSlot(strFromKey) != CFlyRedis::KeyHashSlot(strToKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("RENAMENX");
    m_vecRedisCmdParamList.push_back(strFromKey);
    m_vecRedisCmdParamList.push_back(strToKey);
    return RunRedisCmdOnOneLineResponseString(strFromKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::TOUCH(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("TOUCH");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::TYPE(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("TYPE");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::UNLINK(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("UNLINK");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SETEX(const std::string& strKey, int nTimeOutSeconds, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SETEX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nTimeOutSeconds));
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::SETNX(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SETNX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SETRANGE(const std::string& strKey, int nOffset, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SETRANGE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nOffset));
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::STRLEN(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("STRLEN");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HSET(const std::string& strKey, const std::string& strField, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HSET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HSETNX(const std::string& strKey, const std::string& strField, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HSETNX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HSTRLEN(const std::string& strKey, const std::string& strField, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HSTRLEN");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HVALS(const std::string& strKey, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HVALS");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseVector(strKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::HMSET(const std::string& strKey, const std::map<std::string, std::string>& mapFieldValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HMSET");
    m_vecRedisCmdParamList.push_back(strKey);
    for (auto& kvp : mapFieldValue)
    {
        m_vecRedisCmdParamList.push_back(kvp.first);
        m_vecRedisCmdParamList.push_back(kvp.second);
    }
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::HMSET(const std::string& strKey, const std::string& strField, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HMSET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::HGETALL(const std::string& strKey, std::map<std::string, std::string>& mapFieldValue)
{
    ClearRedisCmdCache();
    mapFieldValue.clear();
    m_vecRedisCmdParamList.push_back("HGETALL");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnResponseKVP(strKey, mapFieldValue, __FUNCTION__);
}

bool CFlyRedisClient::HDEL(const std::string& strKey, const std::string& strField, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HDEL");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HEXISTS(const std::string& strKey, const std::string& strField, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HEXISTS");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HGET(const std::string& strKey, const std::string& strField, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HGET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::HMGET(const std::string& strKey, const std::string& strField, std::string& strValue)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HMGET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseString(strKey, strValue, __FUNCTION__);
}

bool CFlyRedisClient::HMGET(const std::string& strKey, const std::vector<std::string>& vecField, std::vector<std::string>& vecOutput)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HMGET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecField.begin(), vecField.end());
    return RunRedisCmdOnOneLineResponseVector(strKey, vecOutput, __FUNCTION__) && vecOutput.size() == vecField.size();
}

bool CFlyRedisClient::HINCRBY(const std::string& strKey, const std::string& strField, int nIncVal, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HINCRBY");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    m_vecRedisCmdParamList.push_back(std::to_string(nIncVal));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HINCRBYFLOAT(const std::string& strKey, const std::string& strField, double fIncVal, double& fResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HINCRBYFLOAT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    m_vecRedisCmdParamList.push_back(std::to_string(fIncVal));
    return RunRedisCmdOnOneLineResponseDouble(strKey, fResult, __FUNCTION__);
}

bool CFlyRedisClient::HKEYS(const std::string& strKey, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HKEYS");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseVector(strKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::HLEN(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HLEN");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZADD(const std::string& strKey, double fScore, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZADD");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(fScore));
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZCARD(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZCARD");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZCOUNT(const std::string& strKey, const std::string& strMin, const std::string& strMax, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZCOUNT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strMin);
    m_vecRedisCmdParamList.push_back(strMax);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZINCRBY(const std::string& strKey, double fIncrement, const std::string& strMember, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZINCRBY");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(fIncrement));
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::ZRANGE(const std::string& strKey, int nStart, int nStop, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZRANGE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nStop));
    return RunRedisCmdOnOneLineResponseVector(strKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ZRANK(const std::string& strKey, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZRANK");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZREVRANGE_WITHSCORES(const std::string& strKey, int nStart, int nStop, std::vector<std::pair<std::string, double> >& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZREVRANGE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nStop));
    m_vecRedisCmdParamList.push_back("WITHSCORES");
    std::vector< std::pair< std::string, std::string> > vecPairList;
    if (!RunRedisCmdOnResponsePairList(strKey, vecPairList, __FUNCTION__))
    {
        return false;
    }
    for (auto& kvp : vecPairList)
    {
        const std::string& strMember = kvp.first;
        double fScore = atof(kvp.second.c_str());
        vecResult.push_back(std::make_pair(strMember, fScore));
    }
    return true;
}

bool CFlyRedisClient::ZREMRANGEBYSCORE(const std::string& strKey, double fFromScore, double fToScore, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZREMRANGEBYSCORE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(fFromScore));
    m_vecRedisCmdParamList.push_back(std::to_string(fToScore));
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZRANGE_WITHSCORES(const std::string& strKey, int nStart, int nStop, std::vector<std::pair<std::string, double> >& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZRANGE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nStop));
    m_vecRedisCmdParamList.push_back("WITHSCORES");
    std::vector< std::pair< std::string, std::string> > vecPairList;
    if (!RunRedisCmdOnResponsePairList(strKey, vecPairList, __FUNCTION__))
    {
        return false;
    }
    for (auto& kvp : vecPairList)
    {
        const std::string& strMember = kvp.first;
        double fScore = atof(kvp.second.c_str());
        vecResult.push_back(std::make_pair(strMember, fScore));
    }
    return true;
}

bool CFlyRedisClient::ZREVRANGE(const std::string& strKey, int nStart, int nStop, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZREVRANGE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nStop));
    return RunRedisCmdOnOneLineResponseVector(strKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ZSCORE(const std::string& strKey, const std::string& strMember, double& fResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZSCORE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseDouble(strKey, fResult, __FUNCTION__);
}

bool CFlyRedisClient::BLPOP(const std::string& strKey, int nTimeout, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BLPOP");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nTimeout));
    return RunRedisCmdOnOneLineResponseVector(strKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::BRPOP(const std::string& strKey, int nTimeout, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BRPOP");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nTimeout));
    return RunRedisCmdOnOneLineResponseVector(strKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::BRPOPLPUSH(const std::string& strSrcKey, const std::string& strDstKey, int nTimeout, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BRPOPLPUSH");
    m_vecRedisCmdParamList.push_back(strSrcKey);
    m_vecRedisCmdParamList.push_back(strDstKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nTimeout));
    return RunRedisCmdOnOneLineResponseString(strSrcKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::LINDEX(const std::string& strKey, int nIndex, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LINDEX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nIndex));
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::LINSERT_BEFORE(const std::string& strKey, const std::string& strPivot, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LINSERT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back("BEFORE");
    m_vecRedisCmdParamList.push_back(strPivot);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LINSERT_AFTER(const std::string& strKey, const std::string& strPivot, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LINSERT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back("AFTER");
    m_vecRedisCmdParamList.push_back(strPivot);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LLEN(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LLEN");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LPOP(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LPOP");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::ZREM(const std::string& strKey, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZREM");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LPUSH(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LPUSH");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LPUSHX(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LPUSHX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LRANGE(const std::string& strKey, int nStart, int nStop, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LRANGE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nStop));
    return RunRedisCmdOnOneLineResponseVector(strKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::LREM(const std::string& strKey, int nCount, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LREM");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nCount));
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LSET(const std::string& strKey, int nIndex, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LSET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nIndex));
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::LTRIM(const std::string& strKey, int nStart, int nStop, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LTRIM");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nStop));
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::RPOP(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("RPOP");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, strResult, __FUNCTION__);
}

bool CFlyRedisClient::RPOPLPUSH(const std::string& strSrcKey, const std::string& strDestKey, std::vector<std::string>& vecResult)
{
    if (m_bClusterFlag && CFlyRedis::KeyHashSlot(strSrcKey) != CFlyRedis::KeyHashSlot(strDestKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("RPOPLPUSH");
    m_vecRedisCmdParamList.push_back(strSrcKey);
    m_vecRedisCmdParamList.push_back(strDestKey);
    return RunRedisCmdOnOneLineResponseVector(strSrcKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::RPUSH(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("RPUSH");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::RPUSHX(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("RPUSHX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SADD(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SADD");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SCARD(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SCARD");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SDIFF(const std::string& strFirstKey, const std::string& strSecondKey, std::vector<std::string>& vecResult)
{
    if (m_bClusterFlag && CFlyRedis::KeyHashSlot(strFirstKey) != CFlyRedis::KeyHashSlot(strSecondKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SDIFF");
    m_vecRedisCmdParamList.push_back(strFirstKey);
    m_vecRedisCmdParamList.push_back(strSecondKey);
    return RunRedisCmdOnOneLineResponseVector(strFirstKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SDIFF(const std::vector<std::string>& vecKey, std::vector<std::string>& vecResult)
{
    if (vecKey.empty())
    {
        return false;
    }
    if (m_bClusterFlag && !CFlyRedis::IsMlutiKeyOnTheSameSlot(vecKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SDIFF");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecKey.begin(), vecKey.end());
    return RunRedisCmdOnOneLineResponseVector(vecKey.front(), vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SDIFFSTORE(const std::string& strDestKey, const std::vector<std::string>& vecSrcKey, int& nResult)
{
    if (m_bClusterFlag && !CFlyRedis::IsMlutiKeyOnTheSameSlot(vecSrcKey, strDestKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SDIFFSTORE");
    m_vecRedisCmdParamList.push_back(strDestKey);
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecSrcKey.begin(), vecSrcKey.end());
    return RunRedisCmdOnOneLineResponseInt(strDestKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SINTER(const std::string& strFirstKey, const std::string& strSecondKey, std::vector<std::string>& vecResult)
{
    if (m_bClusterFlag && CFlyRedis::KeyHashSlot(strFirstKey) != CFlyRedis::KeyHashSlot(strSecondKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SINTER");
    m_vecRedisCmdParamList.push_back(strFirstKey);
    m_vecRedisCmdParamList.push_back(strSecondKey);
    return RunRedisCmdOnOneLineResponseVector(strFirstKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SINTER(const std::vector<std::string>& vecKey, std::vector<std::string>& vecResult)
{
    if (vecKey.empty())
    {
        return false;
    }
    if (m_bClusterFlag && !CFlyRedis::IsMlutiKeyOnTheSameSlot(vecKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SINTER");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecKey.begin(), vecKey.end());
    return RunRedisCmdOnOneLineResponseVector(vecKey.front(), vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SINTERSTORE(const std::string& strDestKey, const std::vector<std::string>& vecSrcKey, int& nResult)
{
    if (m_bClusterFlag && !CFlyRedis::IsMlutiKeyOnTheSameSlot(vecSrcKey, strDestKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SINTERSTORE");
    m_vecRedisCmdParamList.push_back(strDestKey);
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecSrcKey.begin(), vecSrcKey.end());
    return RunRedisCmdOnOneLineResponseInt(strDestKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SISMEMBER(const std::string& strKey, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SISMEMBER");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SREM(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SREM");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SUNION(const std::vector<std::string>& vecSrcKey, std::vector<std::string>& vecResult)
{
    if (vecSrcKey.empty())
    {
        return false;
    }
    if (m_bClusterFlag && !CFlyRedis::IsMlutiKeyOnTheSameSlot(vecSrcKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SUNION");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecSrcKey.begin(), vecSrcKey.end());
    return RunRedisCmdOnOneLineResponseVector(vecSrcKey.front(), vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SUNIONSTORE(const std::string& strDestKey, const std::vector<std::string>& vecSrcKey, int& nResult)
{
    if (m_bClusterFlag && !CFlyRedis::IsMlutiKeyOnTheSameSlot(vecSrcKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SUNIONSTORE");
    m_vecRedisCmdParamList.push_back(strDestKey);
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecSrcKey.begin(), vecSrcKey.end());
    return RunRedisCmdOnOneLineResponseInt(strDestKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SMEMBERS(const std::string& strKey, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SMEMBERS");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseVector(strKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SMOVE(const std::string& strSrcKey, const std::string& strDestKey, const std::string& strMember, int& nResult)
{
    if (m_bClusterFlag && CFlyRedis::KeyHashSlot(strSrcKey) != CFlyRedis::KeyHashSlot(strDestKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SMOVE");
    m_vecRedisCmdParamList.push_back(strSrcKey);
    m_vecRedisCmdParamList.push_back(strDestKey);
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strSrcKey, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SPOP(const std::string& strKey, int nCount, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SPOP");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nCount));
    return RunRedisCmdOnOneLineResponseVector(strKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SRANDMEMBER(const std::string& strKey, int nCount, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SRANDMEMBER");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nCount));
    return RunRedisCmdOnOneLineResponseVector(strKey, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ResolveRedisSession(const std::string& strKey)
{
    if (!m_bClusterFlag)
    {
        return m_pCurRedisSession != nullptr;
    }
    int nSlot = CFlyRedis::KeyHashSlot(strKey);
    if (nullptr != m_pCurRedisSession && m_pCurRedisSession->AcceptHashSlot(nSlot))
    {
        return true;
    }
    m_pCurRedisSession = nullptr;
    for (auto& kvp : m_mapRedisSession)
    {
        CFlyRedisSession* pRedisSession = kvp.second;
        if (nullptr != pRedisSession && pRedisSession->AcceptHashSlot(nSlot))
        {
            m_pCurRedisSession = pRedisSession;
            break;
        }
    }
    return (nullptr != m_pCurRedisSession);
}

bool CFlyRedisClient::ConnectToEveryMasterRedisNode()
{
    if (nullptr == m_pCurRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull");
        return false;
    }
    std::vector<std::string> vClusterNodes;
    if (!m_pCurRedisSession->CLUSTER_NODES(vClusterNodes))
    {
        return false;
    }
    bool bResult = true;
    int nMasterNodeCount = 0;
    for (const std::string& strNodeLine : vClusterNodes)
    {
        if (strNodeLine.empty())
        {
            continue;
        }
        std::vector<std::string> vecNodeField = CFlyRedis::SplitString(strNodeLine, ' ');
        if (vecNodeField.size() < 2)
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "NodeFieldCountInvalid: [%s]", strNodeLine.c_str());
            bResult = false;
            break;
        }
        std::string strNodeIPPort = vecNodeField[1];
        size_t nPos = strNodeIPPort.find('@');
        if (nPos != std::string::npos)
        {
            strNodeIPPort.erase(nPos, strNodeIPPort.length());
        }
        if (strNodeLine.find("fail") != std::string::npos)
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Warning, "RedisNodeFailed: [%s]", strNodeIPPort.c_str());
            DestroyRedisSession(strNodeIPPort);
            continue;
        }
        m_setRedisAddressSeed.insert(strNodeIPPort);
        if (strNodeLine.find("master") != std::string::npos)
        {
            ++nMasterNodeCount;
            if (vecNodeField.size() != 9)
            {
                CFlyRedis::Logger(FlyRedisLogLevel::Error, "MasterNodeFieldInvalid: [%s]", strNodeLine.c_str());
                bResult = false;
                break;
            }
            CFlyRedisSession* pRedisSession = CreateRedisSession(strNodeIPPort);
            if (nullptr == pRedisSession)
            {
                CFlyRedis::Logger(FlyRedisLogLevel::Error, "CreateRedisSession: [%s]", strNodeLine.c_str());
                bResult = false;
                break;
            }
            if (!pRedisSession->SetSelfSlotRange(vecNodeField[8]))
            {
                CFlyRedis::Logger(FlyRedisLogLevel::Error, "SetSelfSlotRange: [%s]", strNodeLine.c_str());
                bResult = false;
                break;
            }
            continue;
        }
        else if (strNodeLine.find("slave") != std::string::npos)
        {
            DestroyRedisSession(strNodeIPPort);
        }
    }
    if (0 == m_nMasterNodeCount)
    {
        m_nMasterNodeCount = nMasterNodeCount;
        CFlyRedis::Logger(FlyRedisLogLevel::Debug, "MasterNodeCount: [%d]", nMasterNodeCount);
    }
    return bResult;
}

CFlyRedisSession* CFlyRedisClient::CreateRedisSession(const std::string& strRedisAddress)
{
    auto itFind = m_mapRedisSession.find(strRedisAddress);
    if (itFind != m_mapRedisSession.end())
    {
        m_pCurRedisSession = itFind->second;
        return m_pCurRedisSession;
    }
    CFlyRedisSession* pRedisSession = new CFlyRedisSession();
    pRedisSession->SetRedisAddress(strRedisAddress);
    if (!pRedisSession->Connect())
    {
        delete pRedisSession;
        pRedisSession = nullptr;
        return nullptr;
    }
    if (!pRedisSession->AUTH(m_strRedisPasswod))
    {
        delete pRedisSession;
        pRedisSession = nullptr;
        return nullptr;
    }
    m_mapRedisSession.insert(std::make_pair(strRedisAddress, pRedisSession));
    m_pCurRedisSession = pRedisSession;
    return pRedisSession;
}

void CFlyRedisClient::DestroyRedisSession(CFlyRedisSession* pRedisSession)
{
    if (m_pCurRedisSession == pRedisSession)
    {
        m_pCurRedisSession = nullptr;
    }
    if (nullptr != pRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Debug, "DestroyRedisSession: [%s]", pRedisSession->GetRedisAddr().c_str());
        m_mapRedisSession.erase(pRedisSession->GetRedisAddr());
        delete pRedisSession;
        pRedisSession = nullptr;
    }
}

void CFlyRedisClient::DestroyRedisSession(const std::string& strIPPort)
{
    auto itFind = m_mapRedisSession.find(strIPPort);
    if (itFind != m_mapRedisSession.end())
    {
        DestroyRedisSession(itFind->second);
    }
}

void CFlyRedisClient::PingEveryRedisNode(std::vector<CFlyRedisSession*>& vecDeadRedisSession)
{
    for (auto& kvp : m_mapRedisSession)
    {
        CFlyRedisSession* pRedisSession = kvp.second;
        if (nullptr != pRedisSession && !pRedisSession->PING())
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Warning, "RedisNode: [%s] PingFailed", pRedisSession->GetRedisAddr().c_str());
            vecDeadRedisSession.push_back(pRedisSession);
        }
    }
}

bool CFlyRedisClient::DeliverRedisCmd(const std::string& strKey, const char* pszCaller)
{
    if (strKey.empty())
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "KeyIsEmpty: [%s]", pszCaller);
        return false;
    }
    if (m_bHasBadRedisSession)
    {
        VerifyRedisSessionList();
    }
    if (!ResolveRedisSession(strKey))
    {
        m_bHasBadRedisSession = true;
        return false;
    }
    if (nullptr == m_pCurRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull: [%s]", pszCaller);
        m_bHasBadRedisSession = true;
        return false;
    }
    CFlyRedis::BuildRedisCmdRequest(m_pCurRedisSession->GetRedisAddr(), m_vecRedisCmdParamList, m_strRedisCmdRequest);
    if (!m_pCurRedisSession->ProcRedisRequest(m_strRedisCmdRequest, m_vecRedisResponseLine))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ProcRedisRequestFailed: [%s]", pszCaller);
        m_bHasBadRedisSession = true;
        return false;
    }
    return true;
}

bool CFlyRedisClient::RunRedisCmdOnOneLineResponseInt(const std::string& strKey, int& nResult, const char* pszCaller)
{
    std::string strResult;
    if (!RunRedisCmdOnOneLineResponseString(strKey, strResult, pszCaller))
    {
        return false;
    }
    nResult = atoi(strResult.c_str());
    return true;
}

bool CFlyRedisClient::RunRedisCmdOnOneLineResponseDouble(const std::string& strKey, double& fResult, const char* pszCaller)
{
    std::string strResult;
    if (!RunRedisCmdOnOneLineResponseString(strKey, strResult, pszCaller))
    {
        return false;
    }
    fResult = atof(strResult.c_str());
    return true;
}

bool CFlyRedisClient::RunRedisCmdOnOneLineResponseString(const std::string& strKey, std::string& strResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, pszCaller))
    {
        return false;
    }
    // Parse line count
    if (1 != m_vecRedisResponseLine.size())
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineCountInvalid: [%zu], Caller: [%s]", m_vecRedisResponseLine.size(), pszCaller);
        m_bHasBadRedisSession = true;
        return false;
    }
    strResult = m_vecRedisResponseLine[0];
    return true;
}

bool CFlyRedisClient::RunRedisCmdOnOneLineResponseVector(const std::string& strKey, std::vector<std::string>& vecResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, pszCaller))
    {
        return false;
    }
    vecResult.swap(m_vecRedisResponseLine);
    return true;
}

bool CFlyRedisClient::RunRedisCmdOnResponseKVP(const std::string& strKey, std::map<std::string, std::string>& mapResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, pszCaller))
    {
        return false;
    }
    int nLineCount = (int)m_vecRedisResponseLine.size();
    if (nLineCount % 2 != 0)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineCountIsNotEven, [%d]", nLineCount);
        return false;
    }
    bool bResult = true;
    int nKeyIndex = 0;
    int nValueIndex = 1;
    for (; nKeyIndex < nLineCount && nValueIndex < nLineCount; nKeyIndex += 2, nValueIndex += 2)
    {
        const std::string& strField = m_vecRedisResponseLine[nKeyIndex];
        const std::string& strValue = m_vecRedisResponseLine[nValueIndex];
        if (!mapResult.insert(std::make_pair(strField, strValue)).second)
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineReduplicateField, [%s]", pszCaller);
            bResult = false;
            break;
        }
    }
    return bResult;
}

bool CFlyRedisClient::RunRedisCmdOnResponsePairList(const std::string& strKey, std::vector< std::pair<std::string, std::string> >& vecResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, pszCaller))
    {
        return false;
    }
    int nLineCount = (int)m_vecRedisResponseLine.size();
    if (nLineCount % 2 != 0)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineCountIsNotEven, [%d]", nLineCount);
        return false;
    }
    int nKeyIndex = 0;
    int nValueIndex = 1;
    for (; nKeyIndex < nLineCount && nValueIndex < nLineCount; nKeyIndex += 2, nValueIndex += 2)
    {
        const std::string& strField = m_vecRedisResponseLine[nKeyIndex];
        const std::string& strValue = m_vecRedisResponseLine[nValueIndex];
        vecResult.push_back(std::make_pair(strField, strValue));
    }
    return true;
}

void CFlyRedisClient::ClearRedisCmdCache()
{
    m_vecRedisCmdParamList.clear();
    m_strRedisCmdRequest.clear();
    m_vecRedisResponseLine.clear();
}
