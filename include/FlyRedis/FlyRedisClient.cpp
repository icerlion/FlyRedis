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
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("EXISTS");
    m_vRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::GET(const std::string& strKey, std::string& strResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("GET");
    m_vRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strResult, __FUNCTION__);
}

bool CFlyRedisClient::SET(const std::string& strKey, const std::string& strValue, std::string& strResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("SET");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strResult, __FUNCTION__);
}

bool CFlyRedisClient::DEL(const std::string& strKey, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("DEL");
    m_vRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::SETEX(const std::string& strKey, int nTimeOutSeconds, const std::string& strValue, std::string& strResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("SETEX");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(std::to_string(nTimeOutSeconds));
    m_vRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strResult, __FUNCTION__);
}

bool CFlyRedisClient::INCR(const std::string& strKey, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("INCR");
    m_vRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::HSET(const std::string& strKey, const std::string& strField, const std::string& strValue, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("HSET");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strField);
    m_vRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::HSETNX(const std::string& strKey, const std::string& strField, const std::string& strValue, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("HSETNX");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strField);
    m_vRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::HMSET(const std::string& strKey, const std::map<std::string, std::string>& mapFieldValue, std::string& strResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("HMSET");
    m_vRedisCmdParamList.push_back(strKey);
    for (auto& kvp : mapFieldValue)
    {
        m_vRedisCmdParamList.push_back(kvp.first);
        m_vRedisCmdParamList.push_back(kvp.second);
    }
    return RunRedisCmdOnOneLineResponseString(strResult, __FUNCTION__);
}

bool CFlyRedisClient::HMSET(const std::string& strKey, const std::string& strField, const std::string& strValue, std::string& strResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("HMSET");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strField);
    m_vRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strResult, __FUNCTION__);
}

bool CFlyRedisClient::HGETALL(const std::string& strKey, std::map<std::string, std::string>& mapFieldValue)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    mapFieldValue.clear();
    m_vRedisCmdParamList.push_back("HGETALL");
    m_vRedisCmdParamList.push_back(strKey);
    if (nullptr == m_pCurRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull: [%s]", __FUNCTION__);
        m_bHasBadRedisSession = true;
        return false;
    }
    CFlyRedis::BuildRedisCmdRequest(m_pCurRedisSession->GetRedisAddr(), m_vRedisCmdParamList, m_strRedisCmdRequest);
    if (!m_pCurRedisSession->ProcRedisRequest(m_strRedisCmdRequest, m_vRedisResponseLine))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ProcRedisRequestFailed: [%s]", __FUNCTION__);
        m_bHasBadRedisSession = true;
        return false;
    }
    int nLineCount = (int)m_vRedisResponseLine.size();
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
        const std::string& strField = m_vRedisResponseLine[nKeyIndex];
        const std::string& strValue = m_vRedisResponseLine[nValueIndex];
        if (!mapFieldValue.insert(std::make_pair(strField, strValue)).second)
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineReduplicateField, [%s]", strKey.c_str());
            bResult = false;
            break;
        }
    }
    return bResult;
}

bool CFlyRedisClient::HDEL(const std::string& strKey, const std::string& strField, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("HDEL");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::HGET(const std::string& strKey, const std::string& strField, std::string& strResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("HGET");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseString(strResult, __FUNCTION__);
}

bool CFlyRedisClient::HMGET(const std::string& strKey, const std::string& strField, std::string& strValue)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("HMGET");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseString(strValue, __FUNCTION__);
}

bool CFlyRedisClient::HMGET(const std::string& strKey, const std::vector<std::string>& vField, std::vector<std::string>& vOutput)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("HMGET");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.insert(m_vRedisCmdParamList.end(), vField.begin(), vField.end());
    if (nullptr == m_pCurRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull: [%s]", __FUNCTION__);
        m_bHasBadRedisSession = true;
        return false;
    }
    CFlyRedis::BuildRedisCmdRequest(m_pCurRedisSession->GetRedisAddr(), m_vRedisCmdParamList, m_strRedisCmdRequest);
    if (!m_pCurRedisSession->ProcRedisRequest(m_strRedisCmdRequest, m_vRedisResponseLine))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ProcRedisRequestFailed: [%s]", __FUNCTION__);
        m_bHasBadRedisSession = true;
        return false;
    }
    if (vField.size() != m_vRedisResponseLine.size())
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "DifferentField: [%zu-%zu]", vField.size(), m_vRedisResponseLine.size());
        return false;
    }
    vOutput.swap(m_vRedisResponseLine);
    return true;
}

bool CFlyRedisClient::HINCRBY(const std::string& strKey, const std::string& strField, int nIncVal, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("HINCRBY");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strField);
    m_vRedisCmdParamList.push_back(std::to_string(nIncVal));
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZADD(const std::string& strKey, int nScore, const std::string& strMember, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("ZADD");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(std::to_string(nScore));
    m_vRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZCARD(const std::string& strKey, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("ZCARD");
    m_vRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZREVRANGE_WITHSCORES(const std::string& strKey, int nStart, int nStop, std::vector<std::pair<std::string, int> >& vResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("ZREVRANGE");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(std::to_string(nStart));
    m_vRedisCmdParamList.push_back(std::to_string(nStop));
    m_vRedisCmdParamList.push_back("WITHSCORES");
    if (nullptr == m_pCurRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull: [%s]", __FUNCTION__);
        m_bHasBadRedisSession = true;
        return false;
    }
    CFlyRedis::BuildRedisCmdRequest(m_pCurRedisSession->GetRedisAddr(), m_vRedisCmdParamList, m_strRedisCmdRequest);
    if (!m_pCurRedisSession->ProcRedisRequest(m_strRedisCmdRequest, m_vRedisResponseLine))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ProcRedisRequestFailed: [%s]", __FUNCTION__);
        m_bHasBadRedisSession = true;
        return false;
    }
    int nLineCount = (int)m_vRedisResponseLine.size();
    if (nLineCount % 2 != 0)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineCountIsNotEven, [%d]", nLineCount);
        return false;
    }
    int nKeyIndex = 0;
    int nValueIndex = 1;
    for (; nKeyIndex < nLineCount && nValueIndex < nLineCount; nKeyIndex += 2, nValueIndex += 2)
    {
        const std::string& strMember = m_vRedisResponseLine[nKeyIndex];
        int nScore = atoi(m_vRedisResponseLine[nValueIndex].c_str());
        vResult.push_back(std::make_pair(strMember, nScore));
    }
    return true;
}

bool CFlyRedisClient::ZREMRANGEBYSCORE(const std::string& strKey, int nFromScore, int nToScore, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("ZREMRANGEBYSCORE");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(std::to_string(nFromScore));
    m_vRedisCmdParamList.push_back(std::to_string(nToScore));
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZSCORE(const std::string& strKey, const std::string& strMember, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("ZSCORE");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZREM(const std::string& strKey, const std::string& strMember, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("ZREM");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::LPUSH(const std::string& strKey, const std::string& strValue, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("LPUSH");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::RPOP(const std::string& strKey, std::string& strResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("RPOP");
    m_vRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strResult, __FUNCTION__);
}

bool CFlyRedisClient::SADD(const std::string& strKey, const std::string& strValue, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("SADD");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::SREM(const std::string& strKey, const std::string& strValue, int& nResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("SREM");
    m_vRedisCmdParamList.push_back(strKey);
    m_vRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(nResult, __FUNCTION__);
}

bool CFlyRedisClient::SMEMBERS(const std::string& strKey, std::vector<std::string>& vResult)
{
    if (!PrepareRunRedisCmd(strKey))
    {
        return false;
    }
    m_vRedisCmdParamList.push_back("SMEMBERS");
    m_vRedisCmdParamList.push_back(strKey);
    if (nullptr == m_pCurRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull: [%s]", __FUNCTION__);
        m_bHasBadRedisSession = true;
        return false;
    }
    CFlyRedis::BuildRedisCmdRequest(m_pCurRedisSession->GetRedisAddr(), m_vRedisCmdParamList, m_strRedisCmdRequest);
    if (!m_pCurRedisSession->ProcRedisRequest(m_strRedisCmdRequest, m_vRedisResponseLine))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ProcRedisRequestFailed: [%s]", __FUNCTION__);
        m_bHasBadRedisSession = true;
        return false;
    }
    vResult.swap(m_vRedisResponseLine);
    return true;
}

bool CFlyRedisClient::ResolveRedisSession(const std::string& strKey)
{
    if (!m_bClusterFlag)
    {
        return m_pCurRedisSession != nullptr;
    }
    int nSlot = CFlyRedis::KeyHashSlot(strKey.c_str(), (int)strKey.length());
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
        std::vector<std::string> vNodeField = CFlyRedis::SplitString(strNodeLine, ' ');
        if (vNodeField.size() < 2)
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "NodeFieldCountInvalid: [%s]", strNodeLine.c_str());
            bResult = false;
            break;
        }
        std::string strNodeIPPort = vNodeField[1];
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
            if (vNodeField.size() != 9)
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
            if (!pRedisSession->SetSelfSlotRange(vNodeField[8]))
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

void CFlyRedisClient::PingEveryRedisNode(std::vector<CFlyRedisSession*>& vDeadRedisSession)
{
    for (auto& kvp : m_mapRedisSession)
    {
        CFlyRedisSession* pRedisSession = kvp.second;
        if (nullptr != pRedisSession && !pRedisSession->PING())
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Warning, "RedisNode: [%s] PingFailed", pRedisSession->GetRedisAddr().c_str());
            vDeadRedisSession.push_back(pRedisSession);
        }
    }
}

bool CFlyRedisClient::PrepareRunRedisCmd(const std::string& strKey)
{
    ClearRedisCmdCache();
    if (m_bHasBadRedisSession)
    {
        VerifyRedisSessionList();
    }
    if (!ResolveRedisSession(strKey))
    {
        m_bHasBadRedisSession = true;
        return false;
    }
    return (m_pCurRedisSession != nullptr);
}

bool CFlyRedisClient::RunRedisCmdOnOneLineResponseInt(int& nResult, const char* pszCaller)
{
    if (nullptr == m_pCurRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull: [%s]", pszCaller);
        m_bHasBadRedisSession = true;
        return false;
    }
    CFlyRedis::BuildRedisCmdRequest(m_pCurRedisSession->GetRedisAddr(), m_vRedisCmdParamList, m_strRedisCmdRequest);
    if (!m_pCurRedisSession->ProcRedisRequest(m_strRedisCmdRequest, m_vRedisResponseLine))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ProcRedisRequestFailed: [%s]", pszCaller);
        m_bHasBadRedisSession = true;
        return false;
    }
    // Parse line count
    if (1 != m_vRedisResponseLine.size())
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineCountInvalid: [%zu], Caller: [%s]", m_vRedisResponseLine.size(), pszCaller);
        m_bHasBadRedisSession = true;
        return false;
    }
    nResult = atoi(m_vRedisResponseLine[0].c_str());
    return true;
}

bool CFlyRedisClient::RunRedisCmdOnOneLineResponseString(std::string& strResult, const char* pszCaller)
{
    if (nullptr == m_pCurRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull: [%s]", pszCaller);
        m_bHasBadRedisSession = true;
        return false;
    }
    CFlyRedis::BuildRedisCmdRequest(m_pCurRedisSession->GetRedisAddr(), m_vRedisCmdParamList, m_strRedisCmdRequest);
    if (!m_pCurRedisSession->ProcRedisRequest(m_strRedisCmdRequest, m_vRedisResponseLine))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ProcRedisRequestFailed: [%s]", pszCaller);
        m_bHasBadRedisSession = true;
        return false;
    }
    // Parse line count
    if (1 != m_vRedisResponseLine.size())
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineCountInvalid: [%zu], Caller: [%s]", m_vRedisResponseLine.size(), pszCaller);
        m_bHasBadRedisSession = true;
        return false;
    }
    strResult = m_vRedisResponseLine[0];
    return true;
}

void CFlyRedisClient::ClearRedisCmdCache()
{
    m_vRedisCmdParamList.clear();
    m_strRedisCmdRequest.clear();
    m_vRedisResponseLine.clear();
}
