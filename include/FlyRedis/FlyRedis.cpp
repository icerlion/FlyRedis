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
* FileName: FlyRedis.cpp
*
* Purpose:  FlyRedis, util function and config function
*
* Author:   Jhon Frank(icerlion@163.com)
*
* Modify:   2019/5/23 15:24
===================================================================+*/
#include "FlyRedis.h"
#include <stdarg.h>

//////////////////////////////////////////////////////////////////////////
// Begin of RedisSession function
CFlyRedisSession::CFlyRedisSession()
    :m_strRedisAddress(),
    m_nReadTimeOutSeconds(5),
    m_nMinSlot(0),
    m_nMaxSlot(0),
    m_bIsMasterNode(false),
    m_boostTCPIOStream(),
    m_buffBulkStrings(),
    m_bRedisResponseError(false)
{
    memset(m_buffBulkStrings, 0, sizeof(m_buffBulkStrings));
}

CFlyRedisSession::~CFlyRedisSession()
{
}

void CFlyRedisSession::SetRedisAddress(const std::string& strAddress)
{
    m_strRedisAddress = strAddress;
}

void CFlyRedisSession::SetReadTimeOut(int nSeconds)
{
    m_nReadTimeOutSeconds = nSeconds;
}

const std::string& CFlyRedisSession::GetRedisAddr() const
{
    return m_strRedisAddress;
}

bool CFlyRedisSession::Connect()
{
    std::vector<std::string> vecAddressField = CFlyRedis::SplitString(m_strRedisAddress, ':');
    if (vecAddressField.size() != 2)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ParseIPFailed, [%s] Invalid AddressFormat", m_strRedisAddress.c_str());
        return false;
    }
    const std::string& strIP = vecAddressField[0];
    int nPort = atoi(vecAddressField[1].c_str());
    // Parse ip address
    boost::system::error_code boostErrorCode;
    boost::asio::ip::address boostIPAddress = boost::asio::ip::address::from_string(strIP, boostErrorCode);
    if (boostErrorCode)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ParseIPFailed, [%s], Msg: [%d-%s]", strIP.c_str(), boostErrorCode.value(), boostErrorCode.message().c_str());
        return false;
    }
    boost::asio::ip::tcp::endpoint boostEndPoint;
    boostEndPoint.address(boostIPAddress);
    boostEndPoint.port(static_cast<unsigned short>(nPort));
    m_boostTCPIOStream.close();
    // Connect To EndPoint
    m_boostTCPIOStream.connect(boostEndPoint);
    auto& refBooostSocket = m_boostTCPIOStream.socket();
    refBooostSocket.set_option(boost::asio::socket_base::keep_alive());
    if (!m_boostTCPIOStream.good())
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ConnectEndPointFailed, [%s]", m_strRedisAddress.c_str());
        return false;
    }
    CFlyRedis::Logger(FlyRedisLogLevel::Notice, "ConnectToRedis: [%s]", m_strRedisAddress.c_str());
    return true;
}

bool CFlyRedisSession::AcceptHashSlot(int nSlot, bool bIsWrite, FlyRedisReadWriteType nFlyRedisReadWriteType) const
{
    if (FlyRedisReadWriteType::ReadOnSlaveWriteOnMaster == nFlyRedisReadWriteType && bIsWrite != m_bIsMasterNode)
    {
        return false;
    }
    return nSlot >= m_nMinSlot && nSlot <= m_nMaxSlot;
}

void CFlyRedisSession::SetSelfSlotRange(int nMinSlot, int nMaxSlot)
{
    m_nMinSlot = nMinSlot;
    m_nMaxSlot = nMaxSlot;
}

bool CFlyRedisSession::ProcRedisRequest(const std::string& strRedisCmdRequest, std::vector<std::string>& vecRedisResponseLine)
{
    // Build RedisCmdRequest String
    vecRedisResponseLine.clear();
    m_bRedisResponseError = false;
    // Send Msg To RedisServer
    m_boostTCPIOStream.expires_after(std::chrono::seconds(m_nReadTimeOutSeconds));
    m_boostTCPIOStream.write(strRedisCmdRequest.c_str(), strRedisCmdRequest.length());
    if (!RecvRedisResponse(vecRedisResponseLine))
    {
        return false;
    }
    return !m_bRedisResponseError;
}

bool CFlyRedisSession::AUTH(std::string& strPassword)
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.push_back("AUTH");
    vecRedisCmdParamList.push_back(strPassword);
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(m_strRedisAddress, vecRedisCmdParamList, strRedisCmdRequest);
    std::vector<std::string> vecRedisResponseLine;
    if (!ProcRedisRequest(strRedisCmdRequest, vecRedisResponseLine))
    {
        return false;
    }
    if (1 != vecRedisResponseLine.size())
    {
        return false;
    }
    const std::string& strResponse = vecRedisResponseLine[0];
    return strResponse.compare("OK") == 0;
}

bool CFlyRedisSession::PING()
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.push_back("PING");
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(m_strRedisAddress, vecRedisCmdParamList, strRedisCmdRequest);
    std::vector<std::string> vecRedisResponseLine;
    if (!ProcRedisRequest(strRedisCmdRequest, vecRedisResponseLine))
    {
        return false;
    }
    if (1 != vecRedisResponseLine.size())
    {
        return false;
    }
    const std::string& strResponse = vecRedisResponseLine[0];
    return strResponse.compare("PONG") == 0;
}

bool CFlyRedisSession::READONLY()
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.push_back("READONLY");
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(m_strRedisAddress, vecRedisCmdParamList, strRedisCmdRequest);
    std::vector<std::string> vecRedisResponseLine;
    if (!ProcRedisRequest(strRedisCmdRequest, vecRedisResponseLine))
    {
        return false;
    }
    if (1 != vecRedisResponseLine.size())
    {
        return false;
    }
    const std::string& strResponse = vecRedisResponseLine[0];
    return strResponse.compare("OK") == 0;
}

bool CFlyRedisSession::INFO_CLUSTER(bool& bClusterEnable)
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.push_back("INFO");
    vecRedisCmdParamList.push_back("CLUSTER");
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(m_strRedisAddress, vecRedisCmdParamList, strRedisCmdRequest);
    std::vector<std::string> vecRedisResponseLine;
    if (!ProcRedisRequest(strRedisCmdRequest, vecRedisResponseLine))
    {
        return false;
    }
    if (1 != vecRedisResponseLine.size())
    {
        return false;
    }
    const std::string& strResponse = vecRedisResponseLine[0];
    if (strResponse.find("cluster_enabled") == std::string::npos)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineLastLineInvalid: [%s]", strResponse.c_str());
        return false;
    }
    bClusterEnable = (strResponse.find('1') != std::string::npos);
    return true;
}

bool CFlyRedisSession::CLUSTER_NODES(std::vector<std::string>& vecResult)
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.push_back("CLUSTER");
    vecRedisCmdParamList.push_back("NODES");
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(m_strRedisAddress, vecRedisCmdParamList, strRedisCmdRequest);
    std::vector<std::string> vecRedisResponseLine;
    if (!ProcRedisRequest(strRedisCmdRequest, vecRedisResponseLine))
    {
        return false;
    }
    if (1 != vecRedisResponseLine.size())
    {
        return false;
    }
    vecResult = CFlyRedis::SplitString(vecRedisResponseLine[0], '\n');
    return true;
}

bool CFlyRedisSession::SCRIPT_LOAD(const std::string& strScript, std::string& strResult)
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.push_back("SCRIPT");
    vecRedisCmdParamList.push_back("LOAD");
    vecRedisCmdParamList.push_back(strScript);
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(m_strRedisAddress, vecRedisCmdParamList, strRedisCmdRequest);
    std::vector<std::string> vecRedisResponseLine;
    if (!ProcRedisRequest(strRedisCmdRequest, vecRedisResponseLine))
    {
        return false;
    }
    if (1 != vecRedisResponseLine.size())
    {
        return false;
    }
    strResult = vecRedisResponseLine[0];
    return true;
}

bool CFlyRedisSession::RecvRedisResponse(std::vector<std::string>& vecRedisResponseLine)
{
    char chHead = 0;
    m_boostTCPIOStream.expires_after(std::chrono::seconds(m_nReadTimeOutSeconds));
    m_boostTCPIOStream.read(&chHead, 1);
    bool bResult = false;
    switch (chHead)
    {
    case '-': // Errors 
        bResult = ReadRedisResponseError(vecRedisResponseLine);
        break;
    case '+': // Simple Strings
        bResult = ReadRedisResponseSimpleStrings(vecRedisResponseLine);
        break;
    case ':': // Integers 
        bResult = ReadRedisResponseIntegers(vecRedisResponseLine);
        break;
    case '$': // Bulk Strings
        bResult = ReadRedisResponseBulkStrings(vecRedisResponseLine);
        break;
    case '*': // Array
        bResult = ReadRedisResponseArrays(vecRedisResponseLine);
        break;
    default:
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "Unknown HeadCharacter, [%s], Char: [%s]", m_strRedisAddress.c_str(), std::to_string(chHead).c_str());
        break;
    }
    return bResult;

}

bool CFlyRedisSession::ReadRedisResponseError(std::vector<std::string>& vecRedisResponseLine)
{
    std::string strLine;
    if (!ReadUntilCRLF(strLine))
    {
        return false;
    }
    vecRedisResponseLine.push_back(strLine);
    CFlyRedis::Logger(FlyRedisLogLevel::Error, "RedisResponseError: [%s]", strLine.c_str());
    m_bRedisResponseError = true;
    return true;
}

bool CFlyRedisSession::ReadRedisResponseSimpleStrings(std::vector<std::string>& vecRedisResponseLine)
{
    std::string strLine;
    if (!ReadUntilCRLF(strLine))
    {
        return false;
    }
    vecRedisResponseLine.push_back(strLine);
    return true;
}

bool CFlyRedisSession::ReadRedisResponseIntegers(std::vector<std::string>& vecRedisResponseLine)
{
    std::string strLine;
    if (!ReadUntilCRLF(strLine))
    {
        return false;
    }
    vecRedisResponseLine.push_back(strLine);
    return true;
}

bool CFlyRedisSession::ReadRedisResponseBulkStrings(std::vector<std::string>& vecRedisResponseLine)
{
    // Read Length
    std::string strLen;
    if (!ReadUntilCRLF(strLen))
    {
        return false;
    }
    int nLen = atoi(strLen.c_str());
    if (-1 == nLen)
    {
        vecRedisResponseLine.push_back(""); // HGET maybe return Empty String
        return true;
    }
    if (nLen < 0)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "Len LessThan 0: [%s]", strLen.c_str());
        return false;
    }
    // If BulkStrings over than 32M, just return false. according the Redis document, the max length should be 512M
    // Just for safe
    if (nLen >= 1024 * 1024 * 32)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "Len OverThan 32M: [%s]", strLen.c_str());
        return false;
    }
    // Length: 2 char for \r\n
    std::string strBulkStrings;
    memset(m_buffBulkStrings, 0, CONST_BUFF_BULK_STRINGS_ONCE_LEN);
    while (nLen > 0)
    {
        if (nLen <= CONST_BUFF_BULK_STRINGS_ONCE_LEN)
        {
            m_boostTCPIOStream.expires_after(std::chrono::seconds(m_nReadTimeOutSeconds));
            m_boostTCPIOStream.read(m_buffBulkStrings, nLen);
            strBulkStrings.append(m_buffBulkStrings, nLen);
            break;
        }
        else
        {
            m_boostTCPIOStream.expires_after(std::chrono::seconds(m_nReadTimeOutSeconds));
            m_boostTCPIOStream.read(m_buffBulkStrings, CONST_BUFF_BULK_STRINGS_ONCE_LEN);
            strBulkStrings.append(m_buffBulkStrings, CONST_BUFF_BULK_STRINGS_ONCE_LEN);
            nLen -= CONST_BUFF_BULK_STRINGS_ONCE_LEN;
        }
    }
    // Read tail CRLF to make stream empty
    m_boostTCPIOStream.expires_after(std::chrono::seconds(m_nReadTimeOutSeconds));
    m_boostTCPIOStream.read(m_buffBulkStrings, 2);
    vecRedisResponseLine.push_back(strBulkStrings);
    return true;
}

bool CFlyRedisSession::ReadRedisResponseArrays(std::vector<std::string>& vecRedisResponseLine)
{
    // Read Length
    std::string strLine;
    if (!ReadUntilCRLF(strLine))
    {
        return false;
    }
    int nLen = atoi(strLine.c_str());
    if (nLen < 0)
    {
        return false;
    }
    if (0 == nLen)
    {
        return true;
    }
    for (int nIndex = 0; nIndex < nLen; ++nIndex)
    {
        RecvRedisResponse(vecRedisResponseLine);
    }
    return true;
}

bool CFlyRedisSession::ReadUntilCRLF(std::string& strLine)
{
    char chPreValue = 0;
    while (true)
    {
        char chCurValue = 0;
        m_boostTCPIOStream.expires_after(std::chrono::seconds(m_nReadTimeOutSeconds));
        m_boostTCPIOStream.read(&chCurValue, 1);
        strLine.append(1, chCurValue);
        if (chPreValue == '\r' && chCurValue == '\n')
        {
            strLine.pop_back();
            strLine.pop_back();
            break;
        }
        chPreValue = chCurValue;
    }
    return true;
}
// End of RedisSession function
//////////////////////////////////////////////////////////////////////////
// Begin of RedisClient
#define CHECK_CUR_REDIS_SESSION() if (nullptr == m_pCurRedisSession) { CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull"); m_bHasBadRedisSession = true; return false; }

CFlyRedisClient::CFlyRedisClient()
    :m_bClusterFlag(false),
    m_nFlyRedisReadWriteType(FlyRedisReadWriteType::ReadWriteOnMaster),
    m_nReadTimeOutSeconds(5),
    m_pCurRedisSession(nullptr),
    m_nRedisNodeCount(0),
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

void CFlyRedisClient::SetFlyRedisReadWriteType(FlyRedisReadWriteType nFlyRedisReadWriteType)
{
    m_nFlyRedisReadWriteType = nFlyRedisReadWriteType;
}

void CFlyRedisClient::SetRedisReadTimeOutSeconds(int nSeconds)
{
    m_nReadTimeOutSeconds = nSeconds;
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
    m_nRedisNodeCount = 1;
    if (!m_pCurRedisSession->INFO_CLUSTER(m_bClusterFlag))
    {
        return true;
    }
    if (!m_bClusterFlag)
    {
        return true;
    }
    return ConnectToEveryRedisNode();
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
    m_nRedisNodeCount = 0;
    m_bHasBadRedisSession = false;
}

bool CFlyRedisClient::APPEND(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("APPEND");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITCOUNT(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BITCOUNT");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITCOUNT(const std::string& strKey, int nStart, int nEnd, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BITCOUNT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nEnd));
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseInt(strDestKey, true, nResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseInt(strDestKey, true, nResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseInt(strDestKey, true, nResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseInt(strDestKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITPOS(const std::string& strKey, int nBit, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BITPOS");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nBit));
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITPOS(const std::string& strKey, int nBit, int nStart, int nEnd, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BITPOS");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nBit));
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nEnd));
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::DECR(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("DECR");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::DECRBY(const std::string& strKey, int nDecrement, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("DECRBY");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nDecrement));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::GET(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("GET");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::GETBIT(const std::string& strKey, int nOffset, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("GETBIT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nOffset));
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::GETRANGE(const std::string& strKey, int nStart, int nEnd, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("GETRANGE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nEnd));
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::GETSET(const std::string& strKey, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("GETSET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::INCR(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("INCR");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::INCRBY(const std::string& strKey, int nIncrement, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("INCRBY");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nIncrement));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}


bool CFlyRedisClient::INCRBYFLOAT(const std::string& strKey, double fIncrement, double& fResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("INCRBYFLOAT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(fIncrement));
    return RunRedisCmdOnOneLineResponseDouble(strKey, true, fResult, __FUNCTION__);
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
    if (!DeliverRedisCmd(vecKey.front(), false, __FUNCTION__))
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
    return RunRedisCmdOnOneLineResponseString(strFirstKey, true, strResult, __FUNCTION__) && strResult == "OK";
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
    return RunRedisCmdOnOneLineResponseInt(strFirstKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PSETEX(const std::string& strKey, int nTimeOutMS, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("PSETEX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nTimeOutMS));
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::VerifyRedisSessionList()
{
    if (!m_bHasBadRedisSession)
    {
        return true;
    }
    std::vector<CFlyRedisSession*> vBadSession;
    PingEveryRedisNode(vBadSession);
    if (vBadSession.empty() && (int)m_mapRedisSession.size() == m_nRedisNodeCount)
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

bool CFlyRedisClient::EVALSHA(const std::string& strSHA, const std::vector<std::string>& vecKey, const std::vector<std::string>& vecArgv, std::string& strResult)
{
    ClearRedisCmdCache();
    if (!CFlyRedis::IsMlutiKeyOnTheSameSlot(vecKey))
    {
        return false;
    }
    if (vecKey.empty())
    {
        return false;
    }
    const std::string& strKeySeed = vecKey.front();
    m_vecRedisCmdParamList.push_back("EVALSHA");
    m_vecRedisCmdParamList.push_back(strSHA);
    m_vecRedisCmdParamList.push_back(std::to_string(vecKey.size()));
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecKey.begin(), vecKey.end());
    m_vecRedisCmdParamList.push_back(std::to_string(vecArgv.size()));
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecArgv.begin(), vecArgv.end());
    return RunRedisCmdOnOneLineResponseString(strKeySeed, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::EVALSHA(const std::string& strSHA, const std::string& strKey, const std::string& strArgv, std::string& strResult)
{
    std::vector<std::string> vecKey;
    vecKey.push_back(strKey);
    std::vector<std::string> vecArgv;
    vecArgv.push_back(strArgv);
    return EVALSHA(strSHA, vecKey, vecArgv, strResult);
}

bool CFlyRedisClient::EVALSHA(const std::string& strSHA, const std::string& strKey, std::string& strResult)
{
    std::vector<std::string> vecKey;
    vecKey.push_back(strKey);
    std::vector<std::string> vecArgv;
    return EVALSHA(strSHA, vecKey, vecArgv, strResult);
}

bool CFlyRedisClient::EVAL(const std::string& strScript, const std::vector<std::string>& vecKey, const std::vector<std::string>& vecArgv, std::string& strResult)
{
    ClearRedisCmdCache();
    if (!CFlyRedis::IsMlutiKeyOnTheSameSlot(vecKey))
    {
        return false;
    }
    if (vecKey.empty())
    {
        return false;
    }
    const std::string& strKeySeed = vecKey.front();
    m_vecRedisCmdParamList.push_back("EVAL");
    m_vecRedisCmdParamList.push_back(strScript);
    m_vecRedisCmdParamList.push_back(std::to_string(vecKey.size()));
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecKey.begin(), vecKey.end());
    m_vecRedisCmdParamList.push_back(std::to_string(vecArgv.size()));
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecArgv.begin(), vecArgv.end());
    return RunRedisCmdOnOneLineResponseString(strKeySeed, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::EVAL(const std::string& strScript, const std::string& strKey, const std::string& strArgv, std::string& strResult)
{
    std::vector<std::string> vecKey;
    vecKey.push_back(strKey);
    std::vector<std::string> vecArgv;
    vecArgv.push_back(strArgv);
    return EVAL(strScript, vecKey, vecArgv, strResult);
}

bool CFlyRedisClient::EVAL(const std::string& strScript, const std::string& strKey, std::string& strResult)
{
    std::vector<std::string> vecKey;
    vecKey.push_back(strKey);
    std::vector<std::string> vecArgv;
    return EVAL(strScript, vecKey, vecArgv, strResult);
}

bool CFlyRedisClient::EXISTS(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("EXISTS");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::EXPIRE(const std::string& strKey, int nSeconds, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("EXPIRE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nSeconds));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::EXPIREAT(const std::string& strKey, int nTimestamp, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("EXPIREAT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nTimestamp));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PERSIST(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("PERSIST");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PEXPIRE(const std::string& strKey, int nMS, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("PEXPIRE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nMS));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PEXPIREAT(const std::string& strKey, int nMS, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("PEXPIREAT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nMS));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SET(const std::string& strKey, const std::string& strValue)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    std::string strResult;
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__) && strResult.compare("OK") == 0;
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
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::DEL(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("DEL");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::DUMP(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("DUMP");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::TTL(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("TTL");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PTTL(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("PTTL");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseString(strFromKey, true, strResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseString(strFromKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::TOUCH(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("TOUCH");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::TYPE(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("TYPE");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::UNLINK(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("UNLINK");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SETEX(const std::string& strKey, int nTimeOutSeconds, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SETEX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nTimeOutSeconds));
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::SETNX(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SETNX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SETRANGE(const std::string& strKey, int nOffset, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SETRANGE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nOffset));
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::STRLEN(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("STRLEN");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HSET(const std::string& strKey, const std::string& strField, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HSET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HSETNX(const std::string& strKey, const std::string& strField, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HSETNX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HSTRLEN(const std::string& strKey, const std::string& strField, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HSTRLEN");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HVALS(const std::string& strKey, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HVALS");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::HMSET(const std::string& strKey, const std::string& strField, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HMSET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::HGETALL(const std::string& strKey, std::map<std::string, std::string>& mapFieldValue)
{
    ClearRedisCmdCache();
    mapFieldValue.clear();
    m_vecRedisCmdParamList.push_back("HGETALL");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnResponseKVP(strKey, false, mapFieldValue, __FUNCTION__);
}

bool CFlyRedisClient::HDEL(const std::string& strKey, const std::string& strField, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HDEL");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HEXISTS(const std::string& strKey, const std::string& strField, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HEXISTS");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HGET(const std::string& strKey, const std::string& strField, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HGET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::HMGET(const std::string& strKey, const std::string& strField, std::string& strValue)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HMGET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    return RunRedisCmdOnOneLineResponseString(strKey, false, strValue, __FUNCTION__);
}

bool CFlyRedisClient::HMGET(const std::string& strKey, const std::vector<std::string>& vecField, std::vector<std::string>& vecOutput)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HMGET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecField.begin(), vecField.end());
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecOutput, __FUNCTION__) && vecOutput.size() == vecField.size();
}

bool CFlyRedisClient::HINCRBY(const std::string& strKey, const std::string& strField, int nIncVal, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HINCRBY");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    m_vecRedisCmdParamList.push_back(std::to_string(nIncVal));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HINCRBYFLOAT(const std::string& strKey, const std::string& strField, double fIncVal, double& fResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HINCRBYFLOAT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strField);
    m_vecRedisCmdParamList.push_back(std::to_string(fIncVal));
    return RunRedisCmdOnOneLineResponseDouble(strKey, true, fResult, __FUNCTION__);
}

bool CFlyRedisClient::HKEYS(const std::string& strKey, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HKEYS");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::HLEN(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("HLEN");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZADD(const std::string& strKey, double fScore, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZADD");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(fScore));
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZCARD(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZCARD");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZCOUNT(const std::string& strKey, const std::string& strMin, const std::string& strMax, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZCOUNT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strMin);
    m_vecRedisCmdParamList.push_back(strMax);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZINCRBY(const std::string& strKey, double fIncrement, const std::string& strMember, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZINCRBY");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(fIncrement));
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::ZRANGE(const std::string& strKey, int nStart, int nStop, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZRANGE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nStop));
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ZRANK(const std::string& strKey, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZRANK");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
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
    if (!RunRedisCmdOnResponsePairList(strKey, false, vecPairList, __FUNCTION__))
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
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
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
    if (!RunRedisCmdOnResponsePairList(strKey, false, vecPairList, __FUNCTION__))
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
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ZSCORE(const std::string& strKey, const std::string& strMember, double& fResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZSCORE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseDouble(strKey, false, fResult, __FUNCTION__);
}

bool CFlyRedisClient::BLPOP(const std::string& strKey, int nTimeout, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BLPOP");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nTimeout));
    return RunRedisCmdOnOneLineResponseVector(strKey, true, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::BRPOP(const std::string& strKey, int nTimeout, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BRPOP");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nTimeout));
    return RunRedisCmdOnOneLineResponseVector(strKey, true, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::BRPOPLPUSH(const std::string& strSrcKey, const std::string& strDstKey, int nTimeout, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("BRPOPLPUSH");
    m_vecRedisCmdParamList.push_back(strSrcKey);
    m_vecRedisCmdParamList.push_back(strDstKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nTimeout));
    return RunRedisCmdOnOneLineResponseString(strSrcKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::LINDEX(const std::string& strKey, int nIndex, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LINDEX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nIndex));
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::LINSERT_BEFORE(const std::string& strKey, const std::string& strPivot, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LINSERT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back("BEFORE");
    m_vecRedisCmdParamList.push_back(strPivot);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LINSERT_AFTER(const std::string& strKey, const std::string& strPivot, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LINSERT");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back("AFTER");
    m_vecRedisCmdParamList.push_back(strPivot);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LLEN(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LLEN");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LPOP(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LPOP");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::ZREM(const std::string& strKey, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("ZREM");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LPUSH(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LPUSH");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LPUSHX(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LPUSHX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LRANGE(const std::string& strKey, int nStart, int nStop, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LRANGE");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nStop));
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::LREM(const std::string& strKey, int nCount, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LREM");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nCount));
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LSET(const std::string& strKey, int nIndex, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LSET");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nIndex));
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::LTRIM(const std::string& strKey, int nStart, int nStop, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("LTRIM");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nStart));
    m_vecRedisCmdParamList.push_back(std::to_string(nStop));
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::RPOP(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("RPOP");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseVector(strSrcKey, true, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::RPUSH(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("RPUSH");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::RPUSHX(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("RPUSHX");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SADD(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SADD");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SCARD(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SCARD");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseVector(strFirstKey, false, vecResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseVector(vecKey.front(), false, vecResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseInt(strDestKey, true, nResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseVector(strFirstKey, false, vecResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseVector(vecKey.front(), false, vecResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseInt(strDestKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SISMEMBER(const std::string& strKey, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SISMEMBER");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SREM(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SREM");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseVector(vecSrcKey.front(), false, vecResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseInt(strDestKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SMEMBERS(const std::string& strKey, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SMEMBERS");
    m_vecRedisCmdParamList.push_back(strKey);
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
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
    return RunRedisCmdOnOneLineResponseInt(strSrcKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SPOP(const std::string& strKey, int nCount, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SPOP");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nCount));
    return RunRedisCmdOnOneLineResponseVector(strKey, true, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SRANDMEMBER(const std::string& strKey, int nCount, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.push_back("SRANDMEMBER");
    m_vecRedisCmdParamList.push_back(strKey);
    m_vecRedisCmdParamList.push_back(std::to_string(nCount));
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ResolveRedisSession(const std::string& strKey, bool bIsWrite)
{
    if (!m_bClusterFlag)
    {
        return m_pCurRedisSession != nullptr;
    }
    int nSlot = CFlyRedis::KeyHashSlot(strKey);
    if (nullptr != m_pCurRedisSession && m_pCurRedisSession->AcceptHashSlot(nSlot, bIsWrite, m_nFlyRedisReadWriteType))
    {
        return true;
    }
    m_pCurRedisSession = nullptr;
    for (auto& kvp : m_mapRedisSession)
    {
        CFlyRedisSession* pRedisSession = kvp.second;
        if (nullptr != pRedisSession && pRedisSession->AcceptHashSlot(nSlot, bIsWrite, m_nFlyRedisReadWriteType))
        {
            m_pCurRedisSession = pRedisSession;
            break;
        }
    }
    if (nullptr == m_pCurRedisSession && m_nFlyRedisReadWriteType == FlyRedisReadWriteType::ReadOnSlaveWriteOnMaster && !bIsWrite)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Warning, "SlaveFailedSoRedirToMaster: [%s]", strKey.c_str());
        for (auto& kvp : m_mapRedisSession)
        {
            CFlyRedisSession* pRedisSession = kvp.second;
            if (nullptr != pRedisSession && pRedisSession->AcceptHashSlot(nSlot, true, m_nFlyRedisReadWriteType))
            {
                m_pCurRedisSession = pRedisSession;
                break;
            }
        }
    }
    return (nullptr != m_pCurRedisSession);
}

bool CFlyRedisClient::ConnectToEveryRedisNode()
{
    if (nullptr == m_pCurRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull");
        return false;
    }
    std::vector<std::string> vecClusterNodes;
    if (!m_pCurRedisSession->CLUSTER_NODES(vecClusterNodes))
    {
        return false;
    }
    bool bResult = true;
    std::map<std::string, RedisClusterNodesLine> mapRedisClusterNodesLine;
    for (const std::string& strNodeLine : vecClusterNodes)
    {
        if (strNodeLine.empty())
        {
            continue;
        }
        RedisClusterNodesLine stRedisClusterNodesLine;
        if (!stRedisClusterNodesLine.ParseNodeLine(strNodeLine))
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "NodeLineInvalid: [%s]", strNodeLine.c_str());
            bResult = false;
            break;
        }
        if (!mapRedisClusterNodesLine.insert(std::make_pair(stRedisClusterNodesLine.strNodeId, stRedisClusterNodesLine)).second)
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "RedisNodeIdReduplicated: [%s]", strNodeLine.c_str());
            bResult = false;
            break;
        }
    }
    if (!bResult)
    {
        return false;
    }
    m_nRedisNodeCount = static_cast<int>(mapRedisClusterNodesLine.size());
    for (auto& kvp : mapRedisClusterNodesLine)
    {
        RedisClusterNodesLine& refRedisNode = kvp.second;
        // Refresh min max slot first
        if (!refRedisNode.bIsMaster)
        {
            // If read and write on master only, we should destroy redis session
            if (FlyRedisReadWriteType::ReadWriteOnMaster == m_nFlyRedisReadWriteType)
            {
                DestroyRedisSession(refRedisNode.strNodeIPPort);
                continue;
            }
            auto itFindMaster = mapRedisClusterNodesLine.find(refRedisNode.strMasterNodeId);
            if (itFindMaster == mapRedisClusterNodesLine.end())
            {
                CFlyRedis::Logger(FlyRedisLogLevel::Error, "SlaveHasNoMaster: [%s]", kvp.first.c_str());
                bResult = false;
                break;
            }
            const RedisClusterNodesLine& refMasterNode = itFindMaster->second;
            refRedisNode.nMinSlot = refMasterNode.nMinSlot;
            refRedisNode.nMaxSlot = refMasterNode.nMaxSlot;
        }
        if (!ConnectToOneClusterNode(refRedisNode))
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "ConnectToClusterNodesLineFailed: [%s-%s]", kvp.first.c_str(), refRedisNode.strNodeIPPort.c_str());
            bResult = false;
            continue;
        }
    }
    return bResult;
}

bool CFlyRedisClient::ConnectToOneClusterNode(const RedisClusterNodesLine& stRedisNode)
{
    CFlyRedisSession* pRedisSession = CreateRedisSession(stRedisNode.strNodeIPPort);
    if (nullptr == pRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CreateRedisSessionFailed: [%s]", stRedisNode.strNodeIPPort.c_str());
        return false;
    }
    pRedisSession->SetSelfSlotRange(stRedisNode.nMinSlot, stRedisNode.nMaxSlot);
    pRedisSession->SetMasterNodeFlag(stRedisNode.bIsMaster);
    if (!stRedisNode.bIsMaster && FlyRedisReadWriteType::ReadOnSlaveWriteOnMaster == m_nFlyRedisReadWriteType)
    {
        pRedisSession->READONLY();
    }
    return true;
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
    pRedisSession->SetReadTimeOut(m_nReadTimeOutSeconds);
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

bool CFlyRedisClient::DeliverRedisCmd(const std::string& strKey, bool bIsWrite, const char* pszCaller)
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
    if (!ResolveRedisSession(strKey, bIsWrite))
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

bool CFlyRedisClient::RunRedisCmdOnOneLineResponseInt(const std::string& strKey, bool bIsWrite, int& nResult, const char* pszCaller)
{
    std::string strResult;
    if (!RunRedisCmdOnOneLineResponseString(strKey, bIsWrite, strResult, pszCaller))
    {
        return false;
    }
    nResult = atoi(strResult.c_str());
    return true;
}

bool CFlyRedisClient::RunRedisCmdOnOneLineResponseDouble(const std::string& strKey, bool bIsWrite, double& fResult, const char* pszCaller)
{
    std::string strResult;
    if (!RunRedisCmdOnOneLineResponseString(strKey, bIsWrite, strResult, pszCaller))
    {
        return false;
    }
    fResult = atof(strResult.c_str());
    return true;
}

bool CFlyRedisClient::RunRedisCmdOnOneLineResponseString(const std::string& strKey, bool bIsWrite, std::string& strResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, bIsWrite, pszCaller))
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

bool CFlyRedisClient::RunRedisCmdOnOneLineResponseVector(const std::string& strKey, bool bIsWrite, std::vector<std::string>& vecResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, bIsWrite, pszCaller))
    {
        return false;
    }
    vecResult.swap(m_vecRedisResponseLine);
    return true;
}

bool CFlyRedisClient::RunRedisCmdOnResponseKVP(const std::string& strKey, bool bIsWrite, std::map<std::string, std::string>& mapResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, bIsWrite, pszCaller))
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

bool CFlyRedisClient::RunRedisCmdOnResponsePairList(const std::string& strKey, bool bIsWrite, std::vector< std::pair<std::string, std::string> >& vecResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, bIsWrite, pszCaller))
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
// End of RedisClient
//////////////////////////////////////////////////////////////////////////
// Begin of FlyRedis
std::function<void(const char*)> CFlyRedis::ms_pfnLoggerDebug = nullptr;
std::function<void(const char*)> CFlyRedis::ms_pfnLoggerNotice = nullptr;
std::function<void(const char*)> CFlyRedis::ms_pfnLoggerWarning = nullptr;
std::function<void(const char*)> CFlyRedis::ms_pfnLoggerError = nullptr;
std::function<void(const char*)> CFlyRedis::ms_pfnLoggerPersistence = nullptr;

void CFlyRedis::SetLoggerHandler(FlyRedisLogLevel nLogLevel, std::function<void(const char*)> pfnLoggerHandler)
{
    switch (nLogLevel)
    {
    case FlyRedisLogLevel::Debug:
        ms_pfnLoggerDebug = pfnLoggerHandler;
        break;
    case FlyRedisLogLevel::Notice:
        ms_pfnLoggerNotice = pfnLoggerHandler;
        break;
    case FlyRedisLogLevel::Warning:
        ms_pfnLoggerWarning = pfnLoggerHandler;
        break;
    case FlyRedisLogLevel::Error:
        ms_pfnLoggerError = pfnLoggerHandler;
        break;
    case FlyRedisLogLevel::Command:
        ms_pfnLoggerPersistence = pfnLoggerHandler;
        break;
    }
}

std::function<void(const char*)> CFlyRedis::GetLoggerHandler(FlyRedisLogLevel nLogLevel)
{
    std::function<void(const char*)> pfnResult = nullptr;
    switch (nLogLevel)
    {
    case FlyRedisLogLevel::Debug:
        pfnResult = ms_pfnLoggerDebug;
        break;
    case FlyRedisLogLevel::Notice:
        pfnResult = ms_pfnLoggerNotice;
        break;
    case FlyRedisLogLevel::Warning:
        pfnResult = ms_pfnLoggerWarning;
        break;
    case FlyRedisLogLevel::Error:
        pfnResult = ms_pfnLoggerError;
        break;
    case FlyRedisLogLevel::Command:
        pfnResult = ms_pfnLoggerPersistence;
        break;
    }
    return pfnResult;
}

void CFlyRedis::Logger(FlyRedisLogLevel nLevel, const char* pszMsgFormat, ...)
{
    auto pfnLoggerHandler = GetLoggerHandler(nLevel);
    if (nullptr != pfnLoggerHandler)
    {
        char buffLogContent[4096] = { 0 };
        va_list vaList;
        va_start(vaList, pszMsgFormat);
#ifdef WIN32
        vsprintf_s(buffLogContent, pszMsgFormat, vaList);
#else
        vsprintf(buffLogContent, pszMsgFormat, vaList);
#endif
        va_end(vaList);
        pfnLoggerHandler(buffLogContent);
    }
}

static const unsigned short CONST_CRC16_TABLE[256] = {
    0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
    0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
    0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
    0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
    0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
    0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
    0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
    0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
    0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
    0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
    0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
    0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
    0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
    0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
    0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
    0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
    0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
    0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
    0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
    0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
    0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
    0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
    0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
    0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
    0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
    0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
    0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
    0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
    0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
    0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
    0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
    0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

int CFlyRedis::CRC16(const char* buff, int nBuffLen)
{
    // Reference: https://github.com/vipshop/hiredis-vip/blob/master/crc16.c
    unsigned short nCRCValue = 0;
    for (int nIndex = 0; nIndex < nBuffLen; ++nIndex)
    {
        nCRCValue = (nCRCValue << 8) ^ CONST_CRC16_TABLE[((nCRCValue >> 8) ^ *buff++) & 0x00FF];
    }
    return nCRCValue;
}

bool CFlyRedis::IsMlutiKeyOnTheSameSlot(const std::string& strKeyFirst, const std::string& strKeySecond)
{
    return KeyHashSlot(strKeyFirst) == KeyHashSlot(strKeySecond);
}

bool CFlyRedis::IsMlutiKeyOnTheSameSlot(const std::vector<std::string>& vecKey)
{
    std::set<int> setSlot;
    for (auto& strKey : vecKey)
    {
        setSlot.insert(KeyHashSlot(strKey));
    }
    return setSlot.size() == 1;
}

bool CFlyRedis::IsMlutiKeyOnTheSameSlot(const std::map<std::string, std::string>& mapKeyValue)
{
    std::set<int> setSlot;
    for (auto& kvp : mapKeyValue)
    {
        setSlot.insert(KeyHashSlot(kvp.first));
    }
    return setSlot.size() == 1;
}

bool CFlyRedis::IsMlutiKeyOnTheSameSlot(const std::vector<std::string>& vecKey, const std::string& strMoreKey)
{
    std::set<int> setSlot;
    setSlot.insert(KeyHashSlot(strMoreKey));
    for (auto& strKey : vecKey)
    {
        setSlot.insert(KeyHashSlot(strKey));
    }
    return setSlot.size() == 1;
}

int CFlyRedis::KeyHashSlot(const char* pszKey, int nKeyLen)
{
    //////////////////////////////////////////////////////////////////////////
    // Reference: https://github.com/vipshop/hiredis-vip/blob/master/hircluster.c 
    // same as: static unsigned int keyHashSlot(char *key, int keylen)
    //////////////////////////////////////////////////////////////////////////
    // start-end indexes of { and }
    int nStart = 0;
    int nEnd = 0;
    for (nStart = 0; nStart < nKeyLen; ++nStart)
    {
        if (pszKey[nStart] == '{')
        {
            break;
        }
    }

    // No '{' ? Hash the whole key. This is the base case.
    if (nStart == nKeyLen)
    {
        return CRC16(pszKey, nKeyLen) & 0x3FFF;
    }

    // '{' found? Check if we have the corresponding '}'.
    for (nEnd = nStart + 1; nEnd < nKeyLen; ++nEnd)
    {
        if (pszKey[nEnd] == '}')
        {
            break;
        }
    }

    // No '}' or nothing betweeen {} ? Hash the whole key.
    if (nEnd == nKeyLen || nEnd == nStart + 1)
    {
        return CRC16(pszKey, nKeyLen) & 0x3FFF;
    }

    // If we are here there is both a { and a } on its right. Hash what is in the middle between { and }.
    return CRC16(pszKey + nStart + 1, nEnd - nStart - 1) & 0x3FFF;
}

int CFlyRedis::KeyHashSlot(const std::string& strKey)
{
    return KeyHashSlot(strKey.c_str(), (int)strKey.length());
}

std::vector<std::string> CFlyRedis::SplitString(const std::string& strInput, char chDelim)
{
    std::vector<std::string> vecResult;
    if (strInput.empty())
    {
        return vecResult;
    }
    std::string strToken;
    for (char chValue : strInput)
    {
        if (chValue != chDelim)
        {
            strToken.append(1, chValue);
        }
        else
        {
            vecResult.push_back(strToken);
            strToken.clear();
        }
    }
    if (!strToken.empty())
    {
        vecResult.push_back(strToken);
    }
    if (strInput.back() == chDelim)
    {
        vecResult.push_back("");
    }
    return vecResult;
}

void CFlyRedis::BuildRedisCmdRequest(const std::string& strRedisAddress, const std::vector<std::string>& vecRedisCmdParamList, std::string& strRedisCmdRequest)
{
    std::string strCmdLog;
    strRedisCmdRequest.clear();
    strRedisCmdRequest.append("*").append(std::to_string((int)vecRedisCmdParamList.size())).append("\r\n");
    for (const std::string& strParam : vecRedisCmdParamList)
    {
        strRedisCmdRequest.append("$").append(std::to_string((int)strParam.length())).append("\r\n");
        strRedisCmdRequest.append(strParam).append("\r\n");
        if (strCmdLog.length() < 2048)
        {
            strCmdLog.append(strParam).append(" ");
        }
        else
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Command, "RedisCmd,%s,%s", strRedisAddress.c_str(), strCmdLog.c_str());
            strCmdLog.clear();
            strCmdLog.append(strParam).append(" ");
        }
    }
    if (!strCmdLog.empty())
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Command, "RedisCmd,%s,%s", strRedisAddress.c_str(), strCmdLog.c_str());
    }
}

// End of FlyRedis
//////////////////////////////////////////////////////////////////////////
// Define Struct RedisClusterNodesLine
CFlyRedisClient::RedisClusterNodesLine::RedisClusterNodesLine() 
    :strNodeId(),
    strNodeIPPort(),
    bIsMaster(false),
    strMasterNodeId(),
    nMinSlot(0),
    nMaxSlot(0)
{

}

bool CFlyRedisClient::RedisClusterNodesLine::ParseNodeLine(const std::string& strNodeLine)
{
    bIsMaster = (strNodeLine.find("master") != std::string::npos);
    std::vector<std::string> vecNodeField = CFlyRedis::SplitString(strNodeLine, ' ');
    if ((bIsMaster && vecNodeField.size() != 9) || (!bIsMaster && vecNodeField.size() != 8))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "NodeFieldInvalid: [%s]", strNodeLine.c_str());
        return false;
    }
    strNodeId = vecNodeField[0];
    strNodeIPPort = vecNodeField[1];
    size_t nPos = strNodeIPPort.find('@');
    if (nPos != std::string::npos)
    {
        strNodeIPPort.erase(nPos, strNodeIPPort.length());
    }
    if (bIsMaster)
    {
        const std::string& strSlotRange = vecNodeField[8];
        std::vector<std::string> vecIntField = CFlyRedis::SplitString(strSlotRange, '-');
        if (vecIntField.size() != 2)
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "InvalidFieldLen: [%s] Slot: [%s]", strNodeIPPort.c_str(), strSlotRange.c_str());
            return false;
        }
        nMinSlot = atoi(vecIntField[0].c_str());
        nMaxSlot = atoi(vecIntField[1].c_str());
        if (nMinSlot > nMaxSlot)
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "InvalidSlotValue: [%s] Slot: [%s]", strNodeIPPort.c_str(), strSlotRange.c_str());
            return false;
        }
    }
    else
    {
        strMasterNodeId = vecNodeField[3];
    }
    return true;
}
