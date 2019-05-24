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
* FileName: FlyRedisSession.cpp
*
* Purpose:  Define CFlyRedisSession, hold TCP session to redis session
*
* Author:   Jhon Frank(icerlion@163.com)
*
* Modify:   2019/5/23 15:24
===================================================================+*/
#include "FlyRedisSession.h"
#include "FlyRedisClient.h"
#include "FlyRedis.h"


CFlyRedisSession::CFlyRedisSession()
    :m_strRedisAddress(),
    m_nMinSlot(0),
    m_nMaxSlot(0),
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

const std::string& CFlyRedisSession::GetRedisAddr() const
{
    return m_strRedisAddress;
}

bool CFlyRedisSession::Connect()
{
    std::vector<std::string> vAddressField = CFlyRedis::SplitString(m_strRedisAddress, ':');
    if (vAddressField.size() != 2)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ParseIPFailed, [%s] Invalid AddressFormat", m_strRedisAddress.c_str());
        return false;
    }
    const std::string& strIP = vAddressField[0];
    int nPort = atoi(vAddressField[1].c_str());
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

bool CFlyRedisSession::SetSelfSlotRange(const std::string& strSlotRange)
{
    std::vector<std::string> vIntField = CFlyRedis::SplitString(strSlotRange, '-');
    if (vIntField.size() != 2)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "InvalidFieldLen: [%s] Slot: [%s]", m_strRedisAddress.c_str(), strSlotRange.c_str());
        return false;
    }
    m_nMinSlot = atoi(vIntField[0].c_str());
    m_nMaxSlot = atoi(vIntField[1].c_str());
    return m_nMaxSlot >= m_nMinSlot;
}

bool CFlyRedisSession::ProcRedisRequest(const std::string& strRedisCmdRequest, std::vector<std::string>& vRedisResponseLine)
{
    // Build RedisCmdRequest String
    vRedisResponseLine.clear();
    m_bRedisResponseError = false;
    // Send Msg To RedisServer
    m_boostTCPIOStream.expires_after(std::chrono::seconds(CFlyRedis::GetRedisReadTimeOutSeconds()));
    m_boostTCPIOStream.write(strRedisCmdRequest.c_str(), strRedisCmdRequest.length());
    if (!RecvRedisResponse(vRedisResponseLine))
    {
        return false;
    }
    return !m_bRedisResponseError;
}

bool CFlyRedisSession::AUTH(std::string& strPassword)
{
    std::vector<std::string> vRedisCmdParamList;
    vRedisCmdParamList.push_back("AUTH");
    vRedisCmdParamList.push_back(strPassword);
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(m_strRedisAddress, vRedisCmdParamList, strRedisCmdRequest);
    std::vector<std::string> vRedisResponseLine;
    if (!ProcRedisRequest(strRedisCmdRequest, vRedisResponseLine))
    {
        return false;
    }
    if (1 != vRedisResponseLine.size())
    {
        return false;
    }
    const std::string& strResponse = vRedisResponseLine[0];
    return strResponse.compare("OK") == 0;
}

bool CFlyRedisSession::PING()
{
    std::vector<std::string> vRedisCmdParamList;
    vRedisCmdParamList.push_back("PING");
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(m_strRedisAddress, vRedisCmdParamList, strRedisCmdRequest);
    std::vector<std::string> vRedisResponseLine;
    if (!ProcRedisRequest(strRedisCmdRequest, vRedisResponseLine))
    {
        return false;
    }
    if (1 != vRedisResponseLine.size())
    {
        return false;
    }
    const std::string& strResponse = vRedisResponseLine[0];
    return strResponse.compare("PONG") == 0;
}

bool CFlyRedisSession::INFO_CLUSTER(bool& bClusterEnable)
{
    std::vector<std::string> vRedisCmdParamList;
    vRedisCmdParamList.push_back("INFO");
    vRedisCmdParamList.push_back("CLUSTER");
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(m_strRedisAddress, vRedisCmdParamList, strRedisCmdRequest);
    std::vector<std::string> vRedisResponseLine;
    if (!ProcRedisRequest(strRedisCmdRequest, vRedisResponseLine))
    {
        return false;
    }
    if (1 != vRedisResponseLine.size())
    {
        return false;
    }
    const std::string& strResponse = vRedisResponseLine[0];
    if (strResponse.find("cluster_enabled") == std::string::npos)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineLastLineInvalid: [%s]", strResponse.c_str());
        return false;
    }
    bClusterEnable = (strResponse.find('1') != std::string::npos);
    return true;
}

bool CFlyRedisSession::CLUSTER_NODES(std::vector<std::string>& vResult)
{
    std::vector<std::string> vRedisCmdParamList;
    vRedisCmdParamList.push_back("CLUSTER");
    vRedisCmdParamList.push_back("NODES");
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(m_strRedisAddress, vRedisCmdParamList, strRedisCmdRequest);
    std::vector<std::string> vRedisResponseLine;
    if (!ProcRedisRequest(strRedisCmdRequest, vRedisResponseLine))
    {
        return false;
    }
    if (1 != vRedisResponseLine.size())
    {
        return false;
    }
    vResult = CFlyRedis::SplitString(vRedisResponseLine[0], '\n');
    return true;
}

bool CFlyRedisSession::SCRIPT_LOAD(const std::string& strScript, std::string& strResult)
{
    std::vector<std::string> vRedisCmdParamList;
    vRedisCmdParamList.push_back("SCRIPT");
    vRedisCmdParamList.push_back("LOAD");
    vRedisCmdParamList.push_back(strScript);
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(m_strRedisAddress, vRedisCmdParamList, strRedisCmdRequest);
    std::vector<std::string> vRedisResponseLine;
    if (!ProcRedisRequest(strRedisCmdRequest, vRedisResponseLine))
    {
        return false;
    }
    if (1 != vRedisResponseLine.size())
    {
        return false;
    }
    strResult = vRedisResponseLine[0];
    return true;
}

bool CFlyRedisSession::RecvRedisResponse(std::vector<std::string>& vRedisResponseLine)
{
    char chHead = 0;
    m_boostTCPIOStream.expires_after(std::chrono::seconds(CFlyRedis::GetRedisReadTimeOutSeconds()));
    m_boostTCPIOStream.read(&chHead, 1);
    bool bResult = false;
    switch (chHead)
    {
    case '-': // Errors 
        bResult = ReadRedisResponseError(vRedisResponseLine);
        break;
    case '+': // Simple Strings
        bResult = ReadRedisResponseSimpleStrings(vRedisResponseLine);
        break;
    case ':': // Integers 
        bResult = ReadRedisResponseIntegers(vRedisResponseLine);
        break;
    case '$': // Bulk Strings
        bResult = ReadRedisResponseBulkStrings(vRedisResponseLine);
        break;
    case '*': // Array
        bResult = ReadRedisResponseArrays(vRedisResponseLine);
        break;
    default:
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "Unknown HeadCharacter, [%s], Char: [%s]", m_strRedisAddress.c_str(), std::to_string(chHead).c_str());
        break;
    }
    return bResult;

}

bool CFlyRedisSession::ReadRedisResponseError(std::vector<std::string>& vRedisResponseLine)
{
    std::string strLine;
    if (!ReadUntilCRLF(strLine))
    {
        return false;
    }
    vRedisResponseLine.push_back(strLine);
    CFlyRedis::Logger(FlyRedisLogLevel::Debug, "RedisResponseError: [%s]", strLine.c_str());
    m_bRedisResponseError = true;
    return true;
}

bool CFlyRedisSession::ReadRedisResponseSimpleStrings(std::vector<std::string>& vRedisResponseLine)
{
    std::string strLine;
    if (!ReadUntilCRLF(strLine))
    {
        return false;
    }
    vRedisResponseLine.push_back(strLine);
    return true;
}

bool CFlyRedisSession::ReadRedisResponseIntegers(std::vector<std::string>& vRedisResponseLine)
{
    std::string strLine;
    if (!ReadUntilCRLF(strLine))
    {
        return false;
    }
    vRedisResponseLine.push_back(strLine);
    return true;
}

bool CFlyRedisSession::ReadRedisResponseBulkStrings(std::vector<std::string>& vRedisResponseLine)
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
        vRedisResponseLine.push_back(""); // HGET maybe return Empty String
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
            m_boostTCPIOStream.expires_after(std::chrono::seconds(CFlyRedis::GetRedisReadTimeOutSeconds()));
            m_boostTCPIOStream.read(m_buffBulkStrings, nLen);
            strBulkStrings.append(m_buffBulkStrings, nLen);
            break;
        }
        else
        {
            m_boostTCPIOStream.expires_after(std::chrono::seconds(CFlyRedis::GetRedisReadTimeOutSeconds()));
            m_boostTCPIOStream.read(m_buffBulkStrings, CONST_BUFF_BULK_STRINGS_ONCE_LEN);
            strBulkStrings.append(m_buffBulkStrings, CONST_BUFF_BULK_STRINGS_ONCE_LEN);
            nLen -= CONST_BUFF_BULK_STRINGS_ONCE_LEN;
        }
    }
    // Read tail CRLF to make stream empty
    m_boostTCPIOStream.expires_after(std::chrono::seconds(CFlyRedis::GetRedisReadTimeOutSeconds()));
    m_boostTCPIOStream.read(m_buffBulkStrings, 2);
    vRedisResponseLine.push_back(strBulkStrings);
    return true;
}

bool CFlyRedisSession::ReadRedisResponseArrays(std::vector<std::string>& vRedisResponseLine)
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
        RecvRedisResponse(vRedisResponseLine);
    }
    return true;
}

bool CFlyRedisSession::ReadUntilCRLF(std::string& strLine)
{
    char chPreValue = 0;
    while (true)
    {
        char chCurValue = 0;
        m_boostTCPIOStream.expires_after(std::chrono::seconds(CFlyRedis::GetRedisReadTimeOutSeconds()));
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
