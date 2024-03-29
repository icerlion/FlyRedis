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
#include "boost/thread.hpp"
#include <stdarg.h>

//////////////////////////////////////////////////////////////////////////
// Begin of CFlyRedisNetStream
#ifdef FLY_REDIS_ENABLE_TLS
CFlyRedisNetStream::CFlyRedisNetStream(boost::asio::io_context& boostIOContext, bool bUseTLSFlag, boost::asio::ssl::context& boostTLSContext)
    :m_boostIOContext(boostIOContext),
    m_bUseTLSFlag(bUseTLSFlag),
    m_boostTLSSocketStream(boostIOContext, boostTLSContext),
    m_boostTCPSocketStream(boostIOContext)
{
}
#else
CFlyRedisNetStream::CFlyRedisNetStream(boost::asio::io_context& boostIOContext)
    :m_boostIOContext(boostIOContext),
    m_boostTCPSocketStream(boostIOContext)
{
}
#endif // FLY_REDIS_ENABLE_TLS

CFlyRedisNetStream::~CFlyRedisNetStream()
{
}

bool CFlyRedisNetStream::Connect()
{
    std::vector<std::string> vecField = CFlyRedis::SplitString(m_strRedisAddress, ':');
    if (vecField.size() != 2)
    {
        return false;
    }
    boost::asio::ip::tcp::resolver boostResolver(m_boostIOContext);
    boost::asio::ip::tcp::resolver::results_type boostEndPoints = boostResolver.resolve(boost::asio::ip::tcp::v4(), vecField[0], vecField[1]);
#ifdef FLY_REDIS_ENABLE_TLS
    if (m_bUseTLSFlag)
    {
        return ConnectAsTLS(boostEndPoints);
    }
#endif // FLY_REDIS_ENABLE_TLS
    return ConnectAsTCP(boostEndPoints);
}

bool CFlyRedisNetStream::ReadByLength(int nExpectedLen)
{
    if ((int)m_strGlobalRecvBuff.length() >= nExpectedLen)
    {
        return true;
    }
    time_t nExpiredTime = time(nullptr) + m_nReadTimeoutSeconds;
    while (time(nullptr) < nExpiredTime)
    {
        m_boostIOContext.restart();
        StartAsyncRead();
        m_boostIOContext.run_for(std::chrono::milliseconds(50));
        if ((int)m_strGlobalRecvBuff.length() >= nExpectedLen)
        {
            break;
        }
    }
    if ((int)m_strGlobalRecvBuff.size() < nExpectedLen)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "Read Data From Redis Timeout");
        return false;
    }
    return true;
}

bool CFlyRedisNetStream::ReadByTime(int nBlockMS)
{
    m_boostIOContext.restart();
    StartAsyncRead();
    m_boostIOContext.run_for(std::chrono::milliseconds(nBlockMS));
    return !m_strGlobalRecvBuff.empty();
}

bool CFlyRedisNetStream::Write(const char* buffWrite, size_t nBuffLen)
{
    boost::system::error_code boostErrorCode;
    size_t nSendBytes = 0;
#ifdef FLY_REDIS_ENABLE_TLS
    if (m_bUseTLSFlag)
    {
        nSendBytes = boost::asio::write(m_boostTLSSocketStream, boost::asio::buffer(buffWrite, nBuffLen), boostErrorCode);
    }
    else
#endif // FLY_REDIS_ENABLE_TLS
    {
        nSendBytes = boost::asio::write(m_boostTCPSocketStream, boost::asio::buffer(buffWrite, nBuffLen), boostErrorCode);
    }

    if (boostErrorCode)
    {
        return false;
    }
    if (nBuffLen != nSendBytes)
    {
        return false;
    }
    return true;
}

#ifdef FLY_REDIS_ENABLE_TLS
bool CFlyRedisNetStream::ConnectAsTLS(boost::asio::ip::tcp::resolver::results_type& boostEndPoints)
{
    boost::system::error_code boostErrorCode;
    boost::asio::connect(m_boostTLSSocketStream.lowest_layer(), boostEndPoints, boostErrorCode);
    if (boostErrorCode)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "TLS Connect %s Failed As %s", m_strRedisAddress.c_str(), boostErrorCode.message().c_str());
        return false;
    }
    m_strLocalIP = m_boostTLSSocketStream.lowest_layer().local_endpoint(boostErrorCode).to_string();
    m_boostTLSSocketStream.handshake(boost::asio::ssl::stream_base::client, boostErrorCode);
    if (boostErrorCode)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "TLS HandShake %s Failed As %s", m_strRedisAddress.c_str(), boostErrorCode.message().c_str());
        return false;
    }
    auto& refLoewstLayer = m_boostTLSSocketStream.lowest_layer();
    refLoewstLayer.set_option(boost::asio::ip::tcp::socket::keep_alive());
    // Try to recv first error msg after connect to redis-server
    bool bResult = true;
    int nTryCount = 0;
    char buffRecv[1024] = { 0 };
    while (++nTryCount < 10)
    {
        if (refLoewstLayer.available(boostErrorCode) > 0)
        {
            // If run to here, maybe the server is running in protected mode, which will response an error msg, just print it as error log and return false
            m_boostTLSSocketStream.read_some(boost::asio::buffer(buffRecv, sizeof(buffRecv)), boostErrorCode);
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "TLS ConnectEndPointFailed %s As %s", m_strRedisAddress.c_str(), buffRecv);
            bResult = false;
            break;
        }
        boost::this_thread::sleep_for(boost::chrono::milliseconds(1));
    }
    return bResult;
}
#endif // FLY_REDIS_ENABLE_TLS

bool CFlyRedisNetStream::ConnectAsTCP(boost::asio::ip::tcp::resolver::results_type& boostEndPoints)
{
    boost::system::error_code boostErrorCode;
    boost::asio::connect(m_boostTCPSocketStream, boostEndPoints, boostErrorCode);
    if (boostErrorCode)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "TCP Connect %s Failed %s", m_strRedisAddress.c_str(), boostErrorCode.message().c_str());
        return false;
    }
    m_strLocalIP = m_boostTCPSocketStream.local_endpoint(boostErrorCode).address().to_string(boostErrorCode);
    m_boostTCPSocketStream.set_option(boost::asio::ip::tcp::socket::keep_alive());
    // Try to recv first error msg after connect to redis-server
    bool bResult = true;
    int nTryCount = 0;
    char buffRecv[1024] = { 0 };
    while (++nTryCount < 10)
    {
        if (m_boostTCPSocketStream.available(boostErrorCode) > 0)
        {
            // If run to here, maybe the server is running in protected mode, which will response an error msg, just print it as error log and return false
            m_boostTCPSocketStream.read_some(boost::asio::buffer(buffRecv, sizeof(buffRecv)));
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "TCP ConnectEndPointFailed: %s As %s", m_strRedisAddress.c_str(), buffRecv);
            bResult = false;
            break;
        }
        boost::this_thread::sleep_for(boost::chrono::milliseconds(1));
    }
    return bResult;
}

void CFlyRedisNetStream::HandleRead(const boost::system::error_code& boostErrorCode, size_t nBytesTransferred)
{
    //std::string strReadBuff;
    //strReadBuff.append(m_caThisbuffRecv, nBytesTransferred);
    m_bInAsyncRead = false;
    if (boostErrorCode)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "HandleRead Error, Msg %s, Address %s", boostErrorCode.message().c_str(), m_strRedisAddress.c_str());
        return;
    }
    if (0 == nBytesTransferred)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "HandleRead Error, ByteTransfer Is 0, Address %s", m_strRedisAddress.c_str());
        return;
    }
    m_strGlobalRecvBuff.append(m_caThisbuffRecv, nBytesTransferred);
}

void CFlyRedisNetStream::StartAsyncRead()
{
    if (m_bInAsyncRead)
    {
        return;
    }
    m_bInAsyncRead = true;
#ifdef FLY_REDIS_ENABLE_TLS
    if (m_bUseTLSFlag)
    {
        m_boostTLSSocketStream.async_read_some(boost::asio::buffer(m_caThisbuffRecv, sizeof(m_caThisbuffRecv)),
            boost::bind(&CFlyRedisNetStream::HandleRead,
                this,
                boost::asio::placeholders::error,
                boost::asio::placeholders::bytes_transferred));
    }
    else
#endif // FLY_REDIS_ENABLE_TLS
    {
        m_boostTCPSocketStream.async_read_some(boost::asio::buffer(m_caThisbuffRecv, sizeof(m_caThisbuffRecv)),
            boost::bind(&CFlyRedisNetStream::HandleRead,
                this,
                boost::asio::placeholders::error,
                boost::asio::placeholders::bytes_transferred));
    }
}

// End of CFlyRedisNetStream
//////////////////////////////////////////////////////////////////////////
// Begin of RedisSession function
#ifdef FLY_REDIS_ENABLE_TLS
CFlyRedisSession::CFlyRedisSession(boost::asio::io_context& boostIOContext, bool bUseTLSFlag, boost::asio::ssl::context& boostTLSContext)
    :m_hNetStream(boostIOContext, bUseTLSFlag, boostTLSContext)
{
}
#else
CFlyRedisSession::CFlyRedisSession(boost::asio::io_context& boostIOContext)
    :m_hNetStream(boostIOContext)
{
}
#endif // FLY_REDIS_ENABLE_TLS

CFlyRedisSession::~CFlyRedisSession()
{
}

void CFlyRedisSession::SetRedisAddress(const std::string& strAddress)
{
    m_hNetStream.SetRedisAddress(strAddress);
}

const std::string& CFlyRedisSession::GetRedisAddr() const
{
    return m_hNetStream.GetRedisAddr();
}

bool CFlyRedisSession::Connect()
{
    return m_hNetStream.Connect();
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

bool CFlyRedisSession::ProcRedisRequest(const std::string& strRedisCmdRequest)
{
    // Build RedisCmdRequest String
    m_stRedisResponse.Reset();
    m_bRedisResponseError = false;
    // Send Msg To RedisServer
    m_hNetStream.Write(strRedisCmdRequest.c_str(), strRedisCmdRequest.length());
    if (!RecvRedisResponse())
    {
        return false;
    }
    return !m_bRedisResponseError;
}

bool CFlyRedisSession::TrySendRedisRequest(const std::string& strRedisCmdRequest)
{
    // Build RedisCmdRequest String
    m_stRedisResponse.Reset();
    m_bRedisResponseError = false;
    // Send Msg To RedisServer
    m_hNetStream.Write(strRedisCmdRequest.c_str(), strRedisCmdRequest.length());
    return true;
}

bool CFlyRedisSession::TryRecvRedisResponse(int nBlockMS)
{
    if (!m_hNetStream.ReadByTime(nBlockMS))
    {
        return false;
    }
    bool bResult = true;
    while (m_hNetStream.GlobalRecvBuffLen() > 0)
    {
        if (!RecvRedisResponse())
        {
            bResult = false;
            break;
        }
    }
    return bResult && !m_bRedisResponseError;
}

bool CFlyRedisSession::ResolveServerVersion()
{
    // Check server version, update RESP version for Redis 6.*
    std::map<std::string, std::map<std::string, std::string> > mapSectionInfo;
    if (!INFO("Server", mapSectionInfo))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "RedisNode %s Run INFO SERVER failed", GetRedisAddr().c_str());
        return false;
    }
    m_strRedisVersion = GetServerInfoSectionField(mapSectionInfo, "# Server", "redis_version");
    return !m_strRedisVersion.empty();
}

bool CFlyRedisSession::GetClusterEnabledFlag()
{
    std::map<std::string, std::map<std::string, std::string> > mapSectionInfo;
    if (!INFO("Cluster", mapSectionInfo))
    {
        return false;
    }
    std::string strClusterEnabled = GetServerInfoSectionField(mapSectionInfo, "# Cluster", "cluster_enabled");
    return (0 == strClusterEnabled.compare("1"));
}

std::string CFlyRedisSession::GetLastFullResponseString() const
{
    std::string strResult;
    if (!m_stRedisResponse.strRedisResponse.empty())
    {
        strResult.append("Str:").append(m_stRedisResponse.strRedisResponse);
    }
    if (!m_stRedisResponse.vecRedisResponse.empty())
    {
        strResult.append(";Vec:");
        for (auto& strVal : m_stRedisResponse.vecRedisResponse)
        {
            strResult.append(strVal).append(",");
        }
    }
    if (!m_stRedisResponse.setRedisResponse.empty())
    {
        strResult.append(";Set:");
        for (auto& strVal : m_stRedisResponse.setRedisResponse)
        {
            strResult.append(strVal).append(",");
        }
    }
    if (!m_stRedisResponse.mapRedisResponse.empty())
    {
        strResult.append(";Map:");
        for (auto& kvp : m_stRedisResponse.mapRedisResponse)
        {
            strResult.append(kvp.first).append("=").append(kvp.second).append(",");
        }
    }
    return strResult;
}

bool CFlyRedisSession::AUTH(const std::string& strPassword)
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.emplace_back("AUTH");
    vecRedisCmdParamList.emplace_back(strPassword);
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(GetRedisAddr(), vecRedisCmdParamList, strRedisCmdRequest, false);
    if (!ProcRedisRequest(strRedisCmdRequest))
    {
        return false;
    }
    return m_stRedisResponse.strRedisResponse.compare("OK") == 0;
}

bool CFlyRedisSession::PING()
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.emplace_back("PING");
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(GetRedisAddr(), vecRedisCmdParamList, strRedisCmdRequest, false);
    if (!ProcRedisRequest(strRedisCmdRequest))
    {
        return false;
    }
    return m_stRedisResponse.strRedisResponse.compare("PONG") == 0;
}

bool CFlyRedisSession::READONLY()
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.emplace_back("READONLY");
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(GetRedisAddr(), vecRedisCmdParamList, strRedisCmdRequest, false);
    if (!ProcRedisRequest(strRedisCmdRequest))
    {
        return false;
    }
    return m_stRedisResponse.strRedisResponse.compare("OK") == 0;
}

bool CFlyRedisSession::INFO(const std::string& strSection, std::map<std::string, std::map<std::string, std::string> >& mapSectionInfo)
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.emplace_back("INFO");
    if (!strSection.empty())
    {
        vecRedisCmdParamList.emplace_back(strSection);
    }
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(GetRedisAddr(), vecRedisCmdParamList, strRedisCmdRequest, false);
    if (!ProcRedisRequest(strRedisCmdRequest))
    {
        return false;
    }
    // Parse info section key-value field
    size_t nLineLen = m_stRedisResponse.strRedisResponse.length();
    std::string strLine;
    std::string strCurSection;
    std::string strCurKey;
    std::string strCurValue;
    for (size_t nIndex = 0; nIndex < nLineLen; ++nIndex)
    {
        char chCur = m_stRedisResponse.strRedisResponse[nIndex];
        strLine.append(1, chCur);
        if ('\n' == chCur)
        {
            if ('#' == strLine[0])
            {
                // Parse section name
                strCurSection.swap(TrimLastChar(strLine, 2));
                strLine.clear();
            }
            else
            {
                strCurValue.swap(TrimLastChar(strLine, 2));
                strLine.clear();
                auto itFindSection = mapSectionInfo.find(strCurSection);
                if (itFindSection == mapSectionInfo.end())
                {
                    std::map<std::string, std::string> mapKVP;
                    mapKVP.emplace(strCurKey, strCurValue);
                    mapSectionInfo.emplace(strCurSection, mapKVP);
                }
                else
                {
                    std::map<std::string, std::string>& mapKVP = itFindSection->second;
                    mapKVP.emplace(strCurKey, strCurValue);
                }
            }
        }
        else if (':' == chCur)
        {
            strCurKey.swap(TrimLastChar(strLine, 1));
            strLine.clear();
        }
    }
    return true;
}

bool CFlyRedisSession::CLUSTER_NODES(std::vector<std::string>& vecResult)
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.emplace_back("CLUSTER");
    vecRedisCmdParamList.emplace_back("NODES");
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(GetRedisAddr(), vecRedisCmdParamList, strRedisCmdRequest, false);
    if (!ProcRedisRequest(strRedisCmdRequest))
    {
        return false;
    }
    vecResult = CFlyRedis::SplitString(m_stRedisResponse.strRedisResponse, '\n');
    return true;
}

bool CFlyRedisSession::SCRIPT_LOAD(const std::string& strScript, std::string& strResult)
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.emplace_back("SCRIPT");
    vecRedisCmdParamList.emplace_back("LOAD");
    vecRedisCmdParamList.emplace_back(strScript);
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(GetRedisAddr(), vecRedisCmdParamList, strRedisCmdRequest, false);
    if (!ProcRedisRequest(strRedisCmdRequest))
    {
        return false;
    }
    strResult.swap(m_stRedisResponse.strRedisResponse);
    return true;
}

bool CFlyRedisSession::SCRIPT_FLUSH()
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.emplace_back("SCRIPT");
    vecRedisCmdParamList.emplace_back("FLUSH");
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(GetRedisAddr(), vecRedisCmdParamList, strRedisCmdRequest, true);
    if (!ProcRedisRequest(strRedisCmdRequest))
    {
        return false;
    }
    return m_stRedisResponse.strRedisResponse.compare("OK") == 0;
}

bool CFlyRedisSession::SCRIPT_EXISTS(const std::string& strSHA)
{
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.emplace_back("SCRIPT");
    vecRedisCmdParamList.emplace_back("EXISTS");
    vecRedisCmdParamList.emplace_back(strSHA);
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(GetRedisAddr(), vecRedisCmdParamList, strRedisCmdRequest, false);
    if (!ProcRedisRequest(strRedisCmdRequest))
    {
        return false;
    }
    return m_stRedisResponse.strRedisResponse.compare("1") == 0;
}

bool CFlyRedisSession::HELLO(int nVersion)
{
    if (!VerifyRedisServerVersion6("HELLO"))
    {
        return false;
    }
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.emplace_back("HELLO");
    vecRedisCmdParamList.emplace_back(std::to_string(nVersion));
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(GetRedisAddr(), vecRedisCmdParamList, strRedisCmdRequest, false);
    if (!ProcRedisRequest(strRedisCmdRequest))
    {
        return false;
    }
    m_nRESPVersion = nVersion;
    return true;
}

bool CFlyRedisSession::HELLO_AUTH_SETNAME(int nVersion, const std::string& strUserName, const std::string& strPassword, const std::string& strClientName)
{
    if (!VerifyRedisServerVersion6("HELLO"))
    {
        return false;
    }
    std::vector<std::string> vecRedisCmdParamList;
    vecRedisCmdParamList.emplace_back("HELLO");
    vecRedisCmdParamList.emplace_back(std::to_string(nVersion));
    if (!strUserName.empty() && !strPassword.empty())
    {
        vecRedisCmdParamList.emplace_back("AUTH");
        vecRedisCmdParamList.emplace_back(strUserName);
        vecRedisCmdParamList.emplace_back(strPassword);
    }
    if (!strClientName.empty())
    {
        vecRedisCmdParamList.emplace_back("SETNAME");
        vecRedisCmdParamList.emplace_back(strClientName);
    }
    std::string strRedisCmdRequest;
    CFlyRedis::BuildRedisCmdRequest(GetRedisAddr(), vecRedisCmdParamList, strRedisCmdRequest, false);
    if (!ProcRedisRequest(strRedisCmdRequest))
    {
        return false;
    }
    m_nRESPVersion = nVersion;
    return true;
}

bool CFlyRedisSession::RecvRedisResponse()
{
    if (!m_hNetStream.ReadByLength(1))
    {
        return false;
    }
    char chHead = 0;
    if (!m_hNetStream.PickFirstChar(chHead))
    {
        return false;
    }
    bool bResult = false;
    switch (chHead)
    {
    case '-': // Errors 
        bResult = ReadRedisResponseError();
        break;
    case '+': // Simple Strings
        bResult = ReadRedisResponseSimpleStrings();
        break;
    case ':': // Integers 
        bResult = ReadRedisResponseIntegers();
        break;
    case '$': // Bulk Strings
        bResult = ReadRedisResponseBulkStrings();
        break;
    case '*': // Array
        bResult = ReadRedisResponseArrays();
        break;
    case ',': // Double
        bResult = ReadRedisResponseDouble();
        break;
    case '_': // Null
        bResult = ReadRedisResponseNull();
        break;
    case '#': // Boolean
        bResult = ReadRedisResponseBoolean();
        break;
    case '!': // BlobError
        bResult = ReadRedisResponseBlobError();
        break;
    case '=': // VerbatimString
        bResult = ReadRedisResponseVerbatimString();
        break;
    case '(': // BigNumber
        bResult = ReadRedisResponseBigNumber();
        break;
    case '%': // Map
        bResult = ReadRedisResponseMap();
        break;
    case '~': // Set
        bResult = ReadRedisResponseSet();
        break;
    case '|': // Attribute
        bResult = ReadRedisResponseAttribute();
        break;
    default:
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "Unknown HeadCharacter, %s, Char: %s", GetRedisAddr().c_str(), std::to_string(chHead).c_str());
        break;
    }
    return bResult;
}

bool CFlyRedisSession::ReadRedisResponseError()
{
    if (!ReadUntilCRLF())
    {
        return false;
    }
    CFlyRedis::Logger(FlyRedisLogLevel::Error, "RedisResponseError %s", m_stRedisResponse.strRedisResponse.c_str());
    m_bRedisResponseError = true;
    m_strLastResponseErrorMsg = m_stRedisResponse.strRedisResponse;
    return true;
}

bool CFlyRedisSession::ReadRedisResponseSimpleStrings()
{
    if (!ReadUntilCRLF())
    {
        return false;
    }
    m_stRedisResponse.vecRedisResponse.emplace_back(m_stRedisResponse.strRedisResponse);
    return true;
}

bool CFlyRedisSession::ReadRedisResponseIntegers()
{
    if (!ReadUntilCRLF())
    {
        return false;
    }
    m_stRedisResponse.vecRedisResponse.emplace_back(m_stRedisResponse.strRedisResponse);
    return true;
}

bool CFlyRedisSession::ReadRedisResponseBulkStrings()
{
    return ReadRedisResponseVarLenString();
}

bool CFlyRedisSession::ReadRedisResponseArrays()
{
    // Read Length
    if (!ReadUntilCRLF())
    {
        return false;
    }
    int nLen = atoi(m_stRedisResponse.strRedisResponse.c_str());
    m_stRedisResponse.strRedisResponse.clear();
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
        RecvRedisResponse();
    }
    return true;
}

bool CFlyRedisSession::ReadRedisResponseMap()
{
    ReadUntilCRLF();
    std::string strKey;
    std::string strValue;
    int nKVPCount = atoi(m_stRedisResponse.strRedisResponse.c_str());
    m_stRedisResponse.strRedisResponse.clear();
    for (int nKVPIndex = 0; nKVPIndex < nKVPCount; ++nKVPIndex)
    {
        RecvRedisResponse();
        strKey.swap(m_stRedisResponse.strRedisResponse);
        RecvRedisResponse();
        strValue.swap(m_stRedisResponse.strRedisResponse);
        m_stRedisResponse.mapRedisResponse.emplace(strKey, strValue);
    }
    return true;
}

bool CFlyRedisSession::ReadRedisResponseDouble()
{
    ReadUntilCRLF();
    m_stRedisResponse.vecRedisResponse.emplace_back(m_stRedisResponse.strRedisResponse);
    return true;
}

bool CFlyRedisSession::ReadRedisResponseNull()
{
    ReadUntilCRLF();
    m_stRedisResponse.vecRedisResponse.emplace_back(m_stRedisResponse.strRedisResponse);
    return true;
}

bool CFlyRedisSession::ReadRedisResponseBoolean()
{
    ReadUntilCRLF();
    m_stRedisResponse.vecRedisResponse.emplace_back(m_stRedisResponse.strRedisResponse);
    return true;
}

bool CFlyRedisSession::ReadRedisResponseBlobError()
{
    return ReadRedisResponseVarLenString();
}

bool CFlyRedisSession::ReadRedisResponseVerbatimString()
{
    return ReadRedisResponseVarLenString();
}

bool CFlyRedisSession::ReadRedisResponseBigNumber()
{
    if (!ReadUntilCRLF())
    {
        return false;
    }
    m_stRedisResponse.vecRedisResponse.emplace_back(m_stRedisResponse.strRedisResponse);
    return true;
}

bool CFlyRedisSession::ReadRedisResponseSet()
{
    // Read Length
    if (!ReadUntilCRLF())
    {
        return false;
    }
    int nLen = atoi(m_stRedisResponse.strRedisResponse.c_str());
    m_stRedisResponse.strRedisResponse.clear();
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
        RecvRedisResponse();
        m_stRedisResponse.setRedisResponse.emplace(m_stRedisResponse.strRedisResponse);
    }
    return true;
}

bool CFlyRedisSession::ReadRedisResponseAttribute()
{
    ReadUntilCRLF();
    std::string strKey;
    std::string strValue;
    int nKVPCount = atoi(m_stRedisResponse.strRedisResponse.c_str());
    m_stRedisResponse.strRedisResponse.clear();
    for (int nKVPIndex = 0; nKVPIndex < nKVPCount; ++nKVPIndex)
    {
        RecvRedisResponse();
        strKey.swap(m_stRedisResponse.strRedisResponse);
        RecvRedisResponse();
        strValue.swap(m_stRedisResponse.strRedisResponse);
        m_stRedisResponse.mapRedisResponse.emplace(strKey, strValue);
    }
    return true;
}

bool CFlyRedisSession::VerifyRedisServerVersion6(const char* pszCmdName) const
{
    int nMainVersion = atoi(m_strRedisVersion.c_str());
    if (nMainVersion < 6)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Warning, "RedisVersion %s Not Support %s command", m_strRedisVersion.c_str(), pszCmdName);
        return false;
    }
    return true;
}

bool CFlyRedisSession::ReadUntilCRLF()
{
    m_stRedisResponse.strRedisResponse.clear();
    bool bResult = true;
    char chPreValue = 0;
    while (true)
    {
        if (!m_hNetStream.ReadByLength(1))
        {
            bResult = false;
            break;
        }
        char chCurValue = 0;
        if (!m_hNetStream.PickFirstChar(chCurValue))
        {
            bResult = false;
            break;
        }
        m_stRedisResponse.strRedisResponse.append(1, chCurValue);
        if (chPreValue == '\r' && chCurValue == '\n')
        {
            TrimLastChar(m_stRedisResponse.strRedisResponse, 2);
            break;
        }
        chPreValue = chCurValue;
    }
    return bResult;
}

bool CFlyRedisSession::ReadRedisResponseVarLenString()
{
    // Read Length
    if (!ReadUntilCRLF())
    {
        return false;
    }
    int nLen = atoi(m_stRedisResponse.strRedisResponse.c_str());
    m_stRedisResponse.strRedisResponse.clear();
    if (-1 == nLen)
    {
        m_stRedisResponse.vecRedisResponse.emplace_back(""); // HGET maybe return Empty String
        return true;
    }
    if (nLen < 0)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "Len LessThan 0: %s", m_stRedisResponse.strRedisResponse.c_str());
        return false;
    }
    // If BulkStrings over than 512M, just return false. according the Redis document, the max length should be 512M
    // Just for safe
    if (nLen >= 1024 * 1024 * 512)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "Len OverThan 512M: %s", m_stRedisResponse.strRedisResponse.c_str());
        return false;
    }
    // Length: 2 char for \r\n
    m_stRedisResponse.strRedisResponse.clear();
    if (!m_hNetStream.ReadByLength(nLen))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "NetStream Read %d Failed", nLen);
        return false;
    }
    if (!m_hNetStream.ConsumeRecvBuff(m_stRedisResponse.strRedisResponse, nLen))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "NetStream ConsumeRecvBuff %d Failed", nLen);
        return false;
    }
    // Read tail CRLF to make stream empty
    if (!m_hNetStream.ReadByLength(2))
    {
        return false;
    }
    std::string strCRLF;
    if (!m_hNetStream.ConsumeRecvBuff(strCRLF, 2))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "NetStream ConsumeRecvBuff CRLF Failed");
        return false;
    }
    m_stRedisResponse.vecRedisResponse.emplace_back(m_stRedisResponse.strRedisResponse);
    return true;
}

std::string& CFlyRedisSession::TrimLastChar(std::string& strValue, size_t nTrimCount) const
{
    size_t nLength = strValue.length();
    if (nLength >= nTrimCount)
    {
        strValue.erase(nLength - nTrimCount);
    }
    return strValue;
}

std::string CFlyRedisSession::GetServerInfoSectionField(const std::map<std::string, std::map<std::string, std::string> >& mapSectionInfo, const std::string& strSection, const std::string& strField)
{
    auto itFindSection = mapSectionInfo.find(strSection);
    if (itFindSection == mapSectionInfo.end())
    {
        return std::string();
    }
    const std::map<std::string, std::string>& mapKVP = itFindSection->second;
    auto itFindKey = mapKVP.find(strField);
    if (itFindKey == mapKVP.end())
    {
        return std::string();
    }
    return itFindKey->second;
}

// End of RedisSession function
//////////////////////////////////////////////////////////////////////////
// Begin of RedisClient
#define CHECK_CUR_REDIS_SESSION() if (nullptr == m_pCurRedisSession) { CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull"); m_bHasBadRedisSession = true; return false; }

CFlyRedisClient::CFlyRedisClient()
{
}

CFlyRedisClient::~CFlyRedisClient()
{
    Close();
}

void CFlyRedisClient::SetRedisConfig(const std::string& strHost, int nPort, const std::string& strPassword)
{
    std::string strRedisAddress = strHost;
    strRedisAddress.append(":").append(std::to_string(nPort));
    m_setRedisAddressSeed.emplace(strRedisAddress);
    m_strRedisPasswod = strPassword;
}

void CFlyRedisClient::SetReadTimeoutSeconds(int nSeconds)
{
    m_nReadTimeoutSeconds = nSeconds;
}

void CFlyRedisClient::SetRedisReadWriteType(FlyRedisReadWriteType nFlyRedisReadWriteType)
{
    m_nFlyRedisReadWriteType = nFlyRedisReadWriteType;
}

void CFlyRedisClient::SetRedisClusterDetectType(FlyRedisClusterDetectType nFlyRedisClusterDetectType)
{
    m_nFlyRedisClusterDetectType = nFlyRedisClusterDetectType;
}

bool CFlyRedisClient::SetTLSContext(const std::string& strTLSCert, const std::string& strTLSKey, const std::string& strTLSCACert)
{
    return SetTLSContext(strTLSCert, strTLSKey, strTLSCACert, "");
}

bool CFlyRedisClient::SetTLSContext(const std::string& strTLSCert, const std::string& strTLSKey, const std::string& strTLSCACert, const std::string& strTLSCACertDir)
{
#ifdef FLY_REDIS_ENABLE_TLS
    m_bUseTLSFlag = true;
    boost::system::error_code boostErrorCode;
    m_boostTLSContext.set_options(boost::asio::ssl::context::no_sslv2 | boost::asio::ssl::context::no_sslv3, boostErrorCode);
    if (boostErrorCode)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "SSL set_options failed: %s", boostErrorCode.message().c_str());
        return false;
    }
    m_boostTLSContext.set_verify_mode(boost::asio::ssl::verify_peer, boostErrorCode);
    if (boostErrorCode)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "SSL set_verify_mode failed: %s", boostErrorCode.message().c_str());
        return false;
    }
    m_boostTLSContext.load_verify_file(strTLSCACert.c_str(), boostErrorCode);
    if (boostErrorCode)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "SSL load_verify_file failed: %s", boostErrorCode.message().c_str());
        return false;
    }
    if (!strTLSCACertDir.empty())
    {
        m_boostTLSContext.add_verify_path(strTLSCACertDir.c_str(), boostErrorCode);
        if (boostErrorCode)
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "SSL add_verify_path failed: %s", boostErrorCode.message().c_str());
            return false;
        }
    }
    m_boostTLSContext.use_certificate_chain_file(strTLSCert.c_str(), boostErrorCode);
    if (boostErrorCode)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "SSL use_certificate_chain_file failed: %s", boostErrorCode.message().c_str());
        return false;
    }
    m_boostTLSContext.use_private_key_file(strTLSKey.c_str(), boost::asio::ssl::context_base::file_format::pem, boostErrorCode);
    if (boostErrorCode)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "SSL use_private_key_file failed: %s", boostErrorCode.message().c_str());
        return false;
    }
    return true;
#else
    CFlyRedis::Logger(FlyRedisLogLevel::Error, "TLS Is Disable, %s, %s, %s", strTLSCert.c_str(), strTLSKey.c_str(), strTLSCACert.c_str(), strTLSCACertDir.c_str());
    return false;
#endif // FLY_REDIS_ENABLE_TLS
}

std::string CFlyRedisClient::GetLocalIP() const
{
    if (nullptr != m_pCurRedisSession)
    {
        return m_pCurRedisSession->GetLocalIP();
    }
    std::string strResult = "";
    for (auto& kvp : m_mapRedisSession)
    {
        auto& pRedisSession = kvp.second;
        if (nullptr != pRedisSession)
        {
            strResult = pRedisSession->GetLocalIP();
            break;
        }
    }
    return strResult;
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
    bool bResult = true;
    switch (m_nFlyRedisClusterDetectType)
    {
    case FlyRedisClusterDetectType::AutoDetect:
        m_bClusterFlag = m_pCurRedisSession->GetClusterEnabledFlag();
        break;
    case FlyRedisClusterDetectType::EnableCluster:
        m_bClusterFlag = true;
        break;
    case FlyRedisClusterDetectType::DisableCluster:
        m_bClusterFlag = false;
        break;
    default:
        break;
    }
    if (!bResult)
    {
        return false;
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
    std::map<std::string, CFlyRedisSession*> mapRedisSessionCopy = m_mapRedisSession;
    for (auto& kvp : mapRedisSessionCopy)
    {
        DestroyRedisSession(kvp.second);
    }
    m_pCurRedisSession = nullptr;
    m_mapRedisSession.clear();
    m_nRedisNodeCount = 0;
    m_bHasBadRedisSession = false;
}

void CFlyRedisClient::FetchRedisNodeList(std::vector<std::string>& vecRedisNodeList) const
{
    vecRedisNodeList.reserve(m_mapRedisSession.size());
    for (auto& kvp : m_mapRedisSession)
    {
        vecRedisNodeList.emplace_back(kvp.first);
    }
}

std::vector<std::string> CFlyRedisClient::FetchRedisNodeList() const
{
    std::vector<std::string> vecRedisNodeList;
    FetchRedisNodeList(vecRedisNodeList);
    return vecRedisNodeList;
}

bool CFlyRedisClient::ChooseCurRedisNode(const std::string& strNodeAddr)
{
    auto itFind = m_mapRedisSession.find(strNodeAddr);
    if (itFind == m_mapRedisSession.end())
    {
        return false;
    }
    m_pCurRedisSession = itFind->second;
    return true;
}

void CFlyRedisClient::HELLO(int nRESPVersion)
{
    for (auto& kvp : m_mapRedisSession)
    {
        CFlyRedisSession* pFlyRedisSession = kvp.second;
        if (nullptr != pFlyRedisSession)
        {
            pFlyRedisSession->HELLO(nRESPVersion);
        }
    }
}

bool CFlyRedisClient::HELLO_AUTH_SETNAME(int nRESPVersion, const std::string& strUserName, const std::string& strPassword, const std::string& strClientName)
{
    bool bResult = true;
    for (auto& kvp : m_mapRedisSession)
    {
        CFlyRedisSession* pFlyRedisSession = kvp.second;
        if (nullptr != pFlyRedisSession)
        {
            if (!pFlyRedisSession->HELLO_AUTH_SETNAME(nRESPVersion, strUserName, strPassword, strClientName))
            {
                bResult = false;
                break;
            }
        }
    }
    return bResult;
}

bool CFlyRedisClient::PING(const std::string& strMsg, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PING");
    if (!strMsg.empty())
    {
        m_vecRedisCmdParamList.emplace_back(strMsg);
    }
    return RunRedisCmdOnOneLineResponseString("", false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::ACL_CAT(std::vector<std::string>& vecResult)
{
    return CFlyRedisClient::ACL_CAT("", vecResult);
}

bool CFlyRedisClient::ACL_CAT(const std::string& strParam, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("CAT");
    if (!strParam.empty())
    {
        m_vecRedisCmdParamList.emplace_back(strParam);
    }
    return RunRedisCmdOnOneLineResponseVector("", false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ACL_DELUSER(const std::string& strUserName, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("DELUSER");
    m_vecRedisCmdParamList.emplace_back(strUserName);
    return RunRedisCmdOnOneLineResponseInt("", true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ACL_DELUSER(const std::vector<std::string>& vecUserName, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("DELUSER");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecUserName.begin(), vecUserName.end());
    return RunRedisCmdOnOneLineResponseInt("", true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ACL_GENPASS(std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("GENPASS");
    return RunRedisCmdOnOneLineResponseString("", false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::ACL_GENPASS(int nBits, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("GENPASS");
    m_vecRedisCmdParamList.emplace_back(std::to_string(nBits));
    return RunRedisCmdOnOneLineResponseString("", false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::ACL_GETUSER(const std::string& strUserName, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("GETUSER");
    m_vecRedisCmdParamList.emplace_back(strUserName);
    return RunRedisCmdOnOneLineResponseVector("", false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ACL_HELP(std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("HELP");
    return RunRedisCmdOnOneLineResponseVector("", false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ACL_LIST(std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("LIST");
    return RunRedisCmdOnOneLineResponseVector("", false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ACL_LOAD()
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("LOAD");
    std::string strResult;
    if (!RunRedisCmdOnOneLineResponseString("", true, strResult, __FUNCTION__))
    {
        return false;
    }
    return 0 == strResult.compare("OK");
}

bool CFlyRedisClient::ACL_LOG(std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("LOG");
    return RunRedisCmdOnOneLineResponseVector("", false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ACL_SAVE()
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("SAVE");
    std::string strResult;
    if (!RunRedisCmdOnOneLineResponseString("", true, strResult, __FUNCTION__))
    {
        return false;
    }
    return 0 == strResult.compare("OK");
}

bool CFlyRedisClient::ACL_SETUSER(const std::string& strUserName, const std::string& strRules, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("SETUSER");
    m_vecRedisCmdParamList.emplace_back(strUserName);
    std::string strCurField;
    for (char ch : strRules)
    {
        if (ch == ' ')
        {
            m_vecRedisCmdParamList.emplace_back(strCurField);
            strCurField.clear();
            continue;
        }
        strCurField.append(1, ch);
    }
    if (!strCurField.empty())
    {
        m_vecRedisCmdParamList.emplace_back(strCurField);
    }
    if (!RunRedisCmdOnOneLineResponseString("", true, strResult, __FUNCTION__))
    {
        return false;
    }
    return 0 == strResult.compare("OK");
}

bool CFlyRedisClient::ACL_USERS(std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("USERS");
    return RunRedisCmdOnOneLineResponseVector("", false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ACL_WHOAMI(std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ACL");
    m_vecRedisCmdParamList.emplace_back("WHOAMI");
    return RunRedisCmdOnOneLineResponseString("", false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::FLUSHALL(std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("FLUSHALL");
    return RunRedisCmdOnOneLineResponseString("", false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::LASTSAVE(int& nUTCTime)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LASTSAVE");
    return RunRedisCmdOnOneLineResponseInt("", false, nUTCTime, __FUNCTION__);
}

bool CFlyRedisClient::TIME(int& nUnixTime, int& nMicroSeconds)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("TIME");
    std::vector<std::string> vecResult;
    if (!RunRedisCmdOnOneLineResponseVector("", false, vecResult, __FUNCTION__))
    {
        return false;
    }
    if (2 != vecResult.size())
    {
        return false;
    }
    nUnixTime = atoi(vecResult[0].c_str());
    nMicroSeconds = atoi(vecResult[0].c_str());
    return true;
}

bool CFlyRedisClient::ROLE(std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LASTSAVE");
    return RunRedisCmdOnOneLineResponseVector("", false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::DBSIZE(int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("DBSIZE");
    return RunRedisCmdOnOneLineResponseInt("", false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::KEYS(const std::string& strMatchPattern, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("KEYS");
    m_vecRedisCmdParamList.emplace_back(strMatchPattern);
    return RunRedisCmdOnOneLineResponseVector("", false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SELECT(int nIndex)
{
    if (m_bClusterFlag)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "Cluster Not Support Command SELECT");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SELECT");
    m_vecRedisCmdParamList.emplace_back(std::to_string(nIndex));
    std::string strResult;
    return RunRedisCmdOnOneLineResponseString("", false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::APPEND(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("APPEND");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITCOUNT(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("BITCOUNT");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITCOUNT(const std::string& strKey, int nStart, int nEnd, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("BITCOUNT");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStart));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nEnd));
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
    m_vecRedisCmdParamList.emplace_back("BITOP");
    m_vecRedisCmdParamList.emplace_back("AND");
    m_vecRedisCmdParamList.emplace_back(strDestKey);
    m_vecRedisCmdParamList.emplace_back(strSrcKey);
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
    m_vecRedisCmdParamList.emplace_back("BITOP");
    m_vecRedisCmdParamList.emplace_back("OR");
    m_vecRedisCmdParamList.emplace_back(strDestKey);
    m_vecRedisCmdParamList.emplace_back(strSrcKey);
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
    m_vecRedisCmdParamList.emplace_back("BITOP");
    m_vecRedisCmdParamList.emplace_back("XOR");
    m_vecRedisCmdParamList.emplace_back(strDestKey);
    m_vecRedisCmdParamList.emplace_back(strSrcKey);
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
    m_vecRedisCmdParamList.emplace_back("BITOP");
    m_vecRedisCmdParamList.emplace_back("NOT");
    m_vecRedisCmdParamList.emplace_back(strDestKey);
    m_vecRedisCmdParamList.emplace_back(strSrcKey);
    return RunRedisCmdOnOneLineResponseInt(strDestKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITPOS(const std::string& strKey, int nBit, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("BITPOS");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nBit));
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BITPOS(const std::string& strKey, int nBit, int nStart, int nEnd, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("BITPOS");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nBit));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStart));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nEnd));
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::DECR(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("DECR");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::DECRBY(const std::string& strKey, int nDecrement, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("DECRBY");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nDecrement));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::GET(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("GET");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::GETBIT(const std::string& strKey, int nOffset, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("GETBIT");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nOffset));
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::GETRANGE(const std::string& strKey, int nStart, int nEnd, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("GETRANGE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStart));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nEnd));
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::GETSET(const std::string& strKey, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("GETSET");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::INCR(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("INCR");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::INCRBY(const std::string& strKey, int nIncrement, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("INCRBY");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nIncrement));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}


bool CFlyRedisClient::INCRBYFLOAT(const std::string& strKey, double fIncrement, double& fResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("INCRBYFLOAT");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(fIncrement));
    return RunRedisCmdOnOneLineResponseDouble(strKey, true, fResult, __FUNCTION__);
}

bool CFlyRedisClient::MGET(const std::vector<std::string>& vecKey, std::vector<std::string>& vecResult)
{
    if (vecKey.empty())
    {
        return false;
    }
    if (m_bClusterFlag && !CFlyRedis::IsMultiKeyOnTheSameNode(vecKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("MGET");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecKey.begin(), vecKey.end());
    if (!RunRedisCmdOnOneLineResponseVector(vecKey.front(), false, vecResult, __FUNCTION__))
    {
        return false;
    }
    return vecResult.size() == vecKey.size();
}

bool CFlyRedisClient::MSET(const std::map<std::string, std::string>& mapKeyValue)
{
    if (m_bClusterFlag && !CFlyRedis::IsMultiKeyOnTheSameNode(mapKeyValue))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    std::string strFirstKey;
    m_vecRedisCmdParamList.emplace_back("MSET");
    for (auto& kvp : mapKeyValue)
    {
        strFirstKey = kvp.first;
        m_vecRedisCmdParamList.emplace_back(kvp.first);
        m_vecRedisCmdParamList.emplace_back(kvp.second);
    }
    std::string strResult;
    return RunRedisCmdOnOneLineResponseString(strFirstKey, true, strResult, __FUNCTION__) && strResult == "OK";
}

bool CFlyRedisClient::MSETNX(const std::map<std::string, std::string>& mapKeyValue, int& nResult)
{
    if (m_bClusterFlag && !CFlyRedis::IsMultiKeyOnTheSameNode(mapKeyValue))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    std::string strFirstKey;
    m_vecRedisCmdParamList.emplace_back("MSETNX");
    for (auto& kvp : mapKeyValue)
    {
        strFirstKey = kvp.first;
        m_vecRedisCmdParamList.emplace_back(kvp.first);
        m_vecRedisCmdParamList.emplace_back(kvp.second);
    }
    return RunRedisCmdOnOneLineResponseInt(strFirstKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PSETEX(const std::string& strKey, int nTimeOutMS, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PSETEX");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nTimeOutMS));
    m_vecRedisCmdParamList.emplace_back(strValue);
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
    std::set<std::string> setSHA;
    for (auto& kvp : m_mapRedisSession)
    {
        CFlyRedisSession* pRedisSession = kvp.second;
        if (nullptr != pRedisSession)
        {
            if (!pRedisSession->SCRIPT_LOAD(strScript, strResult))
            {
                bResult = false;
            }
            setSHA.emplace(strResult);
            if (setSHA.size() != 1)
            {
                bResult = false;
                CFlyRedis::Logger(FlyRedisLogLevel::Error, "SameLuaScriptButDiffSHA");
            }
        }
    }
    return bResult;
}

bool CFlyRedisClient::SCRIPT_FLUSH()
{
    bool bResult = true;
    for (auto& kvp : m_mapRedisSession)
    {
        CFlyRedisSession* pRedisSession = kvp.second;
        if (nullptr != pRedisSession)
        {
            if (!pRedisSession->SCRIPT_FLUSH())
            {
                bResult = false;
            }
        }
    }
    return bResult;
}

bool CFlyRedisClient::SCRIPT_EXISTS(const std::string& strSHA)
{
    bool bResult = true;
    for (auto& kvp : m_mapRedisSession)
    {
        CFlyRedisSession* pRedisSession = kvp.second;
        if (nullptr != pRedisSession)
        {
            if (!pRedisSession->SCRIPT_EXISTS(strSHA))
            {
                bResult = false;
                break;
            }
        }
    }
    return bResult;
}

bool CFlyRedisClient::EVALSHA(const std::string& strSHA, const std::vector<std::string>& vecKey, const std::vector<std::string>& vecArgv, std::string& strResult)
{
    ClearRedisCmdCache();
    if (!CFlyRedis::IsMultiKeyOnTheSameNode(vecKey))
    {
        return false;
    }
    if (vecKey.empty())
    {
        return false;
    }
    const std::string& strKeySeed = vecKey.front();
    m_vecRedisCmdParamList.emplace_back("EVALSHA");
    m_vecRedisCmdParamList.emplace_back(strSHA);
    m_vecRedisCmdParamList.emplace_back(std::to_string(vecKey.size()));
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecKey.begin(), vecKey.end());
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecArgv.begin(), vecArgv.end());
    return RunRedisCmdOnOneLineResponseString(strKeySeed, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::EVALSHA(const std::string& strSHA, const std::string& strKey, const std::string& strArgv, std::string& strResult)
{
    std::vector<std::string> vecKey;
    vecKey.emplace_back(strKey);
    std::vector<std::string> vecArgv;
    vecArgv.emplace_back(strArgv);
    return EVALSHA(strSHA, vecKey, vecArgv, strResult);
}

bool CFlyRedisClient::EVALSHA(const std::string& strSHA, const std::string& strKey, std::string& strResult)
{
    std::vector<std::string> vecKey;
    vecKey.emplace_back(strKey);
    std::vector<std::string> vecArgv;
    return EVALSHA(strSHA, vecKey, vecArgv, strResult);
}

bool CFlyRedisClient::EVALSHA(const std::string& strSHA, const std::string& strKey, const std::vector<std::string>& vecArgv, std::string& strResult)
{
    std::vector<std::string> vecKey;
    vecKey.emplace_back(strKey);
    return EVALSHA(strSHA, vecKey, vecArgv, strResult);
}

bool CFlyRedisClient::EVAL(const std::string& strScript, const std::vector<std::string>& vecKey, const std::vector<std::string>& vecArgv, std::string& strResult)
{
    ClearRedisCmdCache();
    if (!CFlyRedis::IsMultiKeyOnTheSameNode(vecKey))
    {
        return false;
    }
    if (vecKey.empty())
    {
        return false;
    }
    const std::string& strKeySeed = vecKey.front();
    m_vecRedisCmdParamList.emplace_back("EVAL");
    m_vecRedisCmdParamList.emplace_back(strScript);
    m_vecRedisCmdParamList.emplace_back(std::to_string(vecKey.size()));
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecKey.begin(), vecKey.end());
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecArgv.begin(), vecArgv.end());
    return RunRedisCmdOnOneLineResponseString(strKeySeed, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::EVAL(const std::string& strScript, const std::string& strKey, const std::string& strArgv, std::string& strResult)
{
    std::vector<std::string> vecKey;
    vecKey.emplace_back(strKey);
    std::vector<std::string> vecArgv;
    vecArgv.emplace_back(strArgv);
    return EVAL(strScript, vecKey, vecArgv, strResult);
}

bool CFlyRedisClient::EVAL(const std::string& strScript, const std::string& strKey, std::string& strResult)
{
    std::vector<std::string> vecKey;
    vecKey.emplace_back(strKey);
    std::vector<std::string> vecArgv;
    return EVAL(strScript, vecKey, vecArgv, strResult);
}

bool CFlyRedisClient::EXISTS(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("EXISTS");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::EXPIRE(const std::string& strKey, int nSeconds, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("EXPIRE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nSeconds));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::EXPIREAT(const std::string& strKey, int nTimestamp, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("EXPIREAT");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nTimestamp));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PERSIST(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PERSIST");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PEXPIRE(const std::string& strKey, int nMS, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PEXPIRE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nMS));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PEXPIREAT(const std::string& strKey, int nMS, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PEXPIREAT");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nMS));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SET(const std::string& strKey, const std::string& strValue)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SET");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strValue);
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
    m_vecRedisCmdParamList.emplace_back("SETBIT");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nOffset));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nValue));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SCAN(int nCursor, const std::string& strMatchPattern, int nCount, int& nResultCursor, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SCAN");
    m_vecRedisCmdParamList.emplace_back(std::to_string(nCursor));
    if (!strMatchPattern.empty())
    {
        m_vecRedisCmdParamList.emplace_back("MATCH");
        m_vecRedisCmdParamList.emplace_back(strMatchPattern);
    }
    if (0 != nCount)
    {
        m_vecRedisCmdParamList.emplace_back("COUNT");
        m_vecRedisCmdParamList.emplace_back(std::to_string(nCount));
    }
    return RunRedisCmdOnScanCmd("", nResultCursor, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SSCAN(const std::string& strKey, int nCursor, const std::string& strMatchPattern, int nCount, int& nResultCursor, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SSCAN");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nCursor));
    if (!strMatchPattern.empty())
    {
        m_vecRedisCmdParamList.emplace_back("MATCH");
        m_vecRedisCmdParamList.emplace_back(strMatchPattern);
    }
    if (0 != nCount)
    {
        m_vecRedisCmdParamList.emplace_back("COUNT");
        m_vecRedisCmdParamList.emplace_back(std::to_string(nCount));
    }
    return RunRedisCmdOnScanCmd(strKey, nResultCursor, vecResult, __FUNCTION__);
}


bool CFlyRedisClient::HSCAN(const std::string& strKey, int nCursor, const std::string& strMatchPattern, int nCount, int& nResultCursor, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HSCAN");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nCursor));
    if (!strMatchPattern.empty())
    {
        m_vecRedisCmdParamList.emplace_back("MATCH");
        m_vecRedisCmdParamList.emplace_back(strMatchPattern);
    }
    if (0 != nCount)
    {
        m_vecRedisCmdParamList.emplace_back("COUNT");
        m_vecRedisCmdParamList.emplace_back(std::to_string(nCount));
    }
    return RunRedisCmdOnScanCmd(strKey, nResultCursor, vecResult, __FUNCTION__);
}


bool CFlyRedisClient::ZSCAN(const std::string& strKey, int nCursor, const std::string& strMatchPattern, int nCount, int& nResultCursor, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZSCAN");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nCursor));
    if (!strMatchPattern.empty())
    {
        m_vecRedisCmdParamList.emplace_back("MATCH");
        m_vecRedisCmdParamList.emplace_back(strMatchPattern);
    }
    if (0 != nCount)
    {
        m_vecRedisCmdParamList.emplace_back("COUNT");
        m_vecRedisCmdParamList.emplace_back(std::to_string(nCount));
    }
    return RunRedisCmdOnScanCmd(strKey, nResultCursor, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::DEL(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("DEL");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::DUMP(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("DUMP");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::TTL(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("TTL");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PTTL(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PTTL");
    m_vecRedisCmdParamList.emplace_back(strKey);
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
    m_vecRedisCmdParamList.emplace_back("RENAME");
    m_vecRedisCmdParamList.emplace_back(strFromKey);
    m_vecRedisCmdParamList.emplace_back(strToKey);
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
    m_vecRedisCmdParamList.emplace_back("RENAMENX");
    m_vecRedisCmdParamList.emplace_back(strFromKey);
    m_vecRedisCmdParamList.emplace_back(strToKey);
    return RunRedisCmdOnOneLineResponseString(strFromKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::TOUCH(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("TOUCH");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::TYPE(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("TYPE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::UNLINK(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("UNLINK");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SETEX(const std::string& strKey, int nTimeOutSeconds, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SETEX");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nTimeOutSeconds));
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::SETNX(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SETNX");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SETRANGE(const std::string& strKey, int nOffset, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SETRANGE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nOffset));
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::STRLEN(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("STRLEN");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HSET(const std::string& strKey, const std::string& strField, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HSET");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strField);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HSETNX(const std::string& strKey, const std::string& strField, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HSETNX");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strField);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HSTRLEN(const std::string& strKey, const std::string& strField, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HSTRLEN");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strField);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HVALS(const std::string& strKey, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HVALS");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::HMSET(const std::string& strKey, const std::map<std::string, std::string>& mapFieldValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HMSET");
    m_vecRedisCmdParamList.emplace_back(strKey);
    for (auto& kvp : mapFieldValue)
    {
        m_vecRedisCmdParamList.emplace_back(kvp.first);
        m_vecRedisCmdParamList.emplace_back(kvp.second);
    }
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::HMSET(const std::string& strKey, const std::string& strField, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HMSET");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strField);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::HGETALL(const std::string& strKey, std::map<std::string, std::string>& mapFieldValue)
{
    ClearRedisCmdCache();
    mapFieldValue.clear();
    m_vecRedisCmdParamList.emplace_back("HGETALL");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnResponseKVP(strKey, false, mapFieldValue, __FUNCTION__);
}

bool CFlyRedisClient::HDEL(const std::string& strKey, const std::string& strField, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HDEL");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strField);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HEXISTS(const std::string& strKey, const std::string& strField, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HEXISTS");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strField);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HGET(const std::string& strKey, const std::string& strField, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HGET");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strField);
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::HMGET(const std::string& strKey, const std::string& strField, std::string& strValue)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HMGET");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strField);
    return RunRedisCmdOnOneLineResponseString(strKey, false, strValue, __FUNCTION__);
}

bool CFlyRedisClient::HMGET(const std::string& strKey, const std::vector<std::string>& vecField, std::vector<std::string>& vecOutput)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HMGET");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecField.begin(), vecField.end());
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecOutput, __FUNCTION__) && vecOutput.size() == vecField.size();
}

bool CFlyRedisClient::HINCRBY(const std::string& strKey, const std::string& strField, int nIncVal, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HINCRBY");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strField);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nIncVal));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::HINCRBYFLOAT(const std::string& strKey, const std::string& strField, double fIncVal, double& fResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HINCRBYFLOAT");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strField);
    m_vecRedisCmdParamList.emplace_back(std::to_string(fIncVal));
    return RunRedisCmdOnOneLineResponseDouble(strKey, true, fResult, __FUNCTION__);
}

bool CFlyRedisClient::HKEYS(const std::string& strKey, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HKEYS");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::HLEN(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("HLEN");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZADD(const std::string& strKey, double fScore, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZADD");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(fScore));
    m_vecRedisCmdParamList.emplace_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZADD(const std::string& strKey, unsigned long long nScore, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZADD");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nScore));
    m_vecRedisCmdParamList.emplace_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZCARD(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZCARD");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZCOUNT(const std::string& strKey, const std::string& strMin, const std::string& strMax, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZCOUNT");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strMin);
    m_vecRedisCmdParamList.emplace_back(strMax);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZINCRBY(const std::string& strKey, double fIncrement, const std::string& strMember, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZINCRBY");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(fIncrement));
    m_vecRedisCmdParamList.emplace_back(strMember);
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::ZRANGE(const std::string& strKey, int nStart, int nStop, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZRANGE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStart));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStop));
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ZRANGEBYSCORE(const std::string& strKey, int nMin, int nMax, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZRANGEBYSCORE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nMin));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nMax));
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ZRANGEBYSCORE(const std::string& strKey, const std::string& strMin, const std::string& strMax, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZRANGEBYSCORE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strMin);
    m_vecRedisCmdParamList.emplace_back(strMax);
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ZREVRANGEBYSCORE(const std::string& strKey, int nMin, int nMax, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZREVRANGEBYSCORE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nMin));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nMax));
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ZRANK(const std::string& strKey, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZRANK");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZREVRANGE_WITHSCORES(const std::string& strKey, int nStart, int nStop, std::vector<std::pair<std::string, double> >& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZREVRANGE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStart));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStop));
    m_vecRedisCmdParamList.emplace_back("WITHSCORES");
    std::vector< std::pair< std::string, std::string> > vecPairList;
    if (!RunRedisCmdOnResponsePairList(strKey, false, vecPairList, __FUNCTION__))
    {
        return false;
    }
    for (auto& kvp : vecPairList)
    {
        const std::string& strMember = kvp.first;
        double fScore = atof(kvp.second.c_str());
        vecResult.emplace_back(strMember, fScore);
    }
    return true;
}

bool CFlyRedisClient::ZREMRANGEBYSCORE(const std::string& strKey, double fFromScore, double fToScore, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZREMRANGEBYSCORE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(fFromScore));
    m_vecRedisCmdParamList.emplace_back(std::to_string(fToScore));
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::ZRANGE_WITHSCORES(const std::string& strKey, int nStart, int nStop, std::vector<std::pair<std::string, double> >& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZRANGE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStart));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStop));
    m_vecRedisCmdParamList.emplace_back("WITHSCORES");
    std::vector< std::pair< std::string, std::string> > vecPairList;
    if (!RunRedisCmdOnResponsePairList(strKey, false, vecPairList, __FUNCTION__))
    {
        return false;
    }
    for (auto& kvp : vecPairList)
    {
        const std::string& strMember = kvp.first;
        double fScore = atof(kvp.second.c_str());
        vecResult.emplace_back(strMember, fScore);
    }
    return true;
}

bool CFlyRedisClient::ZREVRANGE(const std::string& strKey, int nStart, int nStop, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZREVRANGE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStart));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStop));
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ZSCORE(const std::string& strKey, const std::string& strMember, double& fResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZSCORE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strMember);
    return RunRedisCmdOnOneLineResponseDouble(strKey, false, fResult, __FUNCTION__);
}

bool CFlyRedisClient::PFADD(const std::string& strKey, const std::string& strElement, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PFADD");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strElement);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PFADD(const std::string& strKey, const std::vector<std::string>& vecElements, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PFADD");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecElements.begin(), vecElements.end());
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PFCOUNT(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PFCOUNT");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PFCOUNT(const std::vector<std::string>& vecKey, int& nResult)
{
    if (vecKey.empty())
    {
        return false;
    }
    if (!CFlyRedis::IsMultiKeyOnTheSameNode(vecKey))
    {
        return false;
    }
    const std::string& strSeedKey = vecKey.front();
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PFCOUNT");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecKey.begin(), vecKey.end());
    return RunRedisCmdOnOneLineResponseInt(strSeedKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PFMERGE(const std::vector<std::string>& vecKey, int& nResult)
{
    if (vecKey.empty())
    {
        return false;
    }
    if (!CFlyRedis::IsMultiKeyOnTheSameNode(vecKey))
    {
        return false;
    }
    const std::string& strSeedKey = vecKey.front();
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PFCOUNT");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecKey.begin(), vecKey.end());
    return RunRedisCmdOnOneLineResponseInt(strSeedKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PFMERGE(const std::string& strKey1, const std::string& strKey2, int& nResult)
{
    if (!CFlyRedis::IsMultiKeyOnTheSameNode(strKey1, strKey2))
    {
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PFCOUNT");
    m_vecRedisCmdParamList.emplace_back(strKey1);
    m_vecRedisCmdParamList.emplace_back(strKey2);
    return RunRedisCmdOnOneLineResponseInt(strKey1, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::BLPOP(const std::string& strKey, int nTimeout, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("BLPOP");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nTimeout));
    return RunRedisCmdOnOneLineResponseVector(strKey, true, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::BRPOP(const std::string& strKey, int nTimeout, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("BRPOP");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nTimeout));
    return RunRedisCmdOnOneLineResponseVector(strKey, true, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::BRPOPLPUSH(const std::string& strSrcKey, const std::string& strDstKey, int nTimeout, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("BRPOPLPUSH");
    m_vecRedisCmdParamList.emplace_back(strSrcKey);
    m_vecRedisCmdParamList.emplace_back(strDstKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nTimeout));
    return RunRedisCmdOnOneLineResponseString(strSrcKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::LINDEX(const std::string& strKey, int nIndex, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LINDEX");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nIndex));
    return RunRedisCmdOnOneLineResponseString(strKey, false, strResult, __FUNCTION__);
}

bool CFlyRedisClient::LINSERT_BEFORE(const std::string& strKey, const std::string& strPivot, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LINSERT");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back("BEFORE");
    m_vecRedisCmdParamList.emplace_back(strPivot);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LINSERT_AFTER(const std::string& strKey, const std::string& strPivot, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LINSERT");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back("AFTER");
    m_vecRedisCmdParamList.emplace_back(strPivot);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LLEN(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LLEN");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LPOP(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LPOP");
    m_vecRedisCmdParamList.emplace_back(strKey);
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::ZREM(const std::string& strKey, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("ZREM");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LPUSH(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LPUSH");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LPUSHX(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LPUSHX");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LRANGE(const std::string& strKey, int nStart, int nStop, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LRANGE");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStart));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStop));
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::LREM(const std::string& strKey, int nCount, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LREM");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nCount));
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::LSET(const std::string& strKey, int nIndex, const std::string& strValue, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LSET");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nIndex));
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::LTRIM(const std::string& strKey, int nStart, int nStop, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("LTRIM");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStart));
    m_vecRedisCmdParamList.emplace_back(std::to_string(nStop));
    return RunRedisCmdOnOneLineResponseString(strKey, true, strResult, __FUNCTION__);
}

bool CFlyRedisClient::RPOP(const std::string& strKey, std::string& strResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("RPOP");
    m_vecRedisCmdParamList.emplace_back(strKey);
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
    m_vecRedisCmdParamList.emplace_back("RPOPLPUSH");
    m_vecRedisCmdParamList.emplace_back(strSrcKey);
    m_vecRedisCmdParamList.emplace_back(strDestKey);
    return RunRedisCmdOnOneLineResponseVector(strSrcKey, true, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::RPUSH(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("RPUSH");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::RPUSHX(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("RPUSHX");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SADD(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SADD");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SCARD(const std::string& strKey, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SCARD");
    m_vecRedisCmdParamList.emplace_back(strKey);
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
    m_vecRedisCmdParamList.emplace_back("SDIFF");
    m_vecRedisCmdParamList.emplace_back(strFirstKey);
    m_vecRedisCmdParamList.emplace_back(strSecondKey);
    return RunRedisCmdOnOneLineResponseVector(strFirstKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SDIFF(const std::vector<std::string>& vecKey, std::vector<std::string>& vecResult)
{
    if (vecKey.empty())
    {
        return false;
    }
    if (m_bClusterFlag && !CFlyRedis::IsMultiKeyOnTheSameNode(vecKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SDIFF");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecKey.begin(), vecKey.end());
    return RunRedisCmdOnOneLineResponseVector(vecKey.front(), false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SDIFFSTORE(const std::string& strDestKey, const std::vector<std::string>& vecSrcKey, int& nResult)
{
    if (m_bClusterFlag && !CFlyRedis::IsMultiKeyOnTheSameNode(vecSrcKey, strDestKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SDIFFSTORE");
    m_vecRedisCmdParamList.emplace_back(strDestKey);
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
    m_vecRedisCmdParamList.emplace_back("SINTER");
    m_vecRedisCmdParamList.emplace_back(strFirstKey);
    m_vecRedisCmdParamList.emplace_back(strSecondKey);
    return RunRedisCmdOnOneLineResponseVector(strFirstKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SINTER(const std::vector<std::string>& vecKey, std::vector<std::string>& vecResult)
{
    if (vecKey.empty())
    {
        return false;
    }
    if (m_bClusterFlag && !CFlyRedis::IsMultiKeyOnTheSameNode(vecKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SINTER");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecKey.begin(), vecKey.end());
    return RunRedisCmdOnOneLineResponseVector(vecKey.front(), false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SINTERSTORE(const std::string& strDestKey, const std::vector<std::string>& vecSrcKey, int& nResult)
{
    if (m_bClusterFlag && !CFlyRedis::IsMultiKeyOnTheSameNode(vecSrcKey, strDestKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SINTERSTORE");
    m_vecRedisCmdParamList.emplace_back(strDestKey);
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecSrcKey.begin(), vecSrcKey.end());
    return RunRedisCmdOnOneLineResponseInt(strDestKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SISMEMBER(const std::string& strKey, const std::string& strMember, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SISMEMBER");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strKey, false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SREM(const std::string& strKey, const std::string& strValue, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SREM");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(strValue);
    return RunRedisCmdOnOneLineResponseInt(strKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SUNION(const std::vector<std::string>& vecSrcKey, std::vector<std::string>& vecResult)
{
    if (vecSrcKey.empty())
    {
        return false;
    }
    if (m_bClusterFlag && !CFlyRedis::IsMultiKeyOnTheSameNode(vecSrcKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SUNION");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecSrcKey.begin(), vecSrcKey.end());
    return RunRedisCmdOnOneLineResponseVector(vecSrcKey.front(), false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SUNIONSTORE(const std::string& strDestKey, const std::vector<std::string>& vecSrcKey, int& nResult)
{
    if (m_bClusterFlag && !CFlyRedis::IsMultiKeyOnTheSameNode(vecSrcKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SUNIONSTORE");
    m_vecRedisCmdParamList.emplace_back(strDestKey);
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecSrcKey.begin(), vecSrcKey.end());
    return RunRedisCmdOnOneLineResponseInt(strDestKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::PUBLISH(const std::string& strChannel, const std::string& strMsg, int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PUBLISH");
    m_vecRedisCmdParamList.emplace_back(strChannel);
    m_vecRedisCmdParamList.emplace_back(strMsg);
    return RunRedisCmdOnOneLineResponseInt("", true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::UNSUBSCRIBE(const std::string& strChannel, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("UNSUBSCRIBE");
    if (!strChannel.empty())
    {
        m_vecRedisCmdParamList.emplace_back(strChannel);
    }
    return RunRedisCmdOnOneLineResponseVector("", true, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::UNSUBSCRIBE(std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("UNSUBSCRIBE");
    return RunRedisCmdOnOneLineResponseVector("", true, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::PUNSUBSCRIBE(const std::string& strPattern, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PUNSUBSCRIBE");
    m_vecRedisCmdParamList.emplace_back(strPattern);
    return RunRedisCmdOnOneLineResponseVector("", true, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::PUBSUB_CHANNELS(const std::string& strPattern, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PUBSUB");
    m_vecRedisCmdParamList.emplace_back("CHANNELS");
    if (!strPattern.empty())
    {
        m_vecRedisCmdParamList.emplace_back(strPattern);
    }
    return RunRedisCmdOnOneLineResponseVector("", true, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::PUBSUB_NUMSUB(const std::string& strChannel, int& nResult)
{
    std::vector<std::string> vecChannel;
    vecChannel.emplace_back(strChannel);
    std::map<std::string, int> mapResult;
    if (!PUBSUB_NUMSUB(vecChannel, mapResult))
    {
        return false;
    }
    auto itFind = mapResult.find(strChannel);
    if (itFind == mapResult.end())
    {
        return false;
    }
    nResult = itFind->second;
    return true;
}

bool CFlyRedisClient::PUBSUB_NUMSUB(const std::vector<std::string>& vecChannel, std::map<std::string, int>& mapResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PUBSUB");
    m_vecRedisCmdParamList.emplace_back("NUMSUB");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecChannel.begin(), vecChannel.end());
    std::map<std::string, std::string> mapKVP;
    if (!RunRedisCmdOnResponseKVP("", false, mapKVP, __FUNCTION__))
    {
        return false;
    }
    for (auto& kvp : mapKVP)
    {
        mapResult.emplace(kvp.first, atoi(kvp.second.c_str()));
    }
    return true;
}

bool CFlyRedisClient::PUBSUB_NUMPAT(int& nResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PUBSUB");
    m_vecRedisCmdParamList.emplace_back("NUMPAT");
    return RunRedisCmdOnOneLineResponseInt("", false, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SUBSCRIBE(const std::string& strChannel, FlyRedisSubscribeResponse& stResult)
{
    std::vector<std::string> vecChannel;
    vecChannel.emplace_back(strChannel);
    std::vector<FlyRedisSubscribeResponse> vecResult;
    if (!SUBSCRIBE(vecChannel, vecResult))
    {
        return false;
    }
    if (vecResult.size() != 1)
    {
        return false;
    }
    stResult = vecResult[0];
    return true;
}

bool CFlyRedisClient::SUBSCRIBE(const std::vector<std::string>& vecChannel, std::vector<FlyRedisSubscribeResponse>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SUBSCRIBE");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecChannel.begin(), vecChannel.end());
    return RunRedisCmdOnSubscribeCmd(vecResult, static_cast<int>(vecChannel.size()), __FUNCTION__);
}

bool CFlyRedisClient::PSUBSCRIBE(const std::string& strPattern, FlyRedisSubscribeResponse& stResult)
{
    std::vector<std::string> vecPattern;
    vecPattern.emplace_back(strPattern);
    std::vector<FlyRedisSubscribeResponse> vecResult;
    if (!PSUBSCRIBE(vecPattern, vecResult))
    {
        return false;
    }
    if (vecResult.size() != 1)
    {
        return false;
    }
    stResult = vecResult[0];
    return true;
}

bool CFlyRedisClient::PSUBSCRIBE(const std::vector<std::string>& vecPattern, std::vector<FlyRedisSubscribeResponse>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("PSUBSCRIBE");
    m_vecRedisCmdParamList.insert(m_vecRedisCmdParamList.end(), vecPattern.begin(), vecPattern.end());
    return RunRedisCmdOnSubscribeCmd(vecResult, static_cast<int>(vecPattern.size()), __FUNCTION__);
}

bool CFlyRedisClient::PollSubscribeMsg(std::vector<FlyRedisSubscribeResponse>& vecResult, int nBlockMS)
{
    if (nullptr == m_pCurRedisSession)
    {
        return false;
    }
    if (!m_pCurRedisSession->TryRecvRedisResponse(nBlockMS))
    {
        return false;
    }
    if (!BuildFlyRedisSubscribeResponse(m_pCurRedisSession->GetRedisResponseVector(), vecResult))
    {
        return false;
    }
    m_pCurRedisSession->ResetRedisResponse();
    return true;
}

bool CFlyRedisClient::PollPSubscribeMsg(std::vector<FlyRedisPMessageResponse>& vecResult, int nBlockMS)
{
    if (nullptr == m_pCurRedisSession)
    {
        return false;
    }
    if (!m_pCurRedisSession->TryRecvRedisResponse(nBlockMS))
    {
        return false;
    }
    if (!BuildFlyRedisPMessageResponse(m_pCurRedisSession->GetRedisResponseVector(), vecResult))
    {
        return false;
    }
    m_pCurRedisSession->ResetRedisResponse();
    return true;
}

bool CFlyRedisClient::SMEMBERS(const std::string& strKey, std::set<std::string>& setResult)
{
    ClearRedisCmdCache();
    if (nullptr == m_pCurRedisSession)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "NoRedisSession When Run SMEMBERS");
        return false;
    }
    m_vecRedisCmdParamList.emplace_back("SMEMBERS");
    m_vecRedisCmdParamList.emplace_back(strKey);
    bool bResult = false;
    if (2 == m_pCurRedisSession->GetRESPVersion())
    {
        std::vector<std::string> vecResult;
        bResult = RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
        for (auto& strValue : vecResult)
        {
            setResult.emplace(strValue);
        }
    }
    else
    {
        bResult = RunRedisCmdOnOneLineResponseSet(strKey, false, setResult, __FUNCTION__);
    }
    return bResult;
}

bool CFlyRedisClient::SMOVE(const std::string& strSrcKey, const std::string& strDestKey, const std::string& strMember, int& nResult)
{
    if (m_bClusterFlag && CFlyRedis::KeyHashSlot(strSrcKey) != CFlyRedis::KeyHashSlot(strDestKey))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CROSSSLOT Keys in request don't hash to the same slot");
        return false;
    }
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SMOVE");
    m_vecRedisCmdParamList.emplace_back(strSrcKey);
    m_vecRedisCmdParamList.emplace_back(strDestKey);
    m_vecRedisCmdParamList.emplace_back(strMember);
    return RunRedisCmdOnOneLineResponseInt(strSrcKey, true, nResult, __FUNCTION__);
}

bool CFlyRedisClient::SPOP(const std::string& strKey, int nCount, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SPOP");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nCount));
    return RunRedisCmdOnOneLineResponseVector(strKey, true, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::SRANDMEMBER(const std::string& strKey, int nCount, std::vector<std::string>& vecResult)
{
    ClearRedisCmdCache();
    m_vecRedisCmdParamList.emplace_back("SRANDMEMBER");
    m_vecRedisCmdParamList.emplace_back(strKey);
    m_vecRedisCmdParamList.emplace_back(std::to_string(nCount));
    return RunRedisCmdOnOneLineResponseVector(strKey, false, vecResult, __FUNCTION__);
}

bool CFlyRedisClient::ResolveRedisSession(const std::string& strKey, bool bIsWrite)
{
    if (!m_bClusterFlag || strKey.empty())
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
        CFlyRedis::Logger(FlyRedisLogLevel::Warning, "SlaveFailedSoRedirToMaster %s", strKey.c_str());
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
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "NodeLineInvalid %s", strNodeLine.c_str());
            bResult = false;
            break;
        }
        if (!mapRedisClusterNodesLine.emplace(stRedisClusterNodesLine.strNodeId, stRedisClusterNodesLine).second)
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "RedisNodeIdReduplicated %s", strNodeLine.c_str());
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
                CFlyRedis::Logger(FlyRedisLogLevel::Error, "SlaveHasNoMaster %s", kvp.first.c_str());
                bResult = false;
                break;
            }
            const RedisClusterNodesLine& refMasterNode = itFindMaster->second;
            refRedisNode.nMinSlot = refMasterNode.nMinSlot;
            refRedisNode.nMaxSlot = refMasterNode.nMaxSlot;
        }
        if (!ConnectToOneClusterNode(refRedisNode))
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "ConnectToClusterNodesLineFailed %s-%s", kvp.first.c_str(), refRedisNode.strNodeIPPort.c_str());
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
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CreateRedisSessionFailed %s", stRedisNode.strNodeIPPort.c_str());
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
#ifdef FLY_REDIS_ENABLE_TLS
    CFlyRedisSession* pRedisSession = new CFlyRedisSession(m_boostIOContext, m_bUseTLSFlag, m_boostTLSContext);
#else
    CFlyRedisSession* pRedisSession = new CFlyRedisSession(m_boostIOContext);
#endif // FLY_REDIS_ENABLE_TLS
    pRedisSession->SetRedisAddress(strRedisAddress);
    pRedisSession->SetReadTimeoutSeconds(m_nReadTimeoutSeconds);
    if (!pRedisSession->Connect())
    {
        delete pRedisSession;
        pRedisSession = nullptr;
        return nullptr;
    }
    if (!m_strRedisPasswod.empty() && !pRedisSession->AUTH(m_strRedisPasswod))
    {
        delete pRedisSession;
        pRedisSession = nullptr;
        return nullptr;
    }
    if (!pRedisSession->ResolveServerVersion())
    {
        delete pRedisSession;
        pRedisSession = nullptr;
        return nullptr;
    }
    m_mapRedisSession.emplace(strRedisAddress, pRedisSession);
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
        CFlyRedis::Logger(FlyRedisLogLevel::Debug, "DestroyRedisSession %s", pRedisSession->GetRedisAddr().c_str());
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
            CFlyRedis::Logger(FlyRedisLogLevel::Warning, "RedisNode %s PingFailed", pRedisSession->GetRedisAddr().c_str());
            vecDeadRedisSession.emplace_back(pRedisSession);
        }
    }
}

bool CFlyRedisClient::DeliverRedisCmd(const std::string& strKey, bool bIsWrite, bool bRunRecvCmd, const char* pszCaller)
{
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
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "CurRedisSessionIsNull %s", pszCaller);
        m_bHasBadRedisSession = true;
        return false;
    }
    // Only write log for write cmd
    CFlyRedis::BuildRedisCmdRequest(m_pCurRedisSession->GetRedisAddr(), m_vecRedisCmdParamList, m_strRedisCmdRequest, bIsWrite);
    if (!bRunRecvCmd)
    {
        m_pCurRedisSession->TrySendRedisRequest(m_strRedisCmdRequest);
        return true;
    }
    if (!m_pCurRedisSession->ProcRedisRequest(m_strRedisCmdRequest))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ProcRedisRequestFailed %s", pszCaller);
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
    if (!DeliverRedisCmd(strKey, bIsWrite, true, pszCaller))
    {
        return false;
    }
    if (nullptr != m_pCurRedisSession)
    {
        strResult.swap(m_pCurRedisSession->GetRedisResponseString());
        return true;
    }
    return true;
}

bool CFlyRedisClient::RunRedisCmdOnOneLineResponseVector(const std::string& strKey, bool bIsWrite, std::vector<std::string>& vecResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, bIsWrite, true, pszCaller))
    {
        return false;
    }
    if (nullptr != m_pCurRedisSession)
    {
        vecResult.swap(m_pCurRedisSession->GetRedisResponseVector());
        return true;
    }
    return false;
}

bool CFlyRedisClient::RunRedisCmdOnOneLineResponseSet(const std::string& strKey, bool bIsWrite, std::set<std::string>& setResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, bIsWrite, true, pszCaller))
    {
        return false;
    }
    if (nullptr != m_pCurRedisSession)
    {
        setResult.swap(m_pCurRedisSession->GetRedisResponseSet());
        return true;
    }
    return false;
}

bool CFlyRedisClient::RunRedisCmdOnResponseKVP(const std::string& strKey, bool bIsWrite, std::map<std::string, std::string>& mapResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, bIsWrite, true, pszCaller))
    {
        return false;
    }
    if (nullptr == m_pCurRedisSession)
    {
        return false;
    }
    if (3 == m_pCurRedisSession->GetRESPVersion())
    {
        mapResult.swap(m_pCurRedisSession->GetRedisResponseMap());
        return true;
    }
    const std::vector<std::string>& vecRedisResponse = m_pCurRedisSession->GetRedisResponseVector();
    int nLineCount = (int)vecRedisResponse.size();
    if (nLineCount % 2 != 0)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineCountIsNotEven %d", nLineCount);
        return false;
    }
    bool bResult = true;
    int nKeyIndex = 0;
    int nValueIndex = 1;
    for (; nKeyIndex < nLineCount && nValueIndex < nLineCount; nKeyIndex += 2, nValueIndex += 2)
    {
        const std::string& strField = vecRedisResponse[nKeyIndex];
        const std::string& strValue = vecRedisResponse[nValueIndex];
        if (!mapResult.emplace(strField, strValue).second)
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineReduplicateField %s", pszCaller);
            bResult = false;
            break;
        }
    }
    return bResult;
}

bool CFlyRedisClient::RunRedisCmdOnResponsePairList(const std::string& strKey, bool bIsWrite, std::vector< std::pair<std::string, std::string> >& vecResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, bIsWrite, true, pszCaller))
    {
        return false;
    }
    const std::vector<std::string>& vecRedisResponse = m_pCurRedisSession->GetRedisResponseVector();
    int nLineCount = (int)vecRedisResponse.size();
    if (nLineCount % 2 != 0)
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "ResponseLineCountIsNotEven %d", nLineCount);
        return false;
    }
    int nKeyIndex = 0;
    int nValueIndex = 1;
    for (; nKeyIndex < nLineCount && nValueIndex < nLineCount; nKeyIndex += 2, nValueIndex += 2)
    {
        const std::string& strField = vecRedisResponse[nKeyIndex];
        const std::string& strValue = vecRedisResponse[nValueIndex];
        vecResult.emplace_back(strField, strValue);
    }
    return true;
}

bool CFlyRedisClient::RunRedisCmdOnScanCmd(const std::string& strKey, int& nResultCursor, std::vector<std::string>& vecResult, const char* pszCaller)
{
    if (!DeliverRedisCmd(strKey, false, true, pszCaller))
    {
        return false;
    }
    if (nullptr == m_pCurRedisSession)
    {
        return false;
    }
    std::vector<std::string>& vecRedisResponse = m_pCurRedisSession->GetRedisResponseVector();
    if (vecRedisResponse.empty())
    {
        return false;
    }
    nResultCursor = atoi(vecRedisResponse.front().c_str());
    vecRedisResponse.erase(vecRedisResponse.begin());
    vecResult.swap(vecRedisResponse);
    return true;
}

bool CFlyRedisClient::RunRedisCmdOnSubscribeCmd(std::vector<FlyRedisSubscribeResponse>& vecResult, int nChannelCount, const char* pszCaller)
{
    if (!DeliverRedisCmd("", false, false, pszCaller))
    {
        return false;
    }
    if (nullptr == m_pCurRedisSession)
    {
        return false;
    }
    bool bResult = true;
    while (static_cast<int>(vecResult.size()) != nChannelCount)
    {
        if (!m_pCurRedisSession->TryRecvRedisResponse(10))
        {
            bResult = false;
            break;
        }
        if (BuildFlyRedisSubscribeResponse(m_pCurRedisSession->GetRedisResponseVector(), vecResult))
        {
            m_pCurRedisSession->ResetRedisResponse();
        }
    }
    return bResult;
}

void CFlyRedisClient::ClearRedisCmdCache()
{
    m_vecRedisCmdParamList.clear();
    m_strRedisCmdRequest.clear();
}

bool CFlyRedisClient::BuildFlyRedisSubscribeResponse(const std::vector<std::string>& vecInput, std::vector<FlyRedisSubscribeResponse>& vecResult) const
{
    int nCount = static_cast<int>(vecInput.size());
    if (nCount % 3 != 0)
    {
        return false;
    }
    for (int nIndex = 2; nIndex < nCount; nIndex += 3)
    {
        FlyRedisSubscribeResponse stResponse;
        stResponse.strCmd = vecInput[nIndex - 2];
        stResponse.strChannel = vecInput[nIndex - 1];
        stResponse.strMsg = vecInput[nIndex];
        vecResult.emplace_back(stResponse);
    }
    return true;
}

bool CFlyRedisClient::BuildFlyRedisPMessageResponse(const std::vector<std::string>& vecInput, std::vector<FlyRedisPMessageResponse>& vecResult) const
{
    int nCount = static_cast<int>(vecInput.size());
    if (nCount % 4 != 0)
    {
        return false;
    }
    for (int nIndex = 3; nIndex < nCount; nIndex += 4)
    {
        FlyRedisPMessageResponse stResponse;
        stResponse.strCmd = vecInput[nIndex - 3];
        stResponse.strPattern = vecInput[nIndex - 2];
        stResponse.strChannel = vecInput[nIndex - 1];
        stResponse.strMsg = vecInput[nIndex];
        vecResult.emplace_back(stResponse);
    }
    return true;
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
    default:
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

bool CFlyRedis::IsMultiKeyOnTheSameNode(const std::string& strKeyFirst, const std::string& strKeySecond)
{
    return KeyHashSlot(strKeyFirst) == KeyHashSlot(strKeySecond);
}

bool CFlyRedis::IsMultiKeyOnTheSameNode(const std::vector<std::string>& vecKey)
{
    std::set<int> setSlot;
    for (auto& strKey : vecKey)
    {
        setSlot.emplace(KeyHashSlot(strKey));
    }
    return setSlot.size() == 1;
}

bool CFlyRedis::IsMultiKeyOnTheSameNode(const std::map<std::string, std::string>& mapKeyValue)
{
    std::set<int> setSlot;
    for (auto& kvp : mapKeyValue)
    {
        setSlot.emplace(KeyHashSlot(kvp.first));
    }
    return setSlot.size() == 1;
}

bool CFlyRedis::IsMultiKeyOnTheSameNode(const std::vector<std::string>& vecKey, const std::string& strMoreKey)
{
    std::set<int> setSlot;
    setSlot.emplace(KeyHashSlot(strMoreKey));
    for (auto& strKey : vecKey)
    {
        setSlot.emplace(KeyHashSlot(strKey));
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
            vecResult.emplace_back(strToken);
            strToken.clear();
        }
    }
    if (!strToken.empty())
    {
        vecResult.emplace_back(strToken);
    }
    if (strInput.back() == chDelim)
    {
        vecResult.emplace_back("");
    }
    return vecResult;
}

void CFlyRedis::BuildRedisCmdRequest(const std::string& strRedisAddress, const std::vector<std::string>& vecRedisCmdParamList, std::string& strRedisCmdRequest, bool bIsWriteCmd)
{
    std::string strCmdLog;
    strRedisCmdRequest.clear();
    strRedisCmdRequest.append("*").append(std::to_string((int)vecRedisCmdParamList.size())).append("\r\n");
    for (const std::string& strParam : vecRedisCmdParamList)
    {
        strRedisCmdRequest.append("$").append(std::to_string((int)strParam.length())).append("\r\n");
        strRedisCmdRequest.append(strParam).append("\r\n");
        if (!bIsWriteCmd)
        {
            continue;
        }
        if (strCmdLog.length() < 4096)
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
    if (bIsWriteCmd && !strCmdLog.empty())
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Command, "RedisCmd,%s,%s", strRedisAddress.c_str(), strCmdLog.c_str());
    }
}

// End of FlyRedis
//////////////////////////////////////////////////////////////////////////
// Define Struct RedisClusterNodesLine
CFlyRedisClient::RedisClusterNodesLine::RedisClusterNodesLine()
{

}

bool CFlyRedisClient::RedisClusterNodesLine::ParseNodeLine(const std::string& strNodeLine)
{
    bIsMaster = (strNodeLine.find("master") != std::string::npos);
    std::vector<std::string> vecNodeField = CFlyRedis::SplitString(strNodeLine, ' ');
    if ((bIsMaster && vecNodeField.size() != 9) || (!bIsMaster && vecNodeField.size() != 8))
    {
        CFlyRedis::Logger(FlyRedisLogLevel::Error, "NodeFieldInvalid %s", strNodeLine.c_str());
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
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "InvalidFieldLen %s Slot %s", strNodeIPPort.c_str(), strSlotRange.c_str());
            return false;
        }
        nMinSlot = atoi(vecIntField[0].c_str());
        nMaxSlot = atoi(vecIntField[1].c_str());
        if (nMinSlot > nMaxSlot)
        {
            CFlyRedis::Logger(FlyRedisLogLevel::Error, "InvalidSlotValue %s Slot %s", strNodeIPPort.c_str(), strSlotRange.c_str());
            return false;
        }
    }
    else
    {
        strMasterNodeId = vecNodeField[3];
    }
    return true;
}
