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

std::function<void(const char*)> CFlyRedis::ms_pfnLoggerDebug = nullptr;
std::function<void(const char*)> CFlyRedis::ms_pfnLoggerNotice = nullptr;
std::function<void(const char*)> CFlyRedis::ms_pfnLoggerWarning = nullptr;
std::function<void(const char*)> CFlyRedis::ms_pfnLoggerError = nullptr;
std::function<void(const char*)> CFlyRedis::ms_pfnLoggerPersistence = nullptr;
int CFlyRedis::ms_nRedisReadTimeOutSeconds = 10;

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
