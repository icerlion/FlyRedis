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
* FileName: FlyRedis.h
*
* Purpose:  FlyRedis, util function and config function
*
* Author:   Jhon Frank(icerlion@163.com)
*
* Modify:   2019/5/23 15:24
===================================================================+*/
#ifndef _FLYREDIS_H_
#define _FLYREDIS_H_

#include <functional>
#include <string>
#include <vector>
#include <map>
#include "FlyRedisClient.h"

//////////////////////////////////////////////////////////////////////////
// Define Log Level
enum class FlyRedisLogLevel : int
{
    Debug = 1,
    Notice = 2,
    Warning = 3,
    Error = 4,
    Command = 5,
};

//////////////////////////////////////////////////////////////////////////
// Define CFlyRedis
class CFlyRedis
{
public:
    // Set logger handler
    static void SetLoggerHandler(FlyRedisLogLevel nLogLevel, std::function<void(const char*)> pfnLoggerHandler);

    // Define logger function
    static void Logger(FlyRedisLogLevel nLevel, const char* pszMsgFormat, ...);

    // Calc the slot index
    static bool IsMlutiKeyOnTheSameSlot(const std::string& strKeyFirst, const std::string& strKeySecond);
    static bool IsMlutiKeyOnTheSameSlot(const std::vector<std::string>& vecKey);
    static bool IsMlutiKeyOnTheSameSlot(const std::vector<std::string>& vecKey, const std::string& strMoreKey);
    static bool IsMlutiKeyOnTheSameSlot(const std::map<std::string, std::string>& mapKeyValue);
    static int KeyHashSlot(const std::string& strKey);
    static int KeyHashSlot(const char* pszKey, int nKeyLen);

    // Util function split string
    static std::vector<std::string> SplitString(const std::string& strInput, char chDelim);

    // Config function, GetRedisReadTimeOutSeconds
    inline static int GetRedisReadTimeOutSeconds()
    {
        return ms_nRedisReadTimeOutSeconds;
    }

    // Config function SetRedisReadTimeOutSeconds
    inline static void SetRedisReadTimeOutSeconds(int nValue)
    {
        ms_nRedisReadTimeOutSeconds = nValue;
    }

    // Util function build RedisCmdRequest
    static void BuildRedisCmdRequest(const std::string& strRedisAddress, const std::vector<std::string>& vecRedisCmdParamList, std::string& strRedisCmdRequest);

private:
    // Get logger handler by log level
    static std::function<void(const char*)> GetLoggerHandler(FlyRedisLogLevel nLogLevel);

    // Util function, CRC16
    static int CRC16(const char* buff, int nLen);


private:
    static std::function<void(const char*)> ms_pfnLoggerDebug;
    static std::function<void(const char*)> ms_pfnLoggerNotice;
    static std::function<void(const char*)> ms_pfnLoggerWarning;
    static std::function<void(const char*)> ms_pfnLoggerError;
    static std::function<void(const char*)> ms_pfnLoggerPersistence;
    static int ms_nRedisReadTimeOutSeconds;
};

#endif // _FLYREDIS_H_