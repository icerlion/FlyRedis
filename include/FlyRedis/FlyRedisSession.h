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
* FileName: FlyRedisSession.h
*
* Purpose:  Define CFlyRedisSession
*
* Author:   Jhon Frank(icerlion@163.com)
*
* Modify:   2019/5/23 15:24
===================================================================+*/
#ifndef _FLYREDISSESSION_H_
#define _FLYREDISSESSION_H_

#include "boost/asio.hpp"
#include <string>
#include <vector>

class CFlyRedisSession
{
public:
    // Constructor
    CFlyRedisSession();

    // Destructor
    ~CFlyRedisSession();

    // Set redis address
    void SetRedisAddress(const std::string& strAddress);

    // Get redis address
    const std::string& GetRedisAddr() const;

    // Connect to redis
    bool Connect();

    // Return true if accept this slot
    inline bool AcceptHashSlot(int nSlot) const
    {
        return nSlot >= m_nMinSlot && nSlot <= m_nMaxSlot;
    }

    // Set self slot range
    bool SetSelfSlotRange(const std::string& strSlotRange);

    // Process redis cmd request
    bool ProcRedisRequest(const std::string& strRedisCmdRequest, std::vector<std::string>& vecRedisResponseLine);

    //////////////////////////////////////////////////////////////////////////
    /// Begin of RedisCmd
    bool AUTH(std::string& strPassword);
    bool PING();
    bool INFO_CLUSTER(bool& bClusterEnable);
    bool CLUSTER_NODES(std::vector<std::string>& vecResult);
    bool SCRIPT_LOAD(const std::string& strScript, std::string& strResult);
    /// End of RedisCmd
    //////////////////////////////////////////////////////////////////////////
private:
    // Recv redis response
    bool RecvRedisResponse(std::vector<std::string>& vecRedisResponseLine);
    bool ReadRedisResponseError(std::vector<std::string>& vecRedisResponseLine);
    bool ReadRedisResponseSimpleStrings(std::vector<std::string>& vecRedisResponseLine);
    bool ReadRedisResponseIntegers(std::vector<std::string>& vecRedisResponseLine);
    bool ReadRedisResponseBulkStrings(std::vector<std::string>& vecRedisResponseLine);
    bool ReadRedisResponseArrays(std::vector<std::string>& vecRedisResponseLine);

    // Read one line from socket
    bool ReadUntilCRLF(std::string& strLine);

private:
    // RedisAddress, format: host:port
    std::string m_strRedisAddress;
    // SlotRange
    int m_nMinSlot;
    int m_nMaxSlot;
    //////////////////////////////////////////////////////////////////////////
    // Network data member
    boost::asio::ip::tcp::iostream m_boostTCPIOStream;
    // According the RESP document, the max length of BulkStrings was 512MB, you can changed the buff len
    const static int CONST_BUFF_BULK_STRINGS_ONCE_LEN = 1024 * 32;
    char m_buffBulkStrings[CONST_BUFF_BULK_STRINGS_ONCE_LEN];
    bool m_bRedisResponseError;
};

#endif // _FLYREDISSESSION_H_
