/*
 * address.cc
 *
 *  Created on: Dec 23, 2013
 *      Author: fan
 */

#include <stdio.h>
#include <muduo/net/InetAddress.h>
#include <raft/base/address.h>

namespace raft {

bool Address(const std::string& addr, std::string* ip, uint16_t* port)
{
    if (ip == NULL || port == NULL) {
        return false;
    }

    std::string portstr;
    size_t pos = addr.find(":");
    if (pos == std::string::npos) {
        return false;
    }

    *ip = addr.substr(0, pos);
    if (ip->empty()) {
        *ip = "0.0.0.0";
    }

    portstr = addr.substr(pos + 1);
    if (sscanf(portstr.c_str(), "%hu", port) < 1) {
        return false;
    }

    return true;
}
} // end of namespace raft


