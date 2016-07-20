/*
 * address.h
 *
 *  Created on: Dec 23, 2013
 *      Author: fan
 */

#ifndef RAFT_BASE_ADDRESS_H_
#define RAFT_BASE_ADDRESS_H_

#include <string>
#include <stdint.h>

namespace raft {
bool Address(const std::string& addr, std::string* ip, uint16_t* port);
}

#endif /* RAFT_BASE_ADDRESS_H_ */
