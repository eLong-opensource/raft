#ifndef RAFT_BASE_KVCONFIG_H
#define RAFT_BASE_KVCONFIG_H

#include <string>
#include <map>

namespace raft {

class KvConfig {
public:
    KvConfig();
    int Parse(const std::string& file);
    std::string& String(const std::string& key);
    bool Exists(const std::string& key);
    int Int(const std::string& key);
    bool Bool(const std::string& key);
private:
    std::map<std::string, std::string> map_;
};

}
#endif
