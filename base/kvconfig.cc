#include <sstream>
#include <stdio.h>
#include "kvconfig.h"

using namespace raft;

KvConfig::KvConfig()
{
}

int KvConfig::Parse(const std::string& file)
{
    FILE* fp = NULL;
    fp = fopen(file.c_str(), "r");
    if (fp == NULL) {
        return -1;
    }
    int line = 0;
    char buf[1024];
    std::string key, value;
    while (fgets(buf, 1024, fp) != NULL) {
        key.clear();
        value.clear();
        line++;
        if (buf[0] == '#') {
            continue;
        }
        char* p = buf;

        // strip blanks
        while (*p && *p == ' ') {
            ++p;
        }

        // jump blank line
        if (*p == '\n') {
            continue;
        }

        while (*p && *p != '=') {
            if (*p != ' ') {
                key.push_back(*p);
            }
            p++;
        }
        if (!*p) {
            fprintf(stderr, "error at line %d:missing `='\n", line);
            return -1;
        }
        ++p;
        while (*p) {
            if (*p != ' ' && *p != '\n') {
                value.push_back(*p);
            }
            p++;
        }
        map_[key] = value;
    }
    return 0;
}

bool KvConfig::Exists(const std::string& key)
{
    return map_.count(key) > 0;
}

std::string& KvConfig::String(const std::string& key)
{
    return map_[key];
}

int KvConfig::Int(const std::string& key)
{
    int ret = 0;
    std::string& val = map_[key];
    std::stringstream ss(val);
    ss >> ret;
    return ret;
}

bool KvConfig::Bool(const std::string& key)
{
    return map_[key] == "true";
}
