#pragma once

#include <string>
#include <unordered_map>
#include "status.h"

using namespace std;

Status write_segment(
    const string &path,
    const unordered_map<string, string> &data
);

Status read_segment(
    const string &path,
    unordered_map<string, string> &out
);