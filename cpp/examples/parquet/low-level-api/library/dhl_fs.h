//
// Created by Wen Lai on 6/28/20.
//

#ifndef AS_DHL_FS_H
#define AS_DHL_FS_H

#include <string>
#include <vector>

class DhlFileSystem {
public:
    static std::vector<std::string> GetAllFilePaths(
            std::string dhl_name, std::string root_path, std::string file_ext, int node_idx);

    static std::vector<std::string> Tokenize(const std::string &str, const char delim);
};

#endif //AS_DHL_FS_H
