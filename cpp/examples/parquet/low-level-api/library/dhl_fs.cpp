#include "dhl_fs.h"

#include <dirent.h>
#include <iostream>

// break down a string using delimiter delim.
std::vector<std::string> DhlFileSystem::Tokenize(const std::string &str, const char delim)
{
    size_t start;
    size_t end = 0;
    std::vector<std::string> tokens;

    while ((start = str.find_first_not_of(delim, end)) != std::string::npos)
    {
        end = str.find(delim, start);
        tokens.push_back(str.substr(start, end - start));
    }

    return tokens;
}

std::vector<std::string> DhlFileSystem::GetAllFilePaths(
        std::string dhl_name, std::string root_path, std::string file_ext, int node_idx) {

    // CONSTANTS declaration, could move else where for more flexibility
    std::string DHL_ROOT_PATH = root_path;

    if (!opendir(DHL_ROOT_PATH.c_str())) {
        DHL_ROOT_PATH = "/Users/wen/github/arrow/data/test_dirs/";
    }

    std::string DIE_ROW = "dierow_";
    std::string SWATH = "swath_";
    std::string CH0PATCH = "channel0." + file_ext;
    std::string CH1PATCH = "channel1." + file_ext;

    std::vector<std::string> file_paths;

    std::string worker_node_path = "R" + std::to_string(node_idx) + "C0S/";

    std::string dhl_path = DHL_ROOT_PATH + worker_node_path + dhl_name;

    // TODO strings for different machine, the strings is one node ONLY
    // for vi3-0009
    //dhl_path = "/Volumes/remoteStorage/" + dhl_name;
    // for windows Linux
    //dhl_path = "/home/wen/github/arrow/data/test_dirs/ROCOS/" + dhl_name;

    std::cout << "Scanning the data path: " << dhl_path << std::endl;

    DIR *dhl_dir = nullptr;
    if ((dhl_dir = opendir(dhl_path.c_str())) != NULL) {
        struct dirent *dhl_dir_item;

        while ((dhl_dir_item = readdir(dhl_dir)) != NULL) {
            if (dhl_dir_item->d_type == DT_DIR) {
                std::string die_row_folder_name = dhl_dir_item->d_name;

                if (die_row_folder_name.find(DIE_ROW) != std::string::npos) {
                    DIR *die_row_dir = nullptr;
                    std::string abs_die_row_path = dhl_path + "/" + die_row_folder_name;
                    if ((die_row_dir = opendir(abs_die_row_path.c_str())) != NULL) {
                        struct dirent *die_row_dir_item;
                        while ((die_row_dir_item = readdir(die_row_dir)) != NULL) {
                            if (die_row_dir_item->d_type == DT_DIR) {
                                std::string swath_folder_name = die_row_dir_item->d_name;

                                if (swath_folder_name.find(SWATH) != std::string::npos) {
                                    DIR *swath_dir = nullptr;
                                    std::string abs_swath_path = abs_die_row_path + "/" + swath_folder_name;
                                    if ((swath_dir = opendir(abs_swath_path.c_str())) != NULL) {
                                        struct dirent *swath_dir_item;
                                        while ((swath_dir_item = readdir(swath_dir)) != NULL) {
                                            if (swath_dir_item->d_type == DT_REG) {
                                                std::string full_path_file_name = swath_dir_item->d_name;
                                                const char delim = '/';
                                                std::vector<std::string> fileTokens = Tokenize(full_path_file_name, delim);
                                                std::string file_name_only = fileTokens.back();

                                                if (file_name_only == CH0PATCH || file_name_only == CH1PATCH) {
                                                    //std::cout << "accepted file = " << full_path_file_name << std::endl;
                                                    file_paths.push_back(abs_swath_path + "/" + full_path_file_name);
                                                }
                                            }
                                        }
                                        closedir(swath_dir);
                                    }
                                }
                            }
                        }
                        closedir(die_row_dir);
                    }
                }
            }
        }
        closedir(dhl_dir);
    }
    return file_paths;
}

