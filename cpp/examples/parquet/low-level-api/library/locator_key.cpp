#include "dhl_fs.h"
#include "locator_key.h"

#include <algorithm>
#include <iostream>

const int kDieRowOffset = 51;
const int kSwath = 39;
const int kSubSwath = 35;
const int kChanNum = 32;

struct DhlLocatorKey {
    int die_row;
    int swath;
    int chan_num;
};

DhlLocatorKey CreateLocatorKey(std::string file_path) {
    // example file_path format is: /root_path/R0C0S/test_dhl/dierow_11/swath_0/channel0.patch
    std::vector<std::string> tokens = DhlFileSystem::Tokenize(file_path, '/');

    // only the last 3 tokens has the information we are looking for: die_row, swath, channel_number
    int size = tokens.size();
    int die_row = std::stoi(tokens.at(size - 3).substr(7)); // dierow_xx
    int swath = std::stoi(tokens.at(size - 2).substr(6)); // swath_xx
    int chan_num = std::stoi(tokens.at(size - 1).substr(7).substr(0, 1));

    DhlLocatorKey key = {die_row, swath, chan_num};

    return key;
}

uint64_t LocatorKey::GetLocatorKeyNum(
        uint64_t die_row,
        uint64_t swath,
        uint64_t sub_swath,
        uint64_t channel_number,
        int row_id) {

    return(die_row << kDieRowOffset) |
    (swath << kSwath) |
    (sub_swath << kSubSwath)|
    (channel_number << kChanNum) |
    row_id;
}

std::string LocatorKey::GetIncrementalIds(
        const std::vector<uint64_t> &locator_ids,
        std::string file_path) {

    // example file_path format is /root_path/R0C0S/test_dhl/dierow_11/swath_0/channel0.patch
    DhlLocatorKey key = CreateLocatorKey(file_path);

//    std::cout << "die_row = " << key.die_row << std::endl;
//    std::cout << "swath = " << key.swath << std::endl;
//    std::cout << "chan_num = " << key.chan_num << std::endl;

    // get the lower bound and upper bound values
    uint64_t lb = GetLocatorKeyNum(key.die_row, key.swath, 0, key.chan_num, 0);

    uint64_t ub;
    if (key.chan_num <= 0) {
        ub = GetLocatorKeyNum(key.die_row, key.swath, 0, 1 , 0);
    } else {
        ub = GetLocatorKeyNum(key.die_row, key.swath + 1, 0, 0 , 0);
    }

//    std::cout << "lb = " << lb << std::endl;
//    std::cout << "ub = " << ub << std::endl;

    // get locator_ids that has the same die_row, swath
    auto lower_it = std::lower_bound(locator_ids.begin(), locator_ids.end(), lb);
    auto upper_it = std::lower_bound(locator_ids.begin(), locator_ids.end(), ub);

//    std::cout << "lower_it = " << *lower_it << std::endl;
//    std::cout << "upper_it = " << *upper_it << std::endl;

    // build the ids string that looks like "1,2,3,4,"
    std::string ids = "";
    uint64_t one = 1;
    for (auto it = lower_it; it < upper_it; it++) {
        uint64_t val = *it;
        //std::cout << val << std::endl;

        // bitwise AND so we only keep the right-most 32 bit for the id
        int id = (int) (val & ((one << kChanNum) - one));

        ids += std::to_string(id) + ",";
    }

    ids.pop_back(); // from "1,2,3,4," to "1,2,3,4"

    return ids;
}

std::vector<uint64_t> LocatorKey::GenerateRandomLocatorKeys(
        std::string dhl_name,
        std::string root_path,
        float size_ratio,
        int actual_row_cout_per_table) {

    std::vector<uint64_t> random_keys;

    std::vector<std::string> file_paths = DhlFileSystem::GetAllFilePaths(dhl_name, root_path, "patch", 0);

    // randomly select an initial value between 0 to (shrink_factor - 1)
    srand(time(0));
    int shrink_factor = 1 / size_ratio;
    int initial = rand() % shrink_factor;

    // we need to build arbitrary number of ids with a hard coded upper limit, because we cannot
    // dig into each database file and find out the max count of rows.  We'll never generate more keys
    // than specified by user.
    int upper_limit = actual_row_cout_per_table / shrink_factor;

    std::cout << "Generate random keys from: " << file_paths.size() << " files on this node.  Shrink factor: "
    << shrink_factor << " , upper limit: " << upper_limit << std::endl;

    for (std::string file_path : file_paths) {
        DhlLocatorKey key = CreateLocatorKey(file_path);

        for (int i = 0; i < upper_limit; i++) {
            random_keys.push_back(
                    GetLocatorKeyNum(key.die_row, key.swath, 0, key.chan_num, initial + i * shrink_factor));
        }
    }

    return random_keys;
}
