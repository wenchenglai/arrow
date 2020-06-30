#ifndef AS_DHL_LOCATOR_KEY_H
#define AS_DHL_LOCATOR_KEY_H

#include <string>
#include <vector>

class LocatorKey {
public:
    static uint64_t GetLocatorKeyNum(
            uint64_t die_row,
            uint64_t swath,
            uint64_t sub_swath,
            uint64_t channel_number,
            int row_id);

    ///
    /// \param locator_ids
    /// \param file_path absolute file path
    /// \return
    static std::string GetIncrementalIds(
            const std::vector<uint64_t> &locator_ids,
            std::string file_path);

    static std::vector<uint64_t> GenerateRandomLocatorKeys(
            std::string dhl_name,
            std::string root_path,
            float size_ratio,
            int actual_row_cout_per_table);
};



#endif //AS_DHL_LOCATOR_KEY_H
