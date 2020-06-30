/*
 * dhl_name: test_dhl
 * root path:
 */
#include "../library/locator_key.h"

#include <algorithm>
#include <iostream>

void TestGetLocatorKey() {
    std::vector<uint64_t> list;

    uint64_t a1 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 0, 0);
    uint64_t a2 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 0, 1);
    uint64_t a3 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 0, 2);
    uint64_t a4 = LocatorKey::GetLocatorKeyNum(2, 1, 0, 1, 0);
    uint64_t a5 = LocatorKey::GetLocatorKeyNum(1, 2, 0, 1, 0);

    std::cout << "a1 = " << a1 << std::endl;
    std::cout << "a2 = " << a2 << std::endl;
    std::cout << "a3 = " << a3 << std::endl;
    std::cout << "a4 = " << a4 << std::endl;
    std::cout << "a5 = " << a5 << std::endl;

    list.push_back(a3);
    list.push_back(a2);
    list.push_back(a1);
    list.push_back(a4);
    list.push_back(a5);

    for (uint64_t val : list) {
        std::cout << val << std::endl;
    }

    std::sort(list.begin(), list.end());

    for (uint64_t val : list) {
        std::cout << val << std::endl;
    }
}

void TestGetIncrementalIds() {
    std::vector<uint64_t> list;

    uint64_t a1 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 0, 0);
    uint64_t a2 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 0, 1);
    uint64_t a3 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 0, 2);
    uint64_t a4 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 0, 3);
    uint64_t a5 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 0, 4);
    uint64_t a6 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 0, 5);
    uint64_t a7 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 1, 0);
    uint64_t a8 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 1, 10);
    uint64_t a9 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 1, 100);
    uint64_t a10 = LocatorKey::GetLocatorKeyNum(1, 1, 0, 1, 1000);

    std::cout << "a1 = " << a1 << std::endl;
    std::cout << "a2 = " << a2 << std::endl;
    std::cout << "a3 = " << a3 << std::endl;
    std::cout << "a4 = " << a4 << std::endl;
    std::cout << "a5 = " << a5 << std::endl;
    std::cout << "a6 = " << a6 << std::endl;
    std::cout << "a7 = " << a7 << std::endl;
    std::cout << "a8 = " << a8 << std::endl;
    std::cout << "a9 = " << a9 << std::endl;
    std::cout << "a10 = " << a10 << std::endl;

    list.push_back(a1);
    list.push_back(a2);
    list.push_back(a3);
    list.push_back(a4);
    list.push_back(a5);
    list.push_back(a6);
    list.push_back(a7);
    list.push_back(a8);
    list.push_back(a9);
    list.push_back(a10);

    std::sort(list.begin(), list.end());

    for (uint64_t val : list) {
        std::cout << val << std::endl;
    }

    std::string ids = LocatorKey::GetIncrementalIds(
            list,"/root_path/R0C0S/test_dhl/dierow_1/swath_1/channel0.patch");

    std::cout << "ids should be 0,1,2,3,4,5, and it's = " << ids << std::endl;
}

void TestRandomLocatorKeyNumGenerator() {
    std::string dhl_name = "test_dhl";
    std::string root_path = "";

    std::vector<uint64_t> random_keys = LocatorKey::GenerateRandomLocatorKeys(dhl_name, root_path, 0.1);

    std::cout << "Number of keys generated: " << random_keys.size() << std::endl;

    for (uint64_t num : random_keys) {
        std::cout << num << std::endl;
    }
}

int main() {

    //TestGetIncrementalIds();

    TestRandomLocatorKeyNumGenerator();

    return 0;
}