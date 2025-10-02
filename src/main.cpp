#include <array>
#include <string>
#include <fstream>
#include <memory>
#include <filesystem>
#include "../include/streams.h"
#include "../include/sort.h"
#include <fcntl.h>


int main(int argc, char const *argv[]) {
    constexpr int FILE_COUNT = 3;
    const std::string SOURCE_PATH = "input.txt";
    const std::unique_ptr<IoDevice> in = std::make_unique<MmappedDevice>(SOURCE_PATH, O_RDWR, 0644);

    std::vector<std::unique_ptr<IoDevice> > b_files(FILE_COUNT);
    initialize_merge_files(b_files, "b");

    std::vector<std::unique_ptr<IoDevice> > c_files(FILE_COUNT);
    initialize_merge_files(c_files, "c");

    load_initial_series(in, b_files);

    std::unique_ptr<IoDevice> &result = external_sort(b_files, c_files);

    return 0;
}
