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
    const std::unique_ptr<FileManager> in_manager = std::make_unique<UnixFileManager>(SOURCE_PATH, O_RDWR, 0644);
    std::vector<std::unique_ptr<FileManager> > b_files(FILE_COUNT);
    initialize_merge_files(b_files, "b");

    std::vector<std::unique_ptr<FileManager> > c_files(FILE_COUNT);
    initialize_merge_files(c_files, "c");

    load_initial_series(in_manager->input(), b_files);

    for (int i = 0; i < FILE_COUNT; i++) {
        b_files[i]->output()->flush();
        b_files[i]->output()->reset_cursor();
    }

    const std::unique_ptr<FileManager> &result = external_sort(b_files, c_files);
    result->output()->flush();

    return 0;
}
