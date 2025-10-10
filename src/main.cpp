#include <array>
#include <string>
#include <fstream>
#include <memory>
#include <filesystem>
#include <fcntl.h>

#if SOLUTION_TYPE == 1
#include "../include/solution/standard.h"
using ActiveSolution = StdSolution;
#elif SOLUTION_TYPE == 2
#include "../include/solution/modified.h"
using ActiveSolution = ModifiedSolution;
#elif SOLUTION_TYPE == 3
#include "../include/solution/ai.h"
using ActiveSolution = AiSolution;
#else
#error "Unknown solution type"
#endif

std::vector<FileManager> initialize_merge_files(const std::string &prefix, std::size_t count) {
    std::vector<FileManager> files;
    for (std::size_t i = 0; i < count; i++) {
        const std::string path = prefix + std::to_string(i) + ".txt";
        if (std::filesystem::exists(path)) {
            std::filesystem::remove(path);
        }
        files.emplace_back(path, true, 0644);
    }
    return std::move(files);
}

int main(int argc, char const *argv[]) {
    constexpr int FILE_COUNT = 3;
    const std::string SOURCE_PATH = "input.txt";
    auto in_manager = FileManager(SOURCE_PATH, O_RDWR, 0644);
    std::vector<FileManager> b_files = initialize_merge_files( "b", FILE_COUNT);
    std::vector<FileManager> c_files = initialize_merge_files( "c", FILE_COUNT);

    ActiveSolution solution(b_files, c_files);

    solution.load_initial_series(in_manager);

    const FileManager &result = solution.external_sort();
    return 0;
}
