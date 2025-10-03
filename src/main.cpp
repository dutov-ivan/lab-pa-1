#include <array>
#include <string>
#include <fstream>
#include <memory>
#include <filesystem>
#include "../include/streams.h"
#include <fcntl.h>
#include "../include/solution/common.h"
#include "solution/modified.h"

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

int main(int argc, char const *argv[]) {
    constexpr int FILE_COUNT = 3;
    const std::string SOURCE_PATH = "input.txt";
    const std::unique_ptr<FileManager> in_manager = std::make_unique<UnixFileManager>(SOURCE_PATH, O_RDWR, 0644);
    std::vector<std::unique_ptr<FileManager> > b_files(FILE_COUNT);
    initialize_merge_files(b_files, "b");

    std::vector<std::unique_ptr<FileManager> > c_files(FILE_COUNT);
    initialize_merge_files(c_files, "c");

    ActiveSolution solution(b_files, c_files);

    solution.load_initial_series(in_manager->input());

    const std::unique_ptr<FileManager> &result = solution.external_sort();
    result->output()->flush();

    return 0;
}
