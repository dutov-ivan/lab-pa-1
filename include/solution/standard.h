// Created by dutov on 10/3/2025.
//

#ifndef EXTERNALSORTINGLAB1_STANDARD_H
#define EXTERNALSORTINGLAB1_STANDARD_H

#include "common.h"
#include <vector>
#include <optional>
#include <string>

#include "io/reader.h"


// Forward-declare the classes from your API to reduce header dependencies.
class FileManager;
class BufferedWriter;

class StdSolution final : public Solution {
public:
    // Constructor now takes non-const references to allow file modification (e.g., clear).
    explicit StdSolution(std::vector<FileManager>& first_bucket,
                         std::vector<FileManager>& second_bucket);

    void load_initial_series(FileManager& source) override;

    const FileManager& external_sort() override;

protected:
    // Helper methods are virtual to allow overriding by derived classes.
     void merge_many_into_many(std::vector<FileManager>* cur_fileset,
                                      std::vector<FileManager>* opposite_fileset);

     void merge_many_into_one(
        std::vector<std::unique_ptr<Reader>>& readers,
        std::vector<std::optional<std::string>>& lookahead_lines,
        BufferedWriter& out_file);

private:
    // Member variables are non-const references to the file buckets.
    std::vector<FileManager>& first_bucket_;
    std::vector<FileManager>& second_bucket_;
};

#endif //EXTERNALSORTINGLAB1_STANDARD_H

