//
// Created by dutov on 10/3/2025.
//

#ifndef EXTERNALSORTINGLAB1_MODIFIED_H
#define EXTERNALSORTINGLAB1_MODIFIED_H

#include "common.h"

class ModifiedSolution final : public Solution {
public:
    explicit ModifiedSolution(const std::vector<std::unique_ptr<FileManager> > &first_bucket,
                              const std::vector<std::unique_ptr<FileManager> > &second_bucket);

    void load_initial_series(const std::unique_ptr<InputDevice> &in) override;

    const FileManager &external_sort(
    ) override;

private:
    void merge_many_into_many(const std::vector<std::unique_ptr<FileManager> > *cur_fileset,
                              const std::vector<std::unique_ptr<FileManager> > *opposite_fileset) override;

    void merge_many_into_one(const std::vector<std::unique_ptr<FileManager> > &cur_fileset,
                             const std::unique_ptr<OutputDevice> &out_file, uint32_t &active_files) override;

    const std::vector<std::unique_ptr<FileManager> > &first_bucket_;
    const std::vector<std::unique_ptr<FileManager> > &second_bucket_;
};

#endif //EXTERNALSORTINGLAB1_MODIFIED_H
