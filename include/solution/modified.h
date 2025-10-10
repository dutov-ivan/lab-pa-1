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

    void load_initial_series(const std::unique_ptr<BufferedInputDevice> &in) override;

    const FileManager &external_sort(
    ) override;

private:
    static void write_entries(const std::vector<std::pair<int, std::string> > &entries,
                              std::unique_ptr<OutputDevice> &out);

    void merge_many_into_many(const std::vector<std::unique_ptr<FileManager> > *cur_fileset,
                              const std::vector<std::unique_ptr<FileManager> > *opposite_fileset) override;

    void merge_many_into_one(const std::vector<std::unique_ptr<FileManager> > &cur_fileset,
                             const std::unique_ptr<OutputDevice> &out_file, uint32_t &active_files) override;

    const std::vector<std::unique_ptr<InputDevice> > &first_bucket_inputs_;
    const std::vector<std::unique_ptr<OutputDevice> > &first_bucket_outputs_;
};

#endif //EXTERNALSORTINGLAB1_MODIFIED_H
