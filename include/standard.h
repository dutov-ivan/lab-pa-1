//
// Created by dutov on 10/3/2025.
//

#ifndef EXTERNALSORTINGLAB1_SOLUTION_H
#define EXTERNALSORTINGLAB1_SOLUTION_H

#include <memory>
#include <vector>
#include "streams.h"

struct Solution {
public:
    virtual ~Solution() = default;

    virtual const std::unique_ptr<FileManager> &external_sort(
    ) = 0;

    virtual void load_initial_series(const std::unique_ptr<InputDevice> &in) = 0;

private:
    virtual void merge_many_into_many(const std::vector<std::unique_ptr<FileManager> > *cur_fileset,
                                      const std::vector<std::unique_ptr<FileManager> > *opposite_fileset) = 0;

    virtual void merge_many_into_one(const std::vector<std::unique_ptr<FileManager> > &cur_fileset,
                                     const std::unique_ptr<OutputDevice> &out_file,
                                     uint32_t &active_files) = 0;
};

class StdSolution final : public Solution {
public:
    explicit StdSolution(const std::vector<std::unique_ptr<FileManager> > &first_bucket,
                         const std::vector<std::unique_ptr<FileManager> > &second_bucket);

    void load_initial_series(const std::unique_ptr<InputDevice> &in) override;

    const std::unique_ptr<FileManager> &external_sort(
    ) override;

private:
    void merge_many_into_many(const std::vector<std::unique_ptr<FileManager> > *cur_fileset,
                              const std::vector<std::unique_ptr<FileManager> > *opposite_fileset) override;

    void merge_many_into_one(const std::vector<std::unique_ptr<FileManager> > &cur_fileset,
                             const std::unique_ptr<OutputDevice> &out_file, uint32_t &active_files) override;

    const std::vector<std::unique_ptr<FileManager> > &first_bucket_;
    const std::vector<std::unique_ptr<FileManager> > &second_bucket_;
};

#endif //EXTERNALSORTINGLAB1_SOLUTION_H
