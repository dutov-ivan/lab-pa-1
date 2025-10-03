//
// Created by dutov on 10/3/2025.
//

#ifndef EXTERNALSORTINGLAB1_SOLUTION_H
#define EXTERNALSORTINGLAB1_SOLUTION_H

#include <memory>
#include <vector>

#include "../file/manager.h"
#include "../file/input.h"
#include "../file/output.h"

struct Solution {
public:
    virtual ~Solution() = default;

    virtual const std::unique_ptr<FileManager> &external_sort(
    ) = 0;

    virtual void load_initial_series(const std::unique_ptr<BufferedInputDevice> &in) = 0;

private:
    virtual void merge_many_into_many(const std::vector<std::unique_ptr<FileManager> > *cur_fileset,
                                      const std::vector<std::unique_ptr<FileManager> > *opposite_fileset) = 0;

    virtual void merge_many_into_one(const std::vector<std::unique_ptr<FileManager> > &cur_fileset,
                                     const std::unique_ptr<OutputDevice> &out_file,
                                     uint32_t &active_files) = 0;
};
#endif //EXTERNALSORTINGLAB1_SOLUTION_H
