//
// Created by dutov on 10/3/2025.
//

#ifndef EXTERNALSORTINGLAB1_SOLUTION_H
#define EXTERNALSORTINGLAB1_SOLUTION_H

#include <memory>
#include <vector>
#include "../streams.h"

struct Solution {
public:
    virtual ~Solution() = default;

    virtual const std::unique_ptr<FileManager> &external_sort(
    ) = 0;

    virtual void load_initial_series(std::unique_ptr<FileManager> &source_file) = 0;
};
#endif //EXTERNALSORTINGLAB1_SOLUTION_H
