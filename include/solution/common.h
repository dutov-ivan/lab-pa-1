//
// Created by dutov on 10/3/2025.
//

#ifndef EXTERNALSORTINGLAB1_SOLUTION_H
#define EXTERNALSORTINGLAB1_SOLUTION_H

#include <memory>
#include <vector>
#include "../io/manager.h"
#include "io/reader.h"

struct Solution {
public:
    virtual ~Solution() = default;

    Solution(std::vector<FileManager>& first_bucket, std::vector<FileManager>& second_bucket);

    virtual void load_initial_series(Reader &reader) = 0;
    virtual FileManager& external_sort() = 0;

protected:
    std::vector<FileManager>& first_;
    std::vector<FileManager>& second_;
};

inline Solution::Solution(std::vector<FileManager> &first_bucket,
    std::vector<FileManager> &second_bucket): first_(first_bucket), second_(second_bucket) {

}
#endif //EXTERNALSORTINGLAB1_SOLUTION_H
