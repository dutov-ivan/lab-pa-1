//
// Created by dutov on 9/9/2025.
//

#ifndef EXTERNALSORTINGLAB1_SORT_H
#define EXTERNALSORTINGLAB1_SORT_H

#include <memory>
#include "streams.h"

std::unique_ptr<IoDevice> &external_sort(std::vector<std::unique_ptr<IoDevice> > &initial_from,
                                        std::vector<std::unique_ptr<IoDevice> > &initial_to);

void merge_many_into_many(const std::vector<std::unique_ptr<IoDevice> > *cur_fileset,
                          const std::vector<std::unique_ptr<IoDevice> > *opposite_fileset);

void merge_many_into_one(const std::vector<std::unique_ptr<IoDevice> > &cur_fileset,
                         const std::unique_ptr<IoDevice> &out_file,
                         uint32_t &active_files);

void load_initial_series(const std::unique_ptr<IoDevice> &in, std::vector<std::unique_ptr<IoDevice> > &b_files);

#endif //EXTERNALSORTINGLAB1_SORT_H
