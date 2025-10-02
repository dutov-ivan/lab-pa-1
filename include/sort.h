//
// Created by dutov on 9/9/2025.
//

#ifndef EXTERNALSORTINGLAB1_SORT_H
#define EXTERNALSORTINGLAB1_SORT_H

#include <memory>
#include "streams.h"

std::unique_ptr<FileManager> &external_sort(std::vector<std::unique_ptr<FileManager> > &initial_from,
                                            std::vector<std::unique_ptr<FileManager> > &initial_to);

void merge_many_into_many(const std::vector<std::unique_ptr<FileManager> > *cur_fileset,
                          const std::vector<std::unique_ptr<FileManager> > *opposite_fileset);

void merge_many_into_one(const std::vector<std::unique_ptr<FileManager> > &cur_fileset,
                         const std::unique_ptr<OutputDevice> &out_file,
                         uint32_t &active_files);

void load_initial_series(const std::unique_ptr<InputDevice> &in,
                         const std::vector<std::unique_ptr<FileManager> > &out_files);

#endif //EXTERNALSORTINGLAB1_SORT_H
