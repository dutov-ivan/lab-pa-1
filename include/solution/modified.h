//
// Created by dutov on 10/3/2025.
//

#ifndef EXTERNALSORTINGLAB1_MODIFIED_H
#define EXTERNALSORTINGLAB1_MODIFIED_H

#include "common.h"
#include <vector>
#include <string>
#include <cassert>

#include "io/reader.h"
#include "io/fast_writer.h"

// ---------- In-memory chunk per input file ----------
// ---------- In-memory segment ----------
struct InMemSegment {
    std::string buffer; // contiguous storage containing lines (each terminated by '\n')
    std::vector<std::string_view> lines; // views into buffer
    std::size_t next_index = 0;

    bool has_next() const { return next_index < lines.size(); }
    std::string_view peek() const {
        assert(has_next());
        return lines[next_index];
    }
    std::string_view pop() {
        assert(has_next());
        return lines[next_index++];
    }
    void clear() {
        // keep capacity to avoid frequent reallocations; clear contents
        buffer.clear();
        lines.clear();
        next_index = 0;
    }
    std::size_t memory_usage() const {
        return buffer.capacity() + lines.capacity() * sizeof(std::string_view);
    }
};

class ModifiedSolution {
public:
    explicit ModifiedSolution(std::vector<FileManager>& first_bucket,
                         std::vector<FileManager>& second_bucket);

    void load_initial_series(FileManager& source);

    const FileManager& external_sort();

protected:
    // Helper methods are virtual to allow overriding by derived classes.
    void merge_many_into_many(std::vector<FileManager>* cur_fileset,
                                     std::vector<FileManager>* opposite_fileset);

    void merge_many_into_one(
    std::vector<std::unique_ptr<Reader>>& readers,
    std::vector<InMemSegment>& segments,
    FastWriterWrapper& out_writer,
    std::size_t per_file_budget);

private:
    // Member variables are non-const references to the file buckets.
    std::vector<FileManager>& first_bucket_;
    std::vector<FileManager>& second_bucket_;
};

#endif //EXTERNALSORTINGLAB1_MODIFIED_H
