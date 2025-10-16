#pragma once

#include "common.h"
#include <queue>
#include <vector>
#include <string>
#include <memory>
#include <functional>

class AiSolution : public Solution {
public:
    /**
     * @brief Constructs an AiSolution instance.
     * @param first_bucket A reference to the first bucket of temporary files.
     * @param second_bucket A reference to the second bucket of temporary files.
     * @param memory_limit_bytes The strict upper bound for in-memory data structures.
     */
    AiSolution(std::vector<FileManager>& first_bucket,
               std::vector<FileManager>& second_bucket,
               std::size_t memory_limit_bytes);

    /**
     * @brief Phase 1: Generates initial sorted runs using the Replacement Selection algorithm.
     * @param in A Reader for the source data file.
     */
    void load_initial_series(Reader& in) override;

    /**
     * @brief Phase 2: Performs a k-way merge of runs until a single sorted file remains.
     * @return A reference to the FileManager containing the final sorted data.
     */
    FileManager& external_sort() override;

private:
    /**
     * @brief Performs a single k-way merge pass from a source bucket to a destination bucket.
     * @param source_bucket The source vector of FileManagers.
     * @param dest_bucket The destination vector of FileManagers.
     */
    void merge_pass(
        std::vector<FileManager>& source_bucket,
        std::vector<FileManager>& dest_bucket);

    /**
     * @brief Checks if the sorting process is complete.
     */
    bool is_sorted() const;

    // Type aliases for min-heaps used in sorting logic.
    using MinHeap = std::priority_queue<std::string, std::vector<std::string>, std::greater<std::string>>;
    using MergeHeapElement = std::pair<std::string, std::size_t>;
    struct MergeHeapComparator {
        bool operator()(const MergeHeapElement& a, const MergeHeapElement& b) const {
            return a.first > b.first;
        }
    };
    using MergeMinHeap = std::priority_queue<MergeHeapElement, std::vector<MergeHeapElement>, MergeHeapComparator>;

    std::size_t memory_limit_bytes_;
    std::size_t current_memory_usage_;
};

