#pragma once

#include <vector>
#include <string>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <atomic>

#include "common.h"
#include "../io/manager.h"
#include "../io/reader.h"
#include "../io/buffered_writer.h"

// A simple thread-safe queue for managing tasks.
template<typename T>
class ThreadSafeQueue {
public:
    void push(T value) {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(value));
    }

    bool try_pop(T& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return false;
        }
        value = std::move(queue_.front());
        queue_.pop();
        return true;
    }

private:
    std::queue<T> queue_;
    std::mutex mutex_;
};


/**
 * @class AiSolution
 * @brief An advanced external sorting implementation using Replacement Selection
 * for initial run generation and parallelized k-way merging.
 *
 * This solution optimizes the external sort process by:
 * 1.  Maximizing initial run length via a large in-memory min-heap (Replacement Selection),
 * which significantly reduces the number of required merge passes.
 * 2.  Leveraging multi-threading to perform concurrent merge operations during the
 * merge phase, aiming to saturate I/O bandwidth and utilize modern multi-core CPUs.
 */
class AiSolution : public Solution {
public:
    /**
     * @brief Constructs an AiSolution instance.
     * @param b_files A reference to the first bucket of temporary files.
     * @param c_files A reference to the second bucket of temporary files.
     */
    explicit AiSolution(
        std::vector<FileManager> &b_files,
        std::vector<FileManager> &c_files
    );

    ~AiSolution() override;

    /**
     * @brief Phase 1: Reads the source file and distributes initial sorted runs
     * into the first bucket of temporary files using Replacement Selection.
     * @param source_file The FileManager for the initial unsorted data.
     */
    void load_initial_series(FileManager &source_file) override;

    /**
     * @brief Phase 2: Iteratively merges runs from one bucket to another in parallel
     * until a single sorted run remains.
     * @return A const reference to the FileManager containing the final sorted data.
     */
    const FileManager &external_sort() override;

private:
    // Struct for the replacement selection heap. Caches the numeric key for sorting.
    struct HeapItem {
        std::string line;
        long long key;

        // Overload operator< to make std::priority_queue a max-heap on the key.
        // This produces runs sorted in descending order.
        bool operator<(const HeapItem& other) const {
            return key < other.key;
        }
    };
    // A max-heap for generating descending-order runs.
    using MaxHeap = std::priority_queue<HeapItem>;

    /**
     * @brief A single k-way merge task that merges multiple source files into one destination file.
     * @param sources A vector of Readers to read from.
     * @param destination The Writer to write the merged run to.
     */
    void merge_group(
        std::vector<std::reference_wrapper<Reader>> sources,
        BufferedWriter &destination
    );

    /**
     * @brief Manages a full, parallelized merge pass from a source bucket to a destination bucket.
     * @param from_bucket The bucket containing runs to be merged.
     * @param to_bucket The bucket to write new, longer runs into.
     * @return The number of runs produced in the destination bucket.
     */
    size_t parallel_merge_pass(
        std::vector<FileManager> &from_bucket,
        std::vector<FileManager> &to_bucket
    );

    // References to the file buckets provided by the framework.
    std::vector<FileManager> &b_files_;
    std::vector<FileManager> &c_files_;

    // --- Thread Pool & Synchronization ---
    std::vector<std::thread> thread_pool_;
    ThreadSafeQueue<std::function<void()>> task_queue_;
    std::atomic<bool> stop_threads_{false};

    // Synchronization for waiting on merge passes to complete.
    std::mutex sync_mutex_;
    std::condition_variable cv_;
    std::atomic<size_t> tasks_in_progress_{0};

    // Helper to calculate the number of active (non-empty) files in a bucket.
    size_t count_active_files(const std::vector<FileManager> &bucket);

    // Memory budget for the replacement selection heap (480 MiB).
    static constexpr size_t HEAP_MEMORY_BUDGET = 480 * 1024 * 1024;
};

