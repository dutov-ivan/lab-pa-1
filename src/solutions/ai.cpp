#include "../include/solution/ai.h"
#include <iostream>
#include <algorithm>
#include <stdexcept>

// A helper struct for the k-way merge min-heap. It holds a line of data
// and the index of the source file it came from.
struct HeapNode {
    std::string line;
    size_t source_index;

    // Invert comparison to make std::priority_queue a min-heap.
    bool operator>(const HeapNode& other) const {
        return line > other.line;
    }
};

AiSolution::AiSolution(
    std::vector<std::unique_ptr<FileManager>> &b_files,
    std::vector<std::unique_ptr<FileManager>> &c_files)
    : b_files_(b_files), c_files_(c_files)
{
    // Initialize the thread pool for the parallel merge phase.
    // We leave one core free for the main thread and potential OS scheduling.
    const unsigned int thread_count = std::max(1u, std::thread::hardware_concurrency() - 1);
    for (unsigned int i = 0; i < thread_count; ++i) {
        thread_pool_.emplace_back([this] {
            while (true) {
                std::function<void()> task;
                if (task_queue_.try_pop(task)) {
                    task();
                } else if (stop_threads_) {
                    break;
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }
}

AiSolution::~AiSolution() {
    // Gracefully shut down the thread pool.
    stop_threads_ = true;
    for (auto& t : thread_pool_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

void AiSolution::load_initial_series(std::unique_ptr<FileManager> &source_file) {
    auto& input = source_file->input();

    MinHeap primary_heap;
    std::vector<std::string> secondary_storage;
    size_t current_heap_mem = 0;
    size_t current_secondary_mem = 0;

    // --- Phase 1: Fill the initial heap up to the memory budget ---
    std::string line;
    while (!input->is_end() && current_heap_mem < HEAP_MEMORY_BUDGET) {
        if (input->get_line(line)) {
            current_heap_mem += line.capacity(); // Approximation of memory usage
            primary_heap.push(std::move(line));
        }
    }

    if (primary_heap.empty()) {
        return; // Source file was empty.
    }

    size_t dest_file_idx = 0;
    OutputDevice* current_output = nullptr;

    // --- Phase 2: Generate runs using Replacement Selection ---
    while (!primary_heap.empty() || !secondary_storage.empty()) {
        if (primary_heap.empty()) {
            // The current run has ended. Start a new run.
            // Swap the secondary storage into the primary heap.
            for (auto& s : secondary_storage) {
                primary_heap.push(std::move(s));
            }
            secondary_storage.clear();
            current_heap_mem = current_secondary_mem;
            current_secondary_mem = 0;

            if (current_output) {
                current_output->flush();
            }
        }

        // Ensure we have an active output file for the current run.
        if (!current_output || primary_heap.size() == secondary_storage.size()) {
             if (dest_file_idx >= b_files_.size()) {
                throw std::runtime_error("Not enough temporary files for initial runs.");
            }
            current_output = b_files_[dest_file_idx]->output().get();
            // Start a new run in the next file, wrapping around if necessary.
            dest_file_idx = (dest_file_idx + 1);
            if (dest_file_idx >= b_files_.size()) dest_file_idx = 0;
        }

        // Pop the minimum element and write it to the current run.
        std::string min_element = std::move(const_cast<std::string&>(primary_heap.top()));
        primary_heap.pop();
        current_heap_mem -= min_element.capacity();
        current_output->write(min_element + "\n");

        // Read the next line from the source.
        if (!input->is_end() && input->get_line(line)) {
            // If the new element can be part of the current run, add it to the heap.
            // Otherwise, place it in storage for the next run.
            if (line >= min_element) {
                current_heap_mem += line.capacity();
                primary_heap.push(std::move(line));
            } else {
                current_secondary_mem += line.capacity();
                secondary_storage.push_back(std::move(line));
            }
        }
    }

    // Final flush for the last written file.
    if(current_output) {
        current_output->flush();
    }
}

const std::unique_ptr<FileManager>& AiSolution::external_sort() {
    auto *from_bucket = &b_files_;
    auto *to_bucket = &c_files_;

    while (count_active_files(*from_bucket) > 1) {
        // Perform a parallel merge pass from one bucket to the other.
        parallel_merge_pass(*from_bucket, *to_bucket);

        // Prepare for the next pass: clear source files and swap bucket roles.
        for (auto& file : *from_bucket) {
            file->clear();
        }
        std::swap(from_bucket, to_bucket);
    }

    // Find the single remaining file with the sorted data.
    for (const auto& file : *from_bucket) {
        if (!file->is_empty()) {
            return file;
        }
    }

    // If all files are empty (e.g., empty input), return the first one.
    return (*from_bucket)[0];
}

size_t AiSolution::count_active_files(const std::vector<std::unique_ptr<FileManager>>& bucket) {
    size_t count = 0;
    for (const auto& file : bucket) {
        if (!file->is_empty()) {
            count++;
        }
    }
    return count;
}

size_t AiSolution::parallel_merge_pass(
    std::vector<std::unique_ptr<FileManager>>& from_bucket,
    std::vector<std::unique_ptr<FileManager>>& to_bucket)
{
    // Reset file cursors for reading and writing.
    for(auto& file : from_bucket) file->reset_cursor();
    for(auto& file : to_bucket) file->clear();

    std::vector<FileManager*> active_sources;
    for (auto& file : from_bucket) {
        if (!file->is_empty()) {
            active_sources.push_back(file.get());
        }
    }

    if (active_sources.empty()) return 0;

    // Number of merge tasks is limited by the number of available destination files.
    size_t num_tasks = to_bucket.size();
    if (num_tasks == 0) throw std::runtime_error("No destination files available for merging.");

    tasks_in_progress_ = 0;

    for (size_t i = 0; i < num_tasks; ++i) {
        std::vector<std::reference_wrapper<InputDevice>> source_group;

        // Distribute source files to tasks in a round-robin fashion.
        for (size_t j = i; j < active_sources.size(); j += num_tasks) {
            // FIX: Dereference the unique_ptr to get the InputDevice& for the reference_wrapper.
            source_group.push_back(*active_sources[j]->input());
        }

        if (source_group.empty()) {
            continue; // No sources assigned to this task, so skip.
        }

        FileManager* dest_fm = to_bucket[i].get();

        tasks_in_progress_++;
        // FIX: Push a simple lambda to the queue. No more std::packaged_task.
        task_queue_.push([this, source_group, dest_fm] {
            this->merge_group(source_group, *dest_fm->output());
            tasks_in_progress_--;
            cv_.notify_one();
        });
    }

    // Wait for all dispatched tasks to complete.
    {
        std::unique_lock<std::mutex> lock(sync_mutex_);
        cv_.wait(lock, [this] { return tasks_in_progress_ == 0; });
    }

    return count_active_files(to_bucket);
}


void AiSolution::merge_group(
    std::vector<std::reference_wrapper<InputDevice>> sources,
    OutputDevice& destination)
{
    if (sources.empty()) return;

    // Use a min-heap for the k-way merge.
    std::priority_queue<HeapNode, std::vector<HeapNode>, std::greater<HeapNode>> merge_heap;

    // Prime the heap with the first line from each source.
    for (size_t i = 0; i < sources.size(); ++i) {
        std::string line;
        if (sources[i].get().get_line(line)) {
            merge_heap.push({std::move(line), i});
        }
    }

    // Main merge loop.
    while (!merge_heap.empty()) {
        HeapNode top = std::move(const_cast<HeapNode&>(merge_heap.top()));
        merge_heap.pop();

        destination.write(top.line + "\n");

        // Fetch the next line from the same source file and add it to the heap.
        std::string next_line;
        if (sources[top.source_index].get().get_line(next_line)) {
            merge_heap.push({std::move(next_line), top.source_index});
        }
    }
    destination.flush();
}
