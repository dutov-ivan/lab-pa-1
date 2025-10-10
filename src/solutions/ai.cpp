#include "../../include/solution/ai.h"
#include "../../include/io/reader.h"
#include "../../include/io/writer.h"
#include <algorithm>
#include <stdexcept>
#include <functional>
#include <charconv> // For std::from_chars

// Helper function to extract the numeric key from the start of a line.
// It's robust and fast, using std::from_chars for conversion.
long long extract_key_sv(std::string_view sv) {
    size_t dash_pos = sv.find('-');
    if (dash_pos != std::string_view::npos) {
        sv = sv.substr(0, dash_pos);
    }
    long long key = 0;
    // std::from_chars is a non-throwing, locale-independent, fast conversion function.
    std::from_chars(sv.data(), sv.data() + sv.size(), key);
    return key;
}

// A helper struct for the k-way merge heap. It holds a line of data,
// its source index, and the cached numeric key for comparison.
struct HeapNode {
    std::string line;
    size_t source_index;
    long long key;

    // For max-heap behavior in std::priority_queue (sorts descending by key)
    bool operator<(const HeapNode& other) const {
        return key < other.key;
    }
};

AiSolution::AiSolution(
    std::vector<FileManager> &b_files,
    std::vector<FileManager> &c_files)
    : b_files_(b_files), c_files_(c_files)
{
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
    stop_threads_ = true;
    for (auto& t : thread_pool_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

void AiSolution::load_initial_series(FileManager &source_file) {
    Reader reader(source_file.path());

    // Use a max-heap to generate runs sorted in descending order.
    MaxHeap primary_heap;
    std::vector<HeapItem> secondary_storage;
    size_t current_heap_mem = 0;
    size_t current_secondary_mem = 0;

    std::string_view line_view;
    while (!reader.is_end() && current_heap_mem < HEAP_MEMORY_BUDGET) {
        if (reader.get_line(line_view)) {
            std::string line(line_view);
            long long key = extract_key_sv(line_view);
            current_heap_mem += line.capacity();
            primary_heap.push({std::move(line), key});
        }
    }

    if (primary_heap.empty()) {
        return;
    }

    size_t dest_file_idx = 0;
    std::unique_ptr<BufferedWriter> current_writer;

    while (!primary_heap.empty() || !secondary_storage.empty()) {
        if (primary_heap.empty()) {
            for (auto& item : secondary_storage) {
                primary_heap.push(std::move(item));
            }
            secondary_storage.clear();
            current_heap_mem = current_secondary_mem;
            current_secondary_mem = 0;
            if (current_writer) {
                current_writer->flush();
            }
        }

        if (!current_writer || primary_heap.size() == secondary_storage.size()) {
             if (dest_file_idx >= b_files_.size()) {
                throw std::runtime_error("Not enough temporary files for initial runs.");
            }
            if (current_writer) current_writer->flush();
            current_writer = std::make_unique<BufferedWriter>(b_files_[dest_file_idx]);
            dest_file_idx = (dest_file_idx + 1);
            if (dest_file_idx >= b_files_.size()) dest_file_idx = 0;
        }

        // Pop the max element (highest key)
        HeapItem max_element = primary_heap.top();
        primary_heap.pop();
        current_heap_mem -= max_element.line.capacity();
        current_writer->write(max_element.line + "\n");

        if (reader.get_line(line_view)) {
            long long current_key = extract_key_sv(line_view);
            std::string line(line_view);
            // For descending runs, the new key must be less than or equal to the last one written.
            if (current_key <= max_element.key) {
                current_heap_mem += line.capacity();
                primary_heap.push({std::move(line), current_key});
            } else {
                current_secondary_mem += line.capacity();
                secondary_storage.push_back({std::move(line), current_key});
            }
        }
    }

    if(current_writer) {
        current_writer->flush();
    }
}

const FileManager& AiSolution::external_sort() {
    auto *from_bucket = &b_files_;
    auto *to_bucket = &c_files_;

    while (count_active_files(*from_bucket) > 1) {
        parallel_merge_pass(*from_bucket, *to_bucket);
        for (auto& file : *from_bucket) {
            file.clear();
        }
        std::swap(from_bucket, to_bucket);
    }

    for (const auto& file : *from_bucket) {
        if (!file.is_empty()) {
            return file;
        }
    }

    if (from_bucket->empty()) {
        throw std::runtime_error("No files to return from external sort.");
    }
    return (*from_bucket)[0];
}

size_t AiSolution::count_active_files(const std::vector<FileManager>& bucket) {
    return std::count_if(bucket.begin(), bucket.end(), [](const auto& file) {
        return !file.is_empty();
    });
}

size_t AiSolution::parallel_merge_pass(
    std::vector<FileManager>& from_bucket,
    std::vector<FileManager>& to_bucket)
{
    for(auto& file : from_bucket) file.reset_cursor();
    for(auto& file : to_bucket) file.clear();

    std::vector<FileManager*> active_sources;
    for (auto& file : from_bucket) {
        if (!file.is_empty()) {
            active_sources.push_back(&file);
        }
    }

    if (active_sources.empty()) return 0;

    size_t num_tasks = to_bucket.size();
    if (num_tasks == 0) throw std::runtime_error("No destination files available for merging.");

    tasks_in_progress_ = 0;

    for (size_t i = 0; i < num_tasks; ++i) {
        std::vector<FileManager*> source_group_fm;
        for (size_t j = i; j < active_sources.size(); j += num_tasks) {
            source_group_fm.push_back(active_sources[j]);
        }

        if (source_group_fm.empty()) continue;

        FileManager* dest_fm = &to_bucket[i];

        tasks_in_progress_++;
        task_queue_.push([this, source_group_fm, dest_fm] {
            // Create reader unique_ptrs. Their lifetime is scoped to this lambda.
            std::vector<std::unique_ptr<Reader>> readers;
            readers.reserve(source_group_fm.size());
            for (const auto* fm : source_group_fm) {
                readers.emplace_back(std::make_unique<Reader>(fm->path()));
            }

            // Create reference_wrappers from the unique_ptrs to pass to merge_group.
            std::vector<std::reference_wrapper<Reader>> reader_refs;
            reader_refs.reserve(readers.size());
            for (const auto& r_ptr : readers) {
                reader_refs.push_back(*r_ptr);
            }

            BufferedWriter writer(*dest_fm);
            this->merge_group(reader_refs, writer);

            tasks_in_progress_--;
            cv_.notify_one();
        });
    }

    std::unique_lock<std::mutex> lock(sync_mutex_);
    cv_.wait(lock, [this] { return tasks_in_progress_ == 0; });

    return count_active_files(to_bucket);
}

void AiSolution::merge_group(
    std::vector<std::reference_wrapper<Reader>> sources,
    BufferedWriter &destination)
{
    if (sources.empty()) return;

    // Use a default priority_queue, which with the overloaded HeapNode::operator<
    // will behave as a max-heap, merging runs into descending order.
    std::priority_queue<HeapNode> merge_heap;

    std::string_view line_view;
    for (size_t i = 0; i < sources.size(); ++i) {
        if (sources[i].get().get_line(line_view)) {
            merge_heap.push({std::string(line_view), i, extract_key_sv(line_view)});
        }
    }

    while (!merge_heap.empty()) {
        HeapNode top = merge_heap.top();
        merge_heap.pop();

        destination.write(top.line + "\n");

        if (sources[top.source_index].get().get_line(line_view)) {
            merge_heap.push({std::string(line_view), top.source_index, extract_key_sv(line_view)});
        }
    }
    destination.flush();
}

