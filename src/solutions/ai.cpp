#include "../../include/solution/ai.h"
#include "../../include/io/buffered_writer.h"
#include "../../include/io/reader.h"
#include <utility>

AiSolution::AiSolution(std::vector<FileManager>& first_bucket,
                       std::vector<FileManager>& second_bucket,
                       std::size_t memory_limit_bytes)
    : Solution(first_bucket, second_bucket),
      memory_limit_bytes_(memory_limit_bytes),
      current_memory_usage_(0) {}

void AiSolution::load_initial_series(Reader& in) {
    if (in.is_end()) {
        return;
    }

    // ARCHITECTURAL NOTE: The Solution API requires treating a container of abstract
    // objects `std::vector<FileManager>` as if it were a container of pointers.
    // This is not directly possible in C++. The following `reinterpret_cast` is a
    // low-level workaround to force the types to align with the required logic.
    auto& writers_bucket = reinterpret_cast<std::vector<std::unique_ptr<FileManager>>&>(first_);


    MinHeap min_heap;
    std::vector<std::string> next_run_elements;

    std::string_view line_view;
    while (current_memory_usage_ < memory_limit_bytes_ && in.get_line(line_view)) {
        std::string line(line_view);
        current_memory_usage_ += line.capacity() + sizeof(std::string);
        min_heap.push(std::move(line));
    }

    if (min_heap.empty()) {
        return;
    }

    std::size_t current_writer_idx = 0;
    auto writer = std::make_unique<BufferedWriter>(*writers_bucket[current_writer_idx]);

    while (!min_heap.empty()) {
        std::string smallest = std::move(const_cast<std::string&>(min_heap.top()));
        min_heap.pop();
        writer->write(smallest);
        current_memory_usage_ -= (smallest.capacity() + sizeof(std::string));

        if (in.get_line(line_view)) {
            std::string new_line(line_view);
            if (new_line >= smallest) {
                current_memory_usage_ += new_line.capacity() + sizeof(std::string);
                min_heap.push(std::move(new_line));
            } else {
                next_run_elements.push_back(std::move(new_line));
            }
        }

        if (min_heap.empty()) {
            writer->flush();

            if (next_run_elements.empty()) {
                break;
            }

            for (auto& elem : next_run_elements) {
                current_memory_usage_ += elem.capacity() + sizeof(std::string);
                min_heap.push(std::move(elem));
            }
            next_run_elements.clear();

            current_writer_idx = (current_writer_idx + 1) % writers_bucket.size();
            writers_bucket[current_writer_idx]->clear();
            writer = std::make_unique<BufferedWriter>(*writers_bucket[current_writer_idx]);
        }
    }
}

FileManager& AiSolution::external_sort() {
    auto* source_bucket_ref = &first_;
    auto* dest_bucket_ref = &second_;

    while (!is_sorted()) {
        merge_pass(*source_bucket_ref, *dest_bucket_ref);

        // The cast is necessary here to access the underlying pointers for clearing.
        auto& source_vector = reinterpret_cast<std::vector<std::unique_ptr<FileManager>>&>(*source_bucket_ref);
        for (auto& file : source_vector) {
            file->clear();
        }
        std::swap(source_bucket_ref, dest_bucket_ref);
    }

    auto& final_bucket = reinterpret_cast<std::vector<std::unique_ptr<FileManager>>&>(*source_bucket_ref);
    for(const auto& file : final_bucket) {
        if (!file->is_empty()) {
            return *file;
        }
    }

    auto& fallback_bucket = reinterpret_cast<std::vector<std::unique_ptr<FileManager>>&>(*dest_bucket_ref);
    for(const auto& file : fallback_bucket) {
        if (!file->is_empty()) {
            return *file;
        }
    }

    return *final_bucket[0];
}

void AiSolution::merge_pass(
    std::vector<FileManager>& source_bucket,
    std::vector<FileManager>& dest_bucket) {

    // Cast to the pointer-based vector type that the logic can work with.
    auto& source_files = reinterpret_cast<std::vector<std::unique_ptr<FileManager>>&>(source_bucket);
    auto& dest_files = reinterpret_cast<std::vector<std::unique_ptr<FileManager>>&>(dest_bucket);


    std::vector<std::unique_ptr<Reader>> readers;
    for (const auto& file : source_files) {
        if (!file->is_empty()) {
            readers.push_back(std::make_unique<Reader>(*file));
        }
    }

    if (readers.empty()) {
        return;
    }

    MergeMinHeap merge_heap;
    std::string_view line_view;

    for (std::size_t i = 0; i < readers.size(); ++i) {
        if (readers[i]->get_line(line_view)) {
            merge_heap.push({std::string(line_view), i});
        }
    }

    dest_files[0]->clear();
    BufferedWriter writer(*dest_files[0]);

    while (!merge_heap.empty()) {
        MergeHeapElement top = merge_heap.top();
        merge_heap.pop();

        writer.write(top.first);

        std::size_t reader_idx = top.second;
        if (readers[reader_idx]->get_line(line_view)) {
            merge_heap.push({std::string(line_view), reader_idx});
        }
    }
    writer.flush();
}

bool AiSolution::is_sorted() const {
    int non_empty_files = 0;
    // Here, we can iterate directly. We must use the dot operator `.` because
    // `file` is a reference to a FileManager object, not a pointer.
    for (const auto& file : first_) {
        if (!file.is_empty()) {
            non_empty_files++;
        }
    }
    for (const auto& file : second_) {
        if (!file.is_empty()) {
            non_empty_files++;
        }
    }
    return non_empty_files <= 1;
}

