// Created by dutov on 10/3/2025.
//
#include "../../include/solution/standard.h"
#include "io/reader.h"
#include "io/buffered_writer.h"
#include <limits>
#include <queue>
#include <cassert>
#include <vector>
#include <string_view>
#include <optional>
#include <memory>

inline int fast_atoi(const char* str, size_t len) noexcept {
    int val = 0;
    for (size_t i = 0; i < len && str[i] >= '0' && str[i] <= '9'; ++i)
        val = val * 10 + (str[i] - '0');
    return val;
}

inline int get_key(std::string_view s) {
    if (s.empty()) throw std::runtime_error("Empty line");
    return fast_atoi(s.data(), s.size());
}


StdSolution::StdSolution(std::vector<FileManager>& first_bucket,
                         std::vector<FileManager>& second_bucket)
    : first_bucket_(first_bucket), second_bucket_(second_bucket) {
    assert(first_bucket_.size() == second_bucket_.size());
}

void StdSolution::load_initial_series(FileManager& source) {
    Reader reader(source);

    // Using unique_ptrs to manage writer lifetimes correctly.
    std::vector<std::unique_ptr<BufferedWriter>> writers;
    for (FileManager& file : first_bucket_) {
        writers.push_back(std::make_unique<BufferedWriter>(file));
    }

    int series_count = 0;
    int last_key = std::numeric_limits<int>::min();
    std::string_view line_view;

    while (reader.get_line(line_view)) {
        if (line_view.empty()) {
            continue; // Skip empty lines
        }
        const int new_key = get_key(std::string(line_view));

        if (new_key < last_key) {
            series_count++;
        }
        last_key = new_key;

        writers[series_count % writers.size()]->write(line_view);
        writers[series_count % writers.size()]->write(std::string("\n"));
    }

    // Explicitly flush all writers before their destructors are called.
    for (auto& writer : writers) {
        writer->flush();
    }
    for (auto& file : first_bucket_) {
        file.reset_cursor();
    }
}

const FileManager& StdSolution::external_sort() {
    auto* cur_fileset = &first_bucket_;
    auto* opposite_fileset = &second_bucket_;

    while (true) {
        size_t files_with_content = 0;
        int content_file_idx = -1;
        for (size_t i = 0; i < cur_fileset->size(); ++i) {
            if (!(*cur_fileset)[i].is_empty()) {
                files_with_content++;
                content_file_idx = i;
            }
        }

        if (files_with_content <= 1) {
            return (content_file_idx == -1) ? (*cur_fileset)[0] : (*cur_fileset)[content_file_idx];
        }

        merge_many_into_many(cur_fileset, opposite_fileset);

        for (auto& file : *opposite_fileset) {
            file.reset_cursor();
        }
        for (auto& file : *cur_fileset) {
            file.clear();
        }

        std::swap(cur_fileset, opposite_fileset);
    }
}

void StdSolution::merge_many_into_many(std::vector<FileManager>* cur_fileset,
                                       std::vector<FileManager>* opposite_fileset) {
    const size_t FILE_COUNT = cur_fileset->size();
    size_t output_idx = 0;

    std::vector<std::unique_ptr<Reader>> readers;
    for (size_t i = 0; i < FILE_COUNT; ++i) {
        readers.push_back(std::make_unique<Reader>((*cur_fileset)[i]));
    }
    std::vector<std::optional<std::string>> lookahead_lines(FILE_COUNT, std::nullopt);

    while (true) {
        bool has_more_data = false;
        for(size_t i = 0; i < FILE_COUNT; ++i) {
            if (lookahead_lines[i].has_value() || !readers[i]->is_end()) {
                has_more_data = true;
                break;
            }
        }
        if (!has_more_data) {
            break;
        }

        BufferedWriter writer((*opposite_fileset)[output_idx]);

        merge_many_into_one(readers, lookahead_lines, writer);
        writer.flush();

        output_idx = (output_idx + 1) % opposite_fileset->size();
    }
}


void StdSolution::merge_many_into_one(
    std::vector<std::unique_ptr<Reader>>& readers,
    std::vector<std::optional<std::string>>& lookahead_lines,
    BufferedWriter& out_file)
{
    const size_t FILE_COUNT = readers.size();
    std::priority_queue<std::pair<int, int>, std::vector<std::pair<int, int>>, std::less<>> pq;

    for (int i = 0; i < FILE_COUNT; ++i) {
        if (lookahead_lines[i].has_value()) {
            pq.emplace(get_key(lookahead_lines[i].value()), i);
        } else {
            std::string_view view;
            if (!readers[i]->is_end() && readers[i]->get_line(view)) {
                std::string line = std::string(view);
                pq.emplace(get_key(line), i);
                lookahead_lines[i] = std::move(line); // Store it in the lookahead buffer
            }
        }
    }

    if (pq.empty()) {
        return;
    }

    int last_key_written = std::numeric_limits<int>::min();

    while (!pq.empty()) {
        auto [key, idx] = pq.top();
        pq.pop();

        out_file.write(lookahead_lines[idx].value());
        out_file.write(std::string("\n"));
        last_key_written = key;
        lookahead_lines[idx].reset();

        // Try to read the next line from the same file.
        std::string_view view;
        if (!readers[idx]->is_end() && readers[idx]->get_line(view)) {
            std::string next_line = std::string(view);
            int next_key = get_key(next_line);

            // If the next line is part of the current sorted run, add it to the queue.
            if (next_key <= last_key_written) {
                pq.emplace(next_key, idx);
                lookahead_lines[idx] = std::move(next_line);
            } else {
                // The run in this file has ended. Store the line we just read
                // in the lookahead buffer for the *next* merge operation.
                lookahead_lines[idx] = std::move(next_line);
            }
        }
    }
}
