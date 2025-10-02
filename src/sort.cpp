//
// Created by dutov on 9/9/2025.
//

#include "../include/sort.h"

#include <assert.h>
#include <string>
#include <limits>
#include <memory>
#include <queue>

#include "../include/streams.h"

int get_key(const std::string &s) {
    const size_t pos = s.find('-');
    const int firstNum = std::stoi(s.substr(0, pos));
    return firstNum;
}

void load_initial_series(const std::unique_ptr<InputDevice> &in,
                         const std::vector<std::unique_ptr<FileManager> > &out_files) {
    int series_count = 0;
    int last_key = std::numeric_limits<int>::max();
    std::string line;
    std::string buffer;

    while (in->get_line(line)) {
        if (line == "") {
            break;
        }
        const int new_key = get_key(line);
        const std::unique_ptr<OutputDevice> &os = out_files[series_count % out_files.size()]->output();
        if (new_key > last_key) {
            os->write(buffer);
            buffer.clear();
            series_count++;
        }
        last_key = new_key;
        buffer += line + "\n";
    }

    if (!buffer.empty()) {
        const std::unique_ptr<OutputDevice> &os = out_files[series_count % out_files.size()]->output();
        os->write(buffer);
    }

    for (const auto &file: out_files) {
        file->reset_cursor();
    }
}

void merge_many_into_many(const std::vector<std::unique_ptr<FileManager> > *cur_fileset,
                          const std::vector<std::unique_ptr<FileManager> > *opposite_fileset) {
    const size_t FILE_COUNT = cur_fileset->size();
    uint32_t active_files = (1u << FILE_COUNT) - 1;

    size_t output_count = 0;
    while (active_files != 0) {
        uint32_t temp = active_files;
        while (temp != 0) {
            const int output_idx = output_count % FILE_COUNT;
            merge_many_into_one(*cur_fileset, (*opposite_fileset)[output_idx]->output(), active_files);
            output_count++;

            temp &= temp - 1;
        }
    }
}

std::unique_ptr<FileManager> &external_sort(std::vector<std::unique_ptr<FileManager> > &initial_from,
                                            std::vector<std::unique_ptr<FileManager> > &initial_to) {
    assert(initial_from.size() == initial_to.size());
    auto *cur_fileset = &initial_from;
    auto *opposite_fileset = &initial_to;
    while (true) {
        if (!(*cur_fileset)[0]->is_empty() && (*cur_fileset)[1]->is_empty()) {
            return (*cur_fileset)[0];
        }
        merge_many_into_many(cur_fileset, opposite_fileset);

        for (const auto &file: *opposite_fileset) { file->output()->flush(); }

        for (const auto &file: *cur_fileset) {
            file->clear();
        }

        for (const auto &file: *opposite_fileset) {
            file->reset_cursor();
        }

        std::swap(cur_fileset, opposite_fileset);
    }
}

auto merge_many_into_one(const std::vector<std::unique_ptr<FileManager> > &cur_fileset,
                         const std::unique_ptr<OutputDevice> &out_file,
                         uint32_t &active_files) -> void {
    const size_t FILE_COUNT = cur_fileset.size();
    std::vector<std::string> lines(FILE_COUNT);
    std::priority_queue<std::pair<int, int> > pq; // (key, file_index)

    // Initial pass to retrieve all initial keys of the runs.
    for (int i = 0; i < FILE_COUNT; i++) {
        const auto &is = cur_fileset[i]->input();
        if (!is->get_line(lines[i])) {
            active_files &= ~(1 << i);
            continue;
        }
        const int key = get_key(lines[i]);
        pq.emplace(key, i);
    }

    while (!pq.empty()) {
        auto [key, idx] = pq.top();
        pq.pop();

        // Push the line with that element to the output
        out_file->write(lines[idx] + "\n");

        // Load new line from the file we got the line we pushed
        const auto &is = cur_fileset[idx]->input();
        if (is->peek(lines[idx])) {
            int new_key = get_key(lines[idx]);

            if (new_key > key) {
                // Invalid key = new series
                continue;
            }
            is->skip(lines[idx].size());
            pq.emplace(new_key, idx);
        } else {
            active_files &= ~(1u << idx);
        }
    }
}
