//
// Created by dutov on 10/3/2025.
//

#include "../../include/solution/standard.h"
#include <limits>
#include <queue>
#include <cassert>

int get_key(const std::string &s) {
    size_t pos;
    int key = std::stoi(s, &pos); // stops at first non-digit
    if (pos == 0) {
        throw std::runtime_error("Could not extract key from line: " + s);
    }
    return key;
}

StdSolution::StdSolution(const std::vector<std::unique_ptr<FileManager> > &first_bucket,
                         const std::vector<std::unique_ptr<FileManager> > &second_bucket) : first_bucket_(first_bucket),
    second_bucket_(second_bucket) {
    assert(first_bucket_.size() == second_bucket_.size());
}

void StdSolution::load_initial_series(const std::unique_ptr<InputDevice> &in) {
    const std::vector<std::unique_ptr<FileManager> > &out_files = first_bucket_;
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

const FileManager &StdSolution::external_sort() {
    auto *cur_fileset = &first_bucket_;
    auto *opposite_fileset = &second_bucket_;
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

void StdSolution::merge_many_into_many(const std::vector<std::unique_ptr<FileManager> > *cur_fileset,
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

void StdSolution::merge_many_into_one(const std::vector<std::unique_ptr<FileManager> > &cur_fileset,
                                      const std::unique_ptr<OutputDevice> &out_file,
                                      uint32_t &active_files) {
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
