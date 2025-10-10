//
// Created by dutov on 10/3/2025.
//

#include "../../include/solution/modified.h"

#include <limits>
#include <charconv>
#include <queue>
#include <cassert>
#include <cstring>
#include <algorithm>
#include <stdexcept>

int get_key(const std::string &s) {
    size_t pos;
    int key = std::stoi(s, &pos); // stops at first non-digit
    if (pos == 0) {
        throw std::runtime_error("Could not extract key from line: " + s);
    }
    return key;
}

bool get_key_from_buffer(int &key, const char *buffer, std::size_t buffer_size, std::size_t &start) {
    const char *begin = buffer + start;
    const char *end = buffer + buffer_size; // or known buffer length

    int tmp;
    auto [ptr, ec] = std::from_chars(begin, end, tmp);

    if (ec != std::errc()) {
        return false; // invalid or out of range
    }

    if (ptr == begin) {
        return false; // no digits consumed
    }

    key = tmp;
    start = static_cast<std::size_t>(ptr - buffer); // update caller’s offset
    return true;
}

ModifiedSolution::ModifiedSolution(const std::vector<std::unique_ptr<FileManager> > &first_bucket,
                                   const std::vector<std::unique_ptr<FileManager> > &second_bucket) {
    assert(first_bucket.size() == second_bucket.size());
    std::vector<std::unique_ptr<InputDevice> > first_inputs(first_bucket.size());
    std::vector<std::unique_ptr<OutputDevice> > first_outputs(first_bucket.size());
    for (size_t i = 0; i < first_bucket.size(); ++i) {
        int fd = first_bucket[i]->get_fd();
        first_inputs[i] = std::make_unique<MmappedInputDevice>(fd);
        first_outputs[i] = std::make_unique<BufferWritingDevice>(fd);
    }

    std::vector<std::unique_ptr<InputDevice> > second_inputs(second_bucket.size());
    std::vector<std::unique_ptr<OutputDevice> > second_outputs(second_bucket.size());
    for (size_t i = 0; i < first_bucket.size(); ++i) {
        int fd = first_bucket[i]->get_fd();
        second_inputs[i] = std::make_unique<MmappedInputDevice>(fd);
        second_outputs[i] = std::make_unique<BufferWritingDevice>(fd);
    }
}

void ModifiedSolution::write_entries(const std::vector<std::pair<int, std::string> > &entries,
                                     std::unique_ptr<OutputDevice> &out) {
    constexpr size_t BUFFER_SIZE = 64 * 1024; // 64 KB buffer
    char buffer[BUFFER_SIZE];
    size_t pos = 0;

    for (size_t i = 0; i < entries.size(); ++i) {
        const auto &[num, str] = entries[i];
        std::string line = std::to_string(num) + "-" + str + "\n";

        if (pos + line.size() > BUFFER_SIZE) {
            out->write(buffer, pos);
            pos = 0;
        }

        std::memcpy(buffer + pos, line.data(), line.size());
        pos += line.size();
    }

    // flush remaining
    if (pos > 0) {
        out->write(buffer, pos);
    }
}

void ModifiedSolution::load_initial_series(const std::unique_ptr<BufferedInputDevice> &in) {
    const std::vector<std::unique_ptr<FileManager> > &out_files = first_bucket_;
    int series_count = 0;
    constexpr std::size_t MB_100 = 100 * 1024 * 1024;
    const char *buffer;
    std::size_t buffer_size;


    std::string remnant = "";
    while ((buffer_size = in->next_buffer(&buffer))) {
        std::vector<std::pair<int, std::string> > entries;
        std::size_t offset = 0;
        while (offset < buffer_size) {
            int key = 0;
            std::string line = "";

            if (!get_key_from_buffer(key, buffer, buffer_size, offset) && remnant.empty()) {
                throw std::runtime_error("Malformed input: could not read key from buffer");
            };

            auto nl = static_cast<const char *>(std::memchr(buffer + offset, '\n', buffer_size - offset));
            if (!nl) {
                remnant = std::string(buffer + offset, buffer_size - offset);
                continue;
            }


            if (!remnant.empty()) {
                line = remnant;
                remnant = "";
            }

            line += std::string(buffer + offset, nl - (buffer + offset));

            entries.emplace_back(key, line);

            offset += (nl - (buffer + offset)) + 1; // move past newline
        }

        std::sort(entries.begin(), entries.end(), [](const auto &a, const auto &b) {
            return a.first < b.first;
        });


        std::unique_ptr<OutputDevice> &os = out_files[series_count % out_files.size()]->output();
        write_entries(entries, os);
        series_count++;
    }

    for (const auto &file: out_files) {
        file->reset_cursor();
    }
}

const FileManager &ModifiedSolution::external_sort() {
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

void ModifiedSolution::merge_many_into_many(const std::vector<std::unique_ptr<FileManager> > *cur_fileset,
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

void ModifiedSolution::merge_many_into_one(const std::vector<std::unique_ptr<FileManager> > &cur_fileset,
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
