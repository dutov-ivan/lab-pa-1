#include <vector>
#include <string>
#include <string_view>
#include <queue>
#include <limits>
#include <cctype>
#include <memory>
#include <stdexcept>
#include <cstdint>
#include <cassert>
#include <algorithm>

#include "io/reader.h"
#include "../../include/io/buffered_writer.h"
#include "../../include/io/fast_writer.h"
#include "../../include/solution/modified.h"

// ---------- Configurable memory budget ----------
static constexpr std::size_t TOTAL_MEMORY_BUDGET_BYTES = 500ull * 1024 * 1024; // 500 MB
// Split budget: 84% readers, 12% writer, rest for overhead
static constexpr std::size_t READER_BUDGET = (TOTAL_MEMORY_BUDGET_BYTES * 84) / 100;
static constexpr std::size_t WRITER_BUFFER_BUDGET = (TOTAL_MEMORY_BUDGET_BYTES * 12) / 100;

// ---------- fast key parser ----------
inline int fast_get_key_sv(std::string_view s) {
    if (s.empty()) throw std::runtime_error("Cannot extract key from empty line.");
    const char *p = s.data();
    const char *end = p + s.size();
    bool neg = false;
    if (*p == '-') { neg = true; ++p; }
    int val = 0;
    bool any = false;
    while (p < end && *p >= '0' && *p <= '9') {
        any = true;
        val = val * 10 + (*p - '0');
        ++p;
    }
    if (!any) throw std::runtime_error("Could not extract key from line: " + std::string(s));
    return neg ? -val : val;
}

// ---------- Safe refill: build buffer first, then create string_views ----------
// This version is robust against reallocations.
static bool refill_segment_from_reader(InMemSegment &seg, Reader &reader, std::size_t max_bytes) {
    seg.clear();

    std::size_t reserve_size = std::max<std::size_t>(max_bytes, 1 << 20);
    seg.buffer.reserve(reserve_size);

    std::string_view line_view;
    std::size_t accumulated = 0;
    std::vector<std::size_t> offsets;
    offsets.reserve(256);

    // Read lines and append to seg.buffer. Save start offsets; do NOT create string_views yet.
    while (!reader.is_end() && reader.get_line(line_view)) {
        std::size_t before = seg.buffer.size();
        seg.buffer.append(line_view.data(), line_view.size());
        seg.buffer.push_back('\n');
        offsets.push_back(before);

        accumulated += line_view.size() + 1;
        if (accumulated >= max_bytes) break;
    }

    if (offsets.empty()) {
        return false;
    }

    // Create stable string_views into seg.buffer AFTER all appends are finished.
    seg.lines.reserve(offsets.size());
    const char* buf_data = seg.buffer.data();
    const char* buf_end = buf_data + seg.buffer.size();
    for (std::size_t off : offsets) {
        const char* start_ptr = buf_data + off;
        const char* cur = start_ptr;
        while (cur < buf_end && *cur != '\n') ++cur;
        std::size_t len = static_cast<std::size_t>(cur - start_ptr);
        seg.lines.emplace_back(start_ptr, len);
    }

    seg.next_index = 0;
    return true;
}

// ---------- ModifiedSolution implementation ----------
ModifiedSolution::ModifiedSolution(std::vector<FileManager> &first_bucket, std::vector<FileManager> &second_bucket)
    : first_bucket_(first_bucket), second_bucket_(second_bucket) {
    assert(first_bucket_.size() == second_bucket_.size());
}

void ModifiedSolution::load_initial_series(FileManager &source) {
    Reader reader(source);

    const size_t OUT_CNT = first_bucket_.size();
    std::vector<std::string> out_buffers(OUT_CNT);

    std::size_t per_writer_flush = std::max<std::size_t>(1 << 20, WRITER_BUFFER_BUDGET / std::max<size_t>(1, OUT_CNT));
    for (auto &s : out_buffers) s.reserve(std::min<std::size_t>(per_writer_flush, 1 << 20));

    int series_count = 0;
    int last_key = std::numeric_limits<int>::min();

    std::string_view line_view;
    size_t writer_idx = 0;
    while (reader.get_line(line_view)) {
        if (line_view.empty()) continue;
        int new_key = fast_get_key_sv(line_view);

        if (new_key < last_key) ++series_count;
        last_key = new_key;
        writer_idx = series_count % OUT_CNT;

        out_buffers[writer_idx].append(line_view.data(), line_view.size());
        out_buffers[writer_idx].push_back('\n');

        if (out_buffers[writer_idx].size() >= per_writer_flush) {
            BufferedWriter writer(first_bucket_[writer_idx]);
            writer.write(out_buffers[writer_idx]);
            writer.flush();
            out_buffers[writer_idx].clear();
        }
    }

    for (size_t i = 0; i < OUT_CNT; ++i) {
        if (!out_buffers[i].empty()) {
            BufferedWriter writer(first_bucket_[i]);
            writer.write(out_buffers[i]);
            writer.flush();
            out_buffers[i].clear();
        }
    }

    for (auto &file : first_bucket_) file.reset_cursor();
}

const FileManager &ModifiedSolution::external_sort() {
    auto *cur_fileset = &first_bucket_;
    auto *opposite_fileset = &second_bucket_;
    while (true) {
        size_t files_with_content = 0;
        int content_file_idx = -1;
        for (size_t i = 0; i < cur_fileset->size(); ++i) {
            if (!(*cur_fileset)[i].is_empty()) {
                files_with_content++;
                content_file_idx = static_cast<int>(i);
            }
        }
        if (files_with_content <= 1) {
            return (content_file_idx == -1) ? (*cur_fileset)[0] : (*cur_fileset)[content_file_idx];
        }
        merge_many_into_many(cur_fileset, opposite_fileset);
        for (auto &file : *opposite_fileset) { file.reset_cursor(); }
        for (auto &file : *cur_fileset) { file.clear(); }
        std::swap(cur_fileset, opposite_fileset);
    }
}

void ModifiedSolution::merge_many_into_many(std::vector<FileManager> *cur_fileset,
                                            std::vector<FileManager> *opposite_fileset) {
    const size_t FILE_COUNT = cur_fileset->size();
    if (FILE_COUNT == 0) return;

    std::size_t per_file_budget = std::max<std::size_t>(1 << 20, READER_BUDGET / FILE_COUNT);

    std::vector<std::unique_ptr<Reader>> readers;
    readers.reserve(FILE_COUNT);
    for (size_t i = 0; i < FILE_COUNT; ++i) {
        readers.push_back(std::make_unique<Reader>((*cur_fileset)[i]));
    }

    std::vector<InMemSegment> segments(FILE_COUNT);

    size_t output_idx = 0;

    for (size_t i = 0; i < FILE_COUNT; ++i) {
        refill_segment_from_reader(segments[i], *readers[i], per_file_budget);
    }

    while (true) {
        bool has_more = false;
        for (size_t i = 0; i < FILE_COUNT; ++i) {
            if (segments[i].has_next() || !readers[i]->is_end()) { has_more = true; break; }
        }
        if (!has_more) break;

        BufferedWriter bw((*opposite_fileset)[output_idx]);
        FastWriterWrapper fastWriter(bw, WRITER_BUFFER_BUDGET);

        merge_many_into_one(readers, segments, fastWriter, per_file_budget);

        output_idx = (output_idx + 1) % opposite_fileset->size();

        for (size_t i = 0; i < FILE_COUNT; ++i) {
            if (!segments[i].has_next() && !readers[i]->is_end()) {
                refill_segment_from_reader(segments[i], *readers[i], per_file_budget);
            }
        }
    }
}

struct PQEntry {
    int key;
    size_t file_idx;
    size_t line_index;
    bool operator>(PQEntry const &o) const {
        return key > o.key || (key == o.key && file_idx > o.file_idx);
    }
};

void ModifiedSolution::merge_many_into_one(
    std::vector<std::unique_ptr<Reader>> &readers,
    std::vector<InMemSegment> &segments,
    FastWriterWrapper &out_writer,
    std::size_t per_file_budget) {

    const size_t FILE_COUNT = readers.size();
    std::priority_queue<PQEntry, std::vector<PQEntry>, std::greater<>> pq;

    for (size_t i = 0; i < FILE_COUNT; ++i) {
        if (segments[i].has_next()) {
            std::string_view sv = segments[i].peek();
            int key = fast_get_key_sv(sv);
            pq.push(PQEntry{key, i, segments[i].next_index});
        }
    }

    if (pq.empty()) return;

    int last_key_written = std::numeric_limits<int>::min();

    while (!pq.empty()) {
        PQEntry e = pq.top(); pq.pop();

        InMemSegment &seg = segments[e.file_idx];
        assert(seg.next_index == e.line_index);
        std::string_view line_sv = seg.pop();
        out_writer.push_line(line_sv);
        last_key_written = e.key;

        if (seg.has_next()) {
            std::string_view next_sv = seg.peek();
            int next_key = fast_get_key_sv(next_sv);
            // --- FIX 1: Corrected Logic ---
            // A new key must be >= the last key to be part of the same sorted run.
            if (next_key >= last_key_written) {
                pq.push(PQEntry{next_key, e.file_idx, seg.next_index});
            }
        } else {
            size_t idx = e.file_idx;
            if (!readers[idx]->is_end()) {
                bool loaded = refill_segment_from_reader(seg, *readers[idx], per_file_budget);
                if (loaded && seg.has_next()) {
                    std::string_view head = seg.peek();
                    int head_key = fast_get_key_sv(head);
                    // --- FIX 2: Corrected Logic ---
                    // Same logic applies after refilling a buffer.
                    if (head_key >= last_key_written) {
                        pq.push(PQEntry{head_key, idx, seg.next_index});
                    }
                }
            }
        }
    }
}
