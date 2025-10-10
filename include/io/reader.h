/**
 * @file reader.h
 * @brief Defines the Reader class for high-performance, direct file reading.
 * @author Senior Software Engineer
 *
 * @section description
 * This class provides a highly optimized mechanism for reading line-delimited
 * data from a file. It bypasses abstraction layers to read directly from a
 * FILE stream into a large internal buffer, minimizing the overhead of system
 * calls.
 *
 * The key performance feature is the `get_line` method, which returns a
 * `std::string_view`. This view points directly into the internal buffer,
 * avoiding heap allocations for each line read. This is particularly effective
 * in algorithms where lines are read for comparison and then immediately
 * discarded.
 *
 * @section lifetime
 * The `std::string_view` returned by `get_line` is ephemeral. It is only
 * valid until the next call to `get_line` on the same `Reader` instance, as
 * a subsequent call may overwrite the internal buffer. Consumers of this class
 * must process the `string_view` immediately or copy its contents into a
 * `std::string` for longer-term storage.
 *
 * @section rationale
 * In the context of external sorting, I/O is the primary bottleneck. By
 * using a dedicated reader that is tightly coupled with the file system and
 * designed to minimize allocations, we can achieve significant speedups. This
 * design prioritizes raw performance over abstraction for the critical data-ingestion
 * path of the algorithm.
 */

#ifndef READER_H
#define READER_H

#include <string>
#include <string_view>
#include <vector>
#include <cstdio> // For FILE*

class Reader {
public:
    /**
     * @brief Constructs a Reader and opens the specified file.
     * @param filepath The path to the file to be read.
     * @throws std::runtime_error if the file cannot be opened.
     */
    explicit Reader(const std::string& filepath);

    /**
     * @brief Closes the file handle upon destruction.
     */
    ~Reader();

    // The class manages a raw file handle, so copy/move is disallowed.
    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;
    Reader(Reader&&) = delete;
    Reader& operator=(Reader&&) = delete;

    /**
     * @brief Reads the next line into a string_view.
     *
     * Provides a view into the internal buffer. For lines that span buffer
     * boundaries, the line is reconstructed in an internal string, and the
     * view points to that. The view is only valid until the next call.
     *
     * @param view A reference to a `std::string_view` to be populated.
     * @return `true` if a line was read, `false` on EOF.
     */
    bool get_line(std::string_view& view);

    /**
     * @brief Checks if the end of the file has been reached.
     * @return `true` if the source is exhausted and the buffer is empty.
     */
    bool is_end() const;

private:
    void fill_buffer();

    static constexpr size_t DEFAULT_BUFFER_SIZE = 64 * 1024;

    FILE* file_handle_ = nullptr;
    std::vector<char> buffer_;
    size_t buffer_pos_ = 0;
    size_t buffer_end_ = 0;
    bool eof_reached_ = false;

    // Used to reconstruct lines that span across buffer boundaries.
    std::string line_buffer_;
};

#endif // READER_H
