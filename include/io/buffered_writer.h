#ifndef BUFFERED_WRITER_H
#define BUFFERED_WRITER_H

#include "writer.h"
#include <vector>
#include <string>
#include <memory>

/**
 * @class BufferedWriter
 * @brief A high-performance writer that buffers output to minimize system calls.
 *
 * This class wraps a low-level Writer to accumulate data in an in-memory
 * buffer. The data is only flushed to disk when the buffer is full,
 * or when flush() is called explicitly, or on destruction. This is ideal for
 * situations with many small, frequent write operations.
 */
class BufferedWriter {
public:
    // Default buffer size of 8 KiB is a common choice.
    static constexpr size_t DEFAULT_BUFFER_SIZE = 8192;

    /**
     * @brief Constructs a BufferedWriter.
     * @param fm The FileManager to write to.
     * @param buffer_size The size of the internal memory buffer.
     */
    explicit BufferedWriter(FileManager& fm, size_t buffer_size = DEFAULT_BUFFER_SIZE)
        : writer_(fm), buffer_(buffer_size) {}

    // On destruction, flush any remaining data in the buffer.
    ~BufferedWriter() {
        flush();
    }

    // Non-copyable
    BufferedWriter(const BufferedWriter&) = delete;
    BufferedWriter& operator=(const BufferedWriter&) = delete;

    /**
     * @brief Writes a string to the buffer. Flushes if the buffer is full.
     * @param s The string to write.
     */
    void write(const std::string& s) {
        const char* data = s.data();
        size_t len = s.size();

        // If the new data doesn't fit in the remaining buffer space, flush first.
        if (current_pos_ + len > buffer_.size()) {
            flush();
        }

        // If the data is larger than the entire buffer, write it directly.
        if (len > buffer_.size()) {
            writer_.write_all(data, len);
        } else {
            // Otherwise, copy the data into the buffer.
            std::copy(data, data + len, buffer_.data() + current_pos_);
            current_pos_ += len;
        }
    }

    /**
 * @brief Writes a string_view to the buffer. Flushes if the buffer is full.
 * @param sv The string_view to write.
 */
    void write(std::string_view sv) {
        const char* data = sv.data();
        size_t len = sv.size();

        // If the new data doesn't fit in the remaining buffer space, flush first.
        if (current_pos_ + len > buffer_.size()) {
            flush();
        }

        // If the data is larger than the entire buffer, write it directly.
        if (len > buffer_.size()) {
            writer_.write_all(data, len);
        } else {
            // Otherwise, copy the data into the buffer.
            std::copy(data, data + len, buffer_.data() + current_pos_);
            current_pos_ += len;
        }
    }

    /**
     * @brief Flushes the contents of the internal buffer to the disk.
     */
    void flush() {
        if (current_pos_ > 0) {
            writer_.write_all(buffer_.data(), current_pos_);
            current_pos_ = 0;
        }
    }

private:
    Writer writer_;
    std::vector<char> buffer_;
    size_t current_pos_{0};
};

#endif // BUFFERED_WRITER_H
