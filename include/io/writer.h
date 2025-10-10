#ifndef WRITER_H
#define WRITER_H

#include <string>
#include <cstddef>
#include <cstdint>

#include "manager.h"

// Minimal Writer that uses FileManager's native handle to write bytes robustly.
class Writer {
public:
    explicit Writer(FileManager& fm);
    ~Writer();

    // non-copyable
    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;

    // Write entire buffer (loops on partial writes). Returns total bytes written.
    // Throws std::system_error on fatal error.
    std::size_t write_all(const char* data, std::size_t len);

    std::size_t write_all(const std::string& s) {
        return write_all(s.data(), s.size());
    }

    // Reset cursor to beginning of file.
    void reset_cursor();

    // Flush to disk (fsync / FlushFileBuffers)
    void flush();

    // Close writer (also closes underlying file manager)
    void close();

private:
    FileManager& fm_;
};

#endif // WRITER_H
