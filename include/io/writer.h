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
    ~Writer() = default; // The writer does not own the file, so it should not close it.

    // non-copyable
    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;

    // Movable (but not move-assignable)
    Writer(Writer&& other) noexcept
        : fm_(other.fm_) {} // Move constructor works by re-binding the reference.

    Writer& operator=(Writer&&) = delete; // Cannot be move-assignable due to reference member.


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

    // The 'close' method has been removed. The FileManager is responsible for closing the file.

private:
    FileManager& fm_;
};

#endif // WRITER_H

