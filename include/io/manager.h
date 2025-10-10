#ifndef MANAGER_H
#define MANAGER_H

// Minimal, cross-platform FileManager (POSIX & Windows).
// Requires C++17 (for std::filesystem uses if desired by caller).

#include <string>
#include <system_error>
#include <cstdint>

#ifdef _WIN32
  #define WIN32_LEAN_AND_MEAN
  #include <windows.h>
  using native_handle_t = HANDLE;
#else
#include <sys/types.h>
using native_handle_t = int;
#endif

class FileManager {
public:
    // Open file at path. create_if_missing==true -> create if not exists.
    // mode is POSIX-style permission (ignored on Windows except during creation).
    FileManager(const std::string& path, bool create_if_missing = true, unsigned mode = 0644);
    ~FileManager();

    // non-copyable
    FileManager(const FileManager&) = delete;
    FileManager& operator=(const FileManager&) = delete;

    // movable
    FileManager(FileManager&& other) noexcept;
    FileManager& operator=(FileManager&& other) noexcept;

    bool is_open() const noexcept;
    native_handle_t native_handle() const noexcept;
    const std::string& path() const noexcept;

    // Close (safe to call multiple times)
    void close();

    // Delete the underlying file (closes first)
    void remove_file();

    // Truncate/clear file contents to zero length.
    void clear();

    // Reset cursor to start
    void reset_cursor();

    // Return file size in bytes
    std::uint64_t size() const;

    // Tests whether file is empty (size == 0)
    bool is_empty() const;

private:
    native_handle_t handle_;
    std::string path_;
    unsigned mode_;
    bool opened_;
    void open_impl(bool create_if_missing);
    void close_impl() noexcept;
};

#endif // MANAGER_H
