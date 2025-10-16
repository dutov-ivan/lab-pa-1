#include "../include/io/writer.h"
#include <system_error>
#include <cerrno>
#include <cstring>

#ifdef _WIN32
  #include <windows.h>
#else
  #include <unistd.h>
  #include <sys/types.h>
  #include <fcntl.h>
#endif

Writer::Writer(FileManager& fm) : fm_(fm) {}

std::size_t Writer::write_all(const char* data, std::size_t len) {
    if (!fm_.is_open()) {
        throw std::system_error(EINVAL, std::generic_category(), "file not open");
    }
    std::size_t written = 0;
#ifdef _WIN32
    HANDLE h = fm_.native_handle();
    if (h == INVALID_HANDLE_VALUE || h == nullptr) {
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "invalid handle");
    }
    while (written < len) {
        DWORD toWrite = static_cast<DWORD>(std::min<std::size_t>(len - written, static_cast<std::size_t>(0xFFFFFFFF)));
        DWORD actually = 0;
        BOOL ok = WriteFile(h, data + written, toWrite, &actually, nullptr);
        if (!ok) {
            DWORD err = GetLastError();
            throw std::system_error(static_cast<int>(err), std::system_category(), "WriteFile failed");
        }
        if (actually == 0 && toWrite != 0) {
            throw std::system_error(EIO, std::generic_category(), "zero bytes written");
        }
        written += actually;
    }
#else
    int fd = fm_.native_handle();
    if (fd < 0) throw std::system_error(EBADF, std::generic_category(), "invalid fd");
    const char* buf = data;
    std::size_t left = len;
    while (left > 0) {
        ssize_t n = ::write(fd, buf, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            int e = errno;
            throw std::system_error(e, std::generic_category(), "write failed");
        }
        buf += n;
        left -= static_cast<std::size_t>(n);
        written += static_cast<std::size_t>(n);
    }
#endif
    return written;
}

void Writer::reset_cursor() {
    fm_.reset_cursor();
}

void Writer::flush() {
#ifdef _WIN32
    HANDLE h = fm_.native_handle();
    if (!FlushFileBuffers(h)) {
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "FlushFileBuffers failed");
    }
#else
    int fd = fm_.native_handle();
    if (fd < 0) throw std::system_error(EBADF, std::generic_category(), "invalid fd");
    if (::fsync(fd) != 0) {
        int e = errno;
        throw std::system_error(e, std::generic_category(), "fsync failed");
    }
#endif
}
