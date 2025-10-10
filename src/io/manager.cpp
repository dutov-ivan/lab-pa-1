#include "../include/io/manager.h"

#include <system_error>
#include <cerrno>
#include <cstring>

#ifdef _WIN32
  #include <fileapi.h>
  #include <handleapi.h>
  #include <synchapi.h>
  #include <winbase.h>
  #include <io.h>
  #include <cstdio>
#else
  #include <fcntl.h>
  #include <unistd.h>
  #include <sys/stat.h>
  #include <sys/types.h>
  #include <cstdio>
#endif

FileManager::FileManager(const std::string& path, bool create_if_missing, unsigned mode)
    : handle_(),
      path_(path),
      mode_(mode),
      opened_(false)
{
    open_impl(create_if_missing);
}

FileManager::~FileManager() {
    close_impl();
}

FileManager::FileManager(FileManager&& other) noexcept
    : handle_(other.handle_), path_(std::move(other.path_)), mode_(other.mode_), opened_(other.opened_) {
    other.handle_ = native_handle_t();
    other.opened_ = false;
}

FileManager& FileManager::operator=(FileManager&& other) noexcept {
    if (this != &other) {
        close_impl();
        handle_ = other.handle_;
        path_ = std::move(other.path_);
        mode_ = other.mode_;
        opened_ = other.opened_;
        other.handle_ = native_handle_t();
        other.opened_ = false;
    }
    return *this;
}

bool FileManager::is_open() const noexcept { return opened_; }
native_handle_t FileManager::native_handle() const noexcept { return handle_; }
const std::string& FileManager::path() const noexcept { return path_; }

void FileManager::open_impl(bool create_if_missing) {
#ifdef _WIN32
    // Use CreateFileA for a simple byte-oriented file.
    DWORD access = GENERIC_READ | GENERIC_WRITE;
    DWORD share = FILE_SHARE_READ | FILE_SHARE_WRITE;
    DWORD disp = create_if_missing ? OPEN_ALWAYS : OPEN_EXISTING;
    handle_ = CreateFileA(path_.c_str(), access, share, nullptr, disp, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle_ == INVALID_HANDLE_VALUE) {
        DWORD err = GetLastError();
        throw std::system_error(static_cast<int>(err), std::system_category(), "CreateFileA failed");
    }
    opened_ = true;
#else
    int flags = O_RDWR;
    if (create_if_missing) flags |= O_CREAT;
    handle_ = ::open(path_.c_str(), flags, static_cast<mode_t>(mode_));
    if (handle_ == -1) {
        int e = errno;
        throw std::system_error(e, std::generic_category(), std::string("open failed: ") + strerror(e));
    }
    opened_ = true;
#endif
}

void FileManager::close_impl() noexcept {
    if (!opened_) return;
#ifdef _WIN32
    if (handle_ && handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(handle_);
        handle_ = INVALID_HANDLE_VALUE;
    }
#else
    if (handle_ >= 0) {
        ::close(handle_);
        handle_ = -1;
    }
#endif
    opened_ = false;
}

void FileManager::close() {
    try { close_impl(); }
    catch(...) { /* destructor-safe */ }
}

void FileManager::remove_file() {
    close();
#ifdef _WIN32
    if (!DeleteFileA(path_.c_str())) {
        DWORD err = GetLastError();
        throw std::system_error(static_cast<int>(err), std::system_category(), "DeleteFileA failed");
    }
#else
    if (::unlink(path_.c_str()) != 0) {
        int e = errno;
        throw std::system_error(e, std::generic_category(), "unlink failed");
    }
#endif
}

void FileManager::clear() {
    if (!opened_) open_impl(true);
#ifdef _WIN32
    // set pointer to 0 and SetEndOfFile
    LARGE_INTEGER zero{};
    zero.QuadPart = 0;
    if (!SetFilePointerEx(handle_, zero, nullptr, FILE_BEGIN)) {
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "SetFilePointerEx failed");
    }
    if (!SetEndOfFile(handle_)) {
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "SetEndOfFile failed");
    }
#else
    if (ftruncate(handle_, 0) != 0) {
        int e = errno;
        throw std::system_error(e, std::generic_category(), "ftruncate failed");
    }
    // reset cursor to beginning:
    if (lseek(handle_, 0, SEEK_SET) == (off_t)-1) {
        int e = errno;
        throw std::system_error(e, std::generic_category(), "lseek failed");
    }
#endif
}

void FileManager::reset_cursor() {
    if (!opened_) open_impl(true);
#ifdef _WIN32
    LARGE_INTEGER zero{}; zero.QuadPart = 0;
    if (!SetFilePointerEx(handle_, zero, nullptr, FILE_BEGIN)) {
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "SetFilePointerEx failed");
    }
#else
    if (lseek(handle_, 0, SEEK_SET) == (off_t)-1) {
        int e = errno;
        throw std::system_error(e, std::generic_category(), "lseek failed");
    }
#endif
}

std::uint64_t FileManager::size() const {
    if (!opened_) {
        // try to open readonly just to query size
        // but keep it simple: throw -- caller should open first
        return 0;
    }
#ifdef _WIN32
    LARGE_INTEGER fileSize;
    if (!GetFileSizeEx(handle_, &fileSize)) {
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "GetFileSizeEx failed");
    }
    return static_cast<std::uint64_t>(fileSize.QuadPart);
#else
    struct stat st;
    if (fstat(handle_, &st) != 0) {
        int e = errno;
        throw std::system_error(e, std::generic_category(), "fstat failed");
    }
    return static_cast<std::uint64_t>(st.st_size);
#endif
}

bool FileManager::is_empty() const {
    return size() == 0;
}
