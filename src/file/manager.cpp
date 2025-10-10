//
// Created by dutov on 10/3/2025.
//
#include "../../include/file/manager.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <filesystem>
#include <unistd.h>

UnixFileManager::UnixFileManager(std::string path, int flags, mode_t mode) : mode_(mode),
                                                                             path_(path), flags_(flags) {
    const int fd = ::open(path.c_str(), flags_, mode_);
    if (fd == -1) {
        ::perror("open");
        fd_ = -1;
        return;
    }
    fd_ = fd;
}


void UnixFileManager::clear() {
    close();

    const int fd = ::open(path_.c_str(), flags_ | O_TRUNC, mode_);
    if (fd == -1) {
        ::perror("open");
        fd_ = -1;
        return;
    }
    fd_ = fd;
}

void UnixFileManager::close() {
    ::close(fd_);
    fd_ = -1;
}

void UnixFileManager::delete_file() {
    close();
    std::filesystem::remove(path_);
}

int UnixFileManager::get_fd() const {
    return fd_;
}


bool UnixFileManager::is_empty() {
    struct stat sb{};
    if (::fstat(fd_, &sb) == -1) {
        perror("fstat");
        return true;
    }

    const size_t filesize = sb.st_size;
    return filesize == 0;
}

void initialize_merge_files(std::vector<std::unique_ptr<FileManager> > &files, const std::string &prefix) {
    for (size_t i = 0; i < files.size(); i++) {
        files[i] = std::make_unique<UnixFileManager>(prefix + std::to_string(i),
                                                     O_RDWR | O_TRUNC | O_CREAT, 0777);
    }
}
