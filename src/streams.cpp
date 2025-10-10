//
// Created by dutov on 9/9/2025.
//

#include "../include/streams.h"

#include <assert.h>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <filesystem>
#include <utility>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string.h>

MmappedInputDevice::MmappedInputDevice(int fd) : fd_(fd) {
    MmappedInputDevice::reset_cursor();
}

bool MmappedInputDevice::get_line(std::string &line) {
    if (!data_) return false;
    if (was_ended_) return false;

    auto *start = data_ + offset_;
    size_t avail = this_chunk_size(cur_chunk_) - offset_;


    if (auto *nl = static_cast<char *>(::memchr(start, '\n', avail))) {
        line.assign(start, nl - start);
        offset_ += nl - start + 1;
        return true;
    }

    if (!next_data_) {
        line.assign(start, avail);
        was_ended_ = true;
        return avail > 0;
    }

    const size_t next_chunk_size = this_chunk_size(cur_chunk_ + 1);
    // No newline in this chunk: check next chunk
    auto *nl2 = static_cast<char *>(::memchr(next_data_, '\n', next_chunk_size));
    if (!nl2) {
        // no newline at all: take remainder of both chunks safely
        line.assign(start, avail);
        line.append(next_data_, next_chunk_size);
        was_ended_ = true;
        return true;
    }

    // newline found in next chunk
    line.assign(start, avail);
    line.append(next_data_, nl2 - next_data_);
    size_t new_offset = nl2 - next_data_ + 1;
    map_next_chunk();
    offset_ = new_offset;
    return true;
}

bool MmappedInputDevice::is_end() {
    return was_ended_;
}

bool MmappedInputDevice::peek(std::string &line) {
    if (!data_) return false;

    if (was_ended_) return false;

    auto *start = data_ + offset_;
    size_t avail = this_chunk_size(cur_chunk_) - offset_;

    if (auto *nl = static_cast<char *>(::memchr(start, '\n', avail))) {
        line.assign(start, nl - start);
        return true;
    }

    if (!next_data_) {
        line.assign(start, avail);
        was_ended_ = true;
        return avail > 0;
    }

    // No newline in this chunk: check next chunk
    auto *nl2 = static_cast<char *>(::memchr(next_data_, '\n', this_chunk_size(cur_chunk_ + 1)));
    if (!nl2) {
        // no newline at all: take remainder of both chunks safely
        line.assign(start, avail);
        line.append(next_data_, this_chunk_size(cur_chunk_ + 1));
        was_ended_ = true;
        return true;
    }

    // newline found in next chunk
    line.assign(start, avail);
    line.append(next_data_, nl2 - next_data_);
    return true;
}

void MmappedInputDevice::skip(std::size_t bytes) {
    if (!data_) return;

    if (was_ended_) return;

    auto *start = data_ + offset_;
    size_t avail = this_chunk_size(cur_chunk_) - offset_;


    if (auto *nl = static_cast<char *>(::memchr(start, '\n', avail))) {
        offset_ += nl - start + 1;
        return;
    }

    if (!next_data_) {
        was_ended_ = true;
        return;
    }

    const size_t next_chunk_size = this_chunk_size(cur_chunk_ + 1);
    // No newline in this chunk: check next chunk
    auto *nl2 = static_cast<char *>(::memchr(next_data_, '\n', next_chunk_size));
    if (!nl2) {
        // no newline at all: take remainder of both chunks safely
        was_ended_ = true;
        return;
    }

    // newline found in next chunk
    size_t new_offset = nl2 - next_data_ + 1;
    map_next_chunk();
    offset_ = new_offset;
}

void MmappedInputDevice::reset_cursor() {
    unmap_chunk(data_, cur_chunk_);
    unmap_chunk(next_data_, cur_chunk_);

    struct stat sb{};
    if (::fstat(fd_, &sb) == -1) {
        perror("fstat");
        return;
    }

    const size_t filesize = sb.st_size;
    const size_t pagesize = ::sysconf(_SC_PAGE_SIZE);

    file_size_ = filesize;
    chunk_size_ = pagesize * 1024; // 4MB chunks
    last_chunk_size_ = filesize % chunk_size_;
    chunk_count_ = (filesize + chunk_size_ - 1) / chunk_size_;
    cur_chunk_ = 0;
    data_ = map_chunk(0);
    next_data_ = file_size_ < chunk_size_ ? nullptr : map_chunk(1);
    offset_ = 0;
}

void MmappedInputDevice::map_next_chunk() {
    assert(data_);
    unmap_chunk(data_, cur_chunk_);

    data_ = next_data_;
    cur_chunk_++;

    if (cur_chunk_ + 1 < chunk_count_) {
        next_data_ = map_chunk(cur_chunk_ + 1);
    } else {
        next_data_ = nullptr;
    }

    if (cur_chunk_ >= chunk_count_) {
        was_ended_ = true;
    }

    offset_ = 0;
}

char *MmappedInputDevice::map_chunk(std::size_t chunk_index) {
    if (file_size_ == 0) {
        return nullptr;
    }
    const size_t bytes_to_map = std::min(chunk_size_, file_size_ - chunk_index * chunk_size_);
    const off_t offset = chunk_index * chunk_size_;

    void *mapped = ::mmap(nullptr, bytes_to_map, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, offset);
    if (mapped == MAP_FAILED) {
        ::perror("mmap");
        ::close(fd_);
        fd_ = -1;
        return nullptr;
    }

    char *data = static_cast<char *>(mapped);
    return data;
}

void MmappedInputDevice::unmap_chunk(char *ptr, std::size_t chunk_index) const {
    if (!ptr || chunk_index >= chunk_count_) return;

    const size_t len = (chunk_index == chunk_count_ - 1) ? last_chunk_size_ : chunk_size_;
    ::munmap(ptr, len);
}

std::size_t MmappedInputDevice::this_chunk_size(std::size_t i) const {
    return i == chunk_count_ - 1 ? last_chunk_size_ : chunk_size_;
}

BufferWritingDevice::BufferWritingDevice(int fd) : fd_(fd) {
}

void BufferWritingDevice::write(const std::string &data) {
    const char *buf = data.c_str();
    size_t left = data.size();
    while (left > 0) {
        ssize_t n = ::write(fd_, buf, left);
        if (n < 0) {
            if (errno == EINTR) continue; // retry
            throw std::system_error(errno, std::generic_category(), "write failed");
        }
        buf += n;
        left -= n;
    }
}

void BufferWritingDevice::reset_cursor() {
    ::lseek(fd_, 0, SEEK_SET);
}

void BufferWritingDevice::flush() {
    if (::fsync(fd_) == -1) {
        ::perror("fsync");
    }
}

std::vector<std::unique_ptr<FileManager>> initialize_merge_files(const std::string &prefix, std::size_t count) {
    std::vector<std::unique_ptr<FileManager> > files(count);
    for (std::size_t i = 0; i < count; i++) {
        const std::string path = prefix + std::to_string(i) + ".txt";
        if (std::filesystem::exists(path)) {
            std::filesystem::remove(path);
        }
        files[i] = std::make_unique<UnixFileManager>(path, O_RDWR | O_CREAT, 0644);
    }
    return std::move(files);
}

UnixFileManager::UnixFileManager(std::string path, int flags, mode_t mode) : mode_(mode), path_(path), flags_(flags) {
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
    input_device_ = nullptr;
    output_device_ = nullptr;
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

std::unique_ptr<InputDevice> &UnixFileManager::input() {
    if (!input_device_) {
        init_input();
    }
    return input_device_;
}

std::unique_ptr<OutputDevice> &UnixFileManager::output() {
    if (!output_device_) {
        init_output();
    }
    return output_device_;
}

void UnixFileManager::reset_cursor() {
    if (input_device_) input_device_->reset_cursor();
    if (output_device_) output_device_->reset_cursor();
}

void UnixFileManager::init_input() {
    input_device_ = std::make_unique<MmappedInputDevice>(fd_);
}

void UnixFileManager::init_output() {
    output_device_ = std::make_unique<BufferWritingDevice>(fd_);
}

bool UnixFileManager::is_empty() {
    struct stat sb{};
    if (::fstat(fd_, &sb) == -1) {
        perror("fstat");
        std::abort();
        return true;
    }

    const size_t filesize = sb.st_size;
    return filesize == 0;
}
