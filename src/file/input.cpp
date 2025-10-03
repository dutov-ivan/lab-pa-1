//
// Created by dutov on 10/4/2025.
//

#include "../include/file/input.h"
#include <assert.h>
#include <cstring>
#include <fstream>
#include <memory>
#include <filesystem>
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


    if (auto *nl = static_cast<char *>(std::memchr(start, '\n', avail))) {
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


MmappedBufferedInputDevice::MmappedBufferedInputDevice(int fd, std::size_t buffer_size) : fd_(fd),
    buffer_size_(buffer_size) {
    assert(buffer_size > 0 && buffer_size % PAGE_SIZE_ == 0);
    struct stat sb{};
    if (::fstat(fd_, &sb) == -1) {
        perror("fstat");
        return;
    }

    const size_t filesize = sb.st_size;
    file_size_ = filesize;
    buffer_count_ = (filesize + buffer_size_ - 1) / buffer_size_;
}

std::size_t MmappedBufferedInputDevice::next_buffer(const char **buffer) {
    if (was_ended_) return 0;

    std::size_t left = file_size_ - cur_buffer_ * buffer_size_;
    std::size_t size = std::min(buffer_size_, left);
    if (size == 0) {
        was_ended_ = true;
        return 0;
    }

    void *mmapped = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd_, cur_buffer_ * buffer_size_);
    if (mmapped == MAP_FAILED) {
        ::perror("mmap");
        return 0;
    }

    const char *data = static_cast<const char *>(mmapped);
    *buffer = data;
    return size;
}


void MmappedBufferedInputDevice::delete_buffer() {
    ::munmap(const_cast<void *>(static_cast<const void *>(buffer_)), this_buffer_size(cur_buffer_));
    buffer_ = nullptr;
}

std::size_t MmappedBufferedInputDevice::this_buffer_size(std::size_t i) const {
    return i == buffer_count_ - 1 ? file_size_ - buffer_count_ * buffer_size_ : buffer_size_;
}
