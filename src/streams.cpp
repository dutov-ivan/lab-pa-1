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

MmappedDevice::MmappedDevice(const std::string &path, int flags, mode_t mode, bool temporary) : path_(path),
    flags_(flags), mode_(mode) {
    initialize();
}


bool MmappedDevice::get_line(std::string &line) {
    assert(data_);
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
        return true;
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

IoDevice &MmappedDevice::write(const std::string &data) {
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
    return *this;
}

void MmappedDevice::reset_cursor() {
    unmap_chunk(data_, cur_chunk_);
    unmap_chunk(next_data_, cur_chunk_);
    initialize();
}

bool MmappedDevice::is_open() {
    return fd_ != -1;
}

void MmappedDevice::clear() {
    reset_cursor();
    ::close(fd_);
    initialize();
}

void MmappedDevice::close() {
    ::close(fd_);
}

void MmappedDevice::flush() {
}

bool MmappedDevice::is_end() {
    return was_ended_;
}

bool MmappedDevice::peek(std::string &line) {
    assert(data_);
    if (was_ended_) return false;

    auto *start = data_ + offset_;
    size_t avail = this_chunk_size(cur_chunk_) - offset_;

    if (auto *nl = static_cast<char *>(::memchr(start, '\n', avail))) {
        line.assign(start, nl - start);
        return true;
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

void MmappedDevice::initialize() {
    const int fd = ::open(path_.c_str(), flags_, mode_);
    if (fd == -1) {
        ::perror("open");
        return;
    }
    fd_ = fd;

    struct stat sb{};
    if (::fstat(fd, &sb) == -1) {
        perror("fstat");
        ::close(fd);
        return;
    }

    const size_t filesize = sb.st_size;
    const size_t pagesize = ::sysconf(_SC_PAGE_SIZE);

    file_size_ = filesize;
    chunk_size_ = pagesize;
    last_chunk_size_ = filesize % chunk_size_;
    chunk_count_ = (filesize + chunk_size_ - 1) / chunk_size_;
    cur_chunk_ = 0;
    data_ = map_chunk(0);
    next_data_ = map_chunk(1);
    offset_ = 0;
}


void MmappedDevice::map_next_chunk() {
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

char *MmappedDevice::map_chunk(const std::size_t chunk_index) {
    const size_t bytes_to_map = std::min(chunk_size_, file_size_ - chunk_index * chunk_size_);
    const off_t offset = chunk_index * chunk_size_;

    void *mapped = ::mmap(nullptr, bytes_to_map, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, offset);
    if (mapped == MAP_FAILED) {
        ::perror("mmap");
        ::close(fd_);
        return nullptr;
    }

    char *data = static_cast<char *>(mapped);
    return data;
}

void MmappedDevice::unmap_chunk(char *ptr, const std::size_t chunk_index) const {
    if (!ptr || chunk_index >= chunk_count_) return;

    const size_t len = (chunk_index == chunk_count_ - 1) ? last_chunk_size_ : chunk_size_;
    ::munmap(ptr, len);
}

std::size_t MmappedDevice::this_chunk_size(std::size_t i) const {
    return i == chunk_count_ - 1 ? last_chunk_size_ : chunk_size_;
}

void MmappedDevice::skip(std::size_t bytes) {
}

FileStream::FileStream(std::string path,
                       const std::ios::openmode mode,
                       const bool temporary)
    : path_(std::move(path)),
      mode_(mode),
      temporary_(temporary) {
    clear();
}


FileStream::~FileStream() {
    if (file_.is_open()) file_.close();
    if (temporary_)
        std::filesystem::remove(path_);
}

bool FileStream::get_line(std::string &line) {
    if (!std::getline(file_, line)) {
        return false;
    }
    return true;
}

IoDevice &FileStream::write(const std::string &data) {
    if (!data.empty() && data.back() != '\n') {
        file_ << data << "\n";
    } else {
        file_ << data;
    }
    return *this;
}


void FileStream::reset_cursor() {
    file_.clear();
    file_.seekg(0, std::ios::beg);
    file_.seekp(0, std::ios::beg);
}

bool FileStream::is_open() {
    return file_.is_open();
}

void FileStream::clear() {
    file_.open(path_, mode_ | std::ios_base::trunc);
    if (!file_.is_open()) {
        throw std::runtime_error("Could not open file " + path_ + " in mode " + to_string(mode_) + " .");
    }
}

void FileStream::close() {
    file_.close();
}

void FileStream::flush() {
    file_.flush();
}

bool FileStream::is_end() {
    const int c = file_.peek();
    return c == EOF;
}

void FileStream::copy_contents_to(const std::string &path) {
    this->close();
    std::filesystem::copy(path_, path, std::filesystem::copy_options::overwrite_existing);
}

bool FileStream::peek(std::string &line) {
    const auto pos = file_.tellg();
    const bool result = this->get_line(line);
    file_.seekg(pos);
    return result;
}

void FileStream::skip(const std::size_t bytes) {
    file_.seekg(bytes, std::ios::cur);
}

void reset_files(const std::vector<std::unique_ptr<IoDevice> > &files) {
    for (const auto &file: files) {
        if (file->is_open()) {
            file->close();
        }

        file->clear();
    }
}

void initialize_merge_files(std::vector<std::unique_ptr<IoDevice> > &files, const std::string &prefix) {
    for (size_t i = 0; i < files.size(); i++) {
        files[i] = std::make_unique<MmappedDevice>(prefix + std::to_string(i),
                                                   O_RDWR | O_TRUNC | O_CREAT, 0777);
    }
}

void reset_file_cursors(const std::vector<std::unique_ptr<IoDevice> > &cur_fileset) {
    for (const auto &file: cur_fileset) {
        file->reset_cursor();
    }
}
