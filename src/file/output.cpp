//
// Created by dutov on 10/4/2025.
//

#include "../include/file/output.h"
#include <assert.h>
#include <cstring>
#include <fstream>
#include <memory>
#include <unistd.h>
#include <string.h>

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

void BufferWritingDevice::write(const char *data, std::size_t size) {
    ssize_t n = ::write(fd_, data, size);
    if (n < 0) {
        throw std::system_error(errno, std::generic_category(), "write failed");
    }
}
