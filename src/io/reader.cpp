#include "../../include/io/reader.h"
#include <stdexcept>
#include <cstring> // For memchr

Reader::Reader(const std::string& filepath) : buffer_(DEFAULT_BUFFER_SIZE) {
    file_handle_ = fopen(filepath.c_str(), "rb");
    if (!file_handle_) {
        throw std::runtime_error("Failed to open file: " + filepath);
    }
}

Reader::~Reader() {
    if (file_handle_) {
        fclose(file_handle_);
    }
}

bool Reader::is_end() const {
    return eof_reached_ && buffer_pos_ >= buffer_end_;
}

void Reader::fill_buffer() {
    if (eof_reached_) {
        return;
    }

    buffer_pos_ = 0;
    buffer_end_ = fread(buffer_.data(), 1, buffer_.size(), file_handle_);

    if (buffer_end_ < buffer_.size()) {
        if (ferror(file_handle_)) {
            throw std::runtime_error("Error reading from file.");
        }
        if (feof(file_handle_)) {
            eof_reached_ = true;
        }
    }
}

bool Reader::get_line(std::string_view& view) {
    if (buffer_pos_ >= buffer_end_) {
        fill_buffer();
        if (buffer_end_ == 0) {
            return false; // End of file
        }
    }

    // Fast path: Search for newline in the current buffer.
    char* current_pos = &buffer_[buffer_pos_];
    size_t remaining = buffer_end_ - buffer_pos_;
    char* newline_pos = static_cast<char*>(memchr(current_pos, '\n', remaining));

    if (newline_pos) {
        size_t len = newline_pos - current_pos;
        view = std::string_view(current_pos, len);
        buffer_pos_ += len + 1;
        return true;
    }

    // Slow path: Line spans buffers or is the last line in the file.
    line_buffer_.assign(current_pos, remaining);

    while (true) {
        fill_buffer();
        if (buffer_end_ == 0) {
            // EOF. The content of line_buffer_ is the last line.
            if (line_buffer_.empty()) {
                return false;
            }
            view = line_buffer_;
            return true;
        }

        newline_pos = static_cast<char*>(memchr(buffer_.data(), '\n', buffer_end_));

        if (newline_pos) {
            size_t len = newline_pos - buffer_.data();
            line_buffer_.append(buffer_.data(), len);
            buffer_pos_ = len + 1;
            view = line_buffer_;
            return true;
        } else {
            line_buffer_.append(buffer_.data(), buffer_end_);
            buffer_pos_ = buffer_end_;
        }
    }
}
