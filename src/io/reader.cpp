#include "../../include/io/reader.h"
#include <stdexcept>
#include <cstring>
#include <cctype>
#include <cstdio>
#include <cassert>
#include <iomanip>
#include <iostream>

#ifdef _WIN32
  #include <io.h>
  #define fdopen _fdopen
#else
  #include <unistd.h>
#endif

// NOTE: Ensure `std::string line_buffer_;` is a member of your Reader class in the header file.
Reader::Reader(const std::string& filepath)
    : buffer_(DEFAULT_BUFFER_SIZE),
      owns_handle_(true) {
 // Ensure member is initialized
    file_handle_ = fopen(filepath.c_str(), "rb");
    if (!file_handle_) {
        throw std::runtime_error("Failed to open file: " + filepath);
    }
}

Reader::Reader(FileManager& manager)
    : buffer_(DEFAULT_BUFFER_SIZE) { // Ensure member is initialized
    if (!manager.is_open()) {
        throw std::runtime_error("FileManager is not open.");
    }

#ifdef _WIN32
    int fd = _open_osfhandle(reinterpret_cast<intptr_t>(manager.native_handle()), 0);
#else
    int fd = manager.native_handle();
#endif

    file_handle_ = fdopen(fd, "rb");
    if (!file_handle_) {
        throw std::runtime_error("Failed to associate FILE* with FileManager handle.");
    }
}

Reader::~Reader() {
    if (owns_handle_ && file_handle_) {
        fclose(file_handle_);
    } else if (file_handle_ && !owns_handle_) {
        // For a non-owning handle, just flush to be safe, but don't close.
        fflush(file_handle_);
    }
}

bool Reader::is_end() const {
    // The end is reached only when the EOF flag is set AND the buffer has been fully consumed.
    return eof_reached_ && buffer_pos_ >= buffer_end_;
}

void Reader::fill_buffer() {
    if (eof_reached_) {
        return;
    }

    buffer_pos_ = 0;
    buffer_end_ = fread(buffer_.data(), 1, buffer_.size(), file_handle_);

    if (buffer_end_ < buffer_.size()) {
        // If we read less than a full buffer, we've either hit the end of the file or an error occurred.
        if (ferror(file_handle_)) {
            throw std::runtime_error("Error reading from file.");
        }
        // feof() is guaranteed to be true here if there was no error.
        eof_reached_ = true;
    }
}

bool Reader::get_line(std::string_view& view) {
    line_buffer_.clear();

    while (true) {
        // If there's data in the current buffer, process it.
        if (buffer_pos_ < buffer_end_) {
            const char* search_start = buffer_.data() + buffer_pos_;
            const size_t search_len = buffer_end_ - buffer_pos_;

            const char* newline_pos = static_cast<const char*>(memchr(search_start, '\n', search_len));

            if (newline_pos) {
                // --- Found a newline: the line is complete ---
                const size_t part_len = newline_pos - search_start;
                line_buffer_.append(search_start, part_len);
                buffer_pos_ += part_len + 1; // Move past the line and the '\n'

                // Handle Windows-style CRLF ('\r\n') endings
                if (!line_buffer_.empty() && line_buffer_.back() == '\r') {
                    line_buffer_.pop_back();
                }

                view = line_buffer_;
                return true;
            } else {
                // --- No newline found: append rest of buffer and continue ---
                line_buffer_.append(search_start, search_len);
                buffer_pos_ = buffer_end_; // Mark buffer as fully read
            }
        }

        // At this point, the buffer is fully consumed (buffer_pos_ >= buffer_end_).

        // If we've already reached the end of the file, we can't read more.
        if (eof_reached_) {
            if (!line_buffer_.empty()) {
                // This is the last line of the file, which doesn't end in a newline.
                view = line_buffer_;
                // We return true now. The next call to get_line() will clear
                // line_buffer_, see that eof_reached_ is true, and return false, correctly
                // signaling the end of iteration.
                return true;
            }
            // No final fragment and at EOF, so we are done.
            return false;
        }

        // If we're not at EOF, refill the buffer and loop again to process it.
        fill_buffer();
    }
}
