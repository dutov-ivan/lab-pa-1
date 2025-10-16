#ifndef READER_H
#define READER_H

#include <string>
#include <string_view>
#include <vector>
#include <cstdio> // FILE*
#include "manager.h"

class Reader {
public:
    explicit Reader(const std::string& filepath);
    explicit Reader(FileManager& manager);

    ~Reader();

    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;
    Reader(Reader&&) = delete;
    Reader& operator=(Reader&&) = delete;

    bool get_line(std::string_view& view);
    bool is_end() const;

private:
    void fill_buffer();

    static constexpr size_t DEFAULT_BUFFER_SIZE = 64 * 1024;

    FILE* file_handle_ = nullptr;
    std::vector<char> buffer_;
    size_t buffer_pos_ = 0;
    size_t buffer_end_ = 0;
    bool eof_reached_ = false;
    bool owns_handle_ = false;

    std::string line_buffer_;
};

#endif // READER_H
