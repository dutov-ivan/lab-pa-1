//
// Created by dutov on 10/3/2025.
//

#ifndef EXTERNALSORTINGLAB1_INPUT_H
#define EXTERNALSORTINGLAB1_INPUT_H

#include <unistd.h>
#include <string>

struct BufferedInputDevice {
    virtual ~BufferedInputDevice() = default;

    virtual std::size_t next_buffer(const char **buffer) = 0;

    virtual void delete_buffer() = 0;
};

class MmappedBufferedInputDevice : public BufferedInputDevice {
public:
    explicit MmappedBufferedInputDevice(int fd, std::size_t buffer_size);

    std::size_t next_buffer(const char **buffer) override;

private:
    void delete_buffer() override;

    std::size_t this_buffer_size(std::size_t i) const;

    const std::size_t PAGE_SIZE_ = ::sysconf(_SC_PAGE_SIZE);
    std::size_t buffer_size_;
    std::size_t cur_buffer_ = 0;
    std::size_t buffer_count_;
    const char *buffer_ = nullptr;

    int fd_;
    bool was_ended_ = false;
    std::size_t file_size_;
};

struct InputDevice {
    virtual ~InputDevice() = default;

    virtual bool get_line(std::string &line) = 0;

    virtual bool is_end() = 0;

    virtual bool peek(std::string &line) = 0;

    virtual void skip(std::size_t bytes) = 0;

    virtual void reset_cursor() = 0;
};


class MmappedInputDevice : public InputDevice {
public:
    explicit MmappedInputDevice(int fd);

    bool get_line(std::string &line) override;

    bool is_end() override;

    bool peek(std::string &line) override;

    void skip(std::size_t bytes) override;

    void reset_cursor() override;

private:
    void map_next_chunk();

    char *map_chunk(std::size_t chunk_index);

    void unmap_chunk(char *ptr, std::size_t chunk_index) const;

    std::size_t this_chunk_size(std::size_t i) const;

    size_t last_chunk_size_;
    size_t chunk_count_;
    size_t chunk_size_;
    size_t file_size_;
    size_t cur_chunk_;
    size_t offset_;
    int flags_;
    int fd_;
    bool was_ended_ = false;
    char *data_ = nullptr;
    char *next_data_ = nullptr;
};
#endif //EXTERNALSORTINGLAB1_INPUT_H
