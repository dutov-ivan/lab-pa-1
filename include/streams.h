//
// Created by dutov on 9/9/2025.
//

#ifndef EXTERNALSORTINGLAB1_STREAMS_H
#define EXTERNALSORTINGLAB1_STREAMS_H
#include <string>
#include <ios>
#include <memory>
#include <fstream>
#include <vector>

struct FileManager;


struct InputDevice {
    virtual ~InputDevice() = default;

    virtual bool get_line(std::string &line) = 0;

    virtual bool is_end() = 0;

    virtual bool peek(std::string &line) = 0;

    virtual void skip(std::size_t bytes) = 0;

    virtual void reset_cursor() = 0;
};

struct OutputDevice {
    virtual ~OutputDevice() = default;

    virtual void write(const std::string &data) = 0;

    virtual void reset_cursor() = 0;

    virtual void flush() = 0;
};


struct FileManager {
    virtual ~FileManager() = default;

    virtual void clear() = 0;

    virtual void close() = 0;

    virtual void delete_file() = 0;

    virtual int get_fd() const = 0;

    virtual std::unique_ptr<InputDevice> &input() = 0;

    virtual std::unique_ptr<OutputDevice> &output() = 0;

    virtual void reset_cursor() = 0;

    virtual bool is_empty() = 0;
};


class UnixFileManager : public FileManager {
public:
    explicit UnixFileManager(std::string path, int flags, mode_t mode);

    void clear() override;

    void close() override;

    void delete_file() override;

    int get_fd() const override;

    std::unique_ptr<InputDevice> &input() override;

    std::unique_ptr<OutputDevice> &output() override;

    void reset_cursor() override;

private:
    void init_input();

    void init_output();

public:
    bool is_empty() override;

private:
    std::unique_ptr<InputDevice> input_device_ = nullptr;
    std::unique_ptr<OutputDevice> output_device_ = nullptr;

    int fd_;
    std::string path_;
    int flags_;
    mode_t mode_;
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


class BufferWritingDevice : public OutputDevice {
public:
    explicit BufferWritingDevice(int fd);

    void write(const std::string &data) override;

    void reset_cursor() override;

    void flush() override;

private:
    int fd_;
};

std::vector<std::unique_ptr<FileManager>> initialize_merge_files(const std::string &prefix, std::size_t count);

#endif //EXTERNALSORTINGLAB1_STREAMS_H
