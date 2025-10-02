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

struct IoDevice {
    virtual ~IoDevice() = default;

    virtual bool get_line(std::string &line) = 0;

    virtual void skip(std::size_t bytes) = 0;

    virtual bool peek(std::string &line) = 0;

    virtual IoDevice &write(const std::string &data) = 0;

    virtual void reset_cursor() = 0;

    virtual bool is_open() = 0;

    virtual void clear() = 0;

    virtual void close() = 0;

    virtual void flush() = 0;

    virtual bool is_end() = 0;
};

class MmappedDevice : public IoDevice {
public:
    explicit MmappedDevice(const std::string &path, int flags, mode_t mode, bool temporary = false);

    bool get_line(std::string &line) override;

    IoDevice &write(const std::string &data) override;

    void reset_cursor() override;

    bool is_open() override;

    void clear() override;

    void close() override;

    void flush() override;

    bool is_end() override;

    bool peek(std::string &line) override;

private:
    void initialize();

    void map_next_chunk();

    char *map_chunk(std::size_t chunk_index);

    void unmap_chunk(char *ptr, std::size_t chunk_index) const;

    std::size_t this_chunk_size(std::size_t i) const;

public:
    void skip(std::size_t bytes) override;

private:
    size_t last_chunk_size_;
    size_t chunk_count_;
    size_t chunk_size_;
    size_t file_size_;
    size_t cur_chunk_;
    size_t offset_ = -1;
    const std::string &path_;
    int flags_;
    mode_t mode_;
    int fd_;
    bool was_ended_ = false;
    char *data_ = nullptr;
    char *next_data_ = nullptr;
};

class FileStream final : public IoDevice {
public:
    explicit FileStream(std::string path, std::ios::openmode mode, bool temporary = false);

    ~FileStream() override;

    bool get_line(std::string &line) override;

    IoDevice &write(const std::string &data) override;

    void reset_cursor() override;

    bool is_open() override;

    void clear() override;

    void close() override;

    void flush() override;

    bool is_end() override;

    void copy_contents_to(const std::string &path);

    bool peek(std::string &line) override;

    void skip(std::size_t bytes) override;

private:
    std::fstream file_;
    std::string path_;
    std::ios::openmode mode_;
    bool temporary_;
};

void reset_files(const std::vector<std::unique_ptr<IoDevice> > &files);

void initialize_merge_files(std::vector<std::unique_ptr<IoDevice> > &files, const std::string &prefix);

void reset_file_cursors(const std::vector<std::unique_ptr<IoDevice> > &cur_fileset);

#endif //EXTERNALSORTINGLAB1_STREAMS_H
