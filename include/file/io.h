//
// Created by dutov on 10/3/2025.
//

#ifndef EXTERNALSORTINGLAB1_IO_H
#define EXTERNALSORTINGLAB1_IO_H
#include <string>
#include <fstream>

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

#endif //EXTERNALSORTINGLAB1_IO_H
