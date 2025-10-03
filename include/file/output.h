//
// Created by dutov on 10/3/2025.
//

#ifndef EXTERNALSORTINGLAB1_OUTPUT_H
#define EXTERNALSORTINGLAB1_OUTPUT_H

#include <string>

struct OutputDevice {
    virtual ~OutputDevice() = default;

    virtual void write(const std::string &data) = 0;

    virtual void write(const char *data, std::size_t size) = 0;

    virtual void reset_cursor() = 0;

    virtual void flush() = 0;
};


class BufferWritingDevice : public OutputDevice {
public:
    explicit BufferWritingDevice(int fd);

    void write(const std::string &data) override;

    void reset_cursor() override;

    void flush() override;

    void write(const char *data, std::size_t size) override;

private:
    int fd_;
};
#endif //EXTERNALSORTINGLAB1_OUTPUT_H
