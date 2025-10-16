//
// Created by dutov on 10/16/2025.
//

#ifndef EXTERNALSORTINGLAB1_FAST_WRITER_H
#define EXTERNALSORTINGLAB1_FAST_WRITER_H

#include "buffered_writer.h"
struct FastWriterWrapper {
    BufferedWriter &writer;
    std::string outbuf;
    std::size_t buffer_limit;

    FastWriterWrapper(BufferedWriter &w, std::size_t limit_bytes)
        : writer(w), buffer_limit(limit_bytes) {
        outbuf.reserve(std::min<std::size_t>(64 * 1024, buffer_limit)); // start reserve
    }

    void push_line(std::string_view sv) {
        outbuf.append(sv);
        outbuf.push_back('\n');
        if (outbuf.size() >= buffer_limit) flush();
    }

    void flush() {
        if (!outbuf.empty()) {
            writer.write(outbuf);
            outbuf.clear();
        }
        writer.flush();
    }

    ~FastWriterWrapper() {
        flush();
    }
};
#endif //EXTERNALSORTINGLAB1_FAST_WRITER_H