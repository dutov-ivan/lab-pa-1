//
// Created by dutov on 10/3/2025.
//

#include "../include/file/io.h"
#include <filesystem>

FileStream::FileStream(std::string path,
                       const std::ios::openmode mode,
                       const bool temporary)
    : path_(std::move(path)),
      mode_(mode),
      temporary_(temporary) {
    clear();
}


FileStream::~FileStream() {
    if (file_.is_open()) file_.close();
    if (temporary_)
        std::filesystem::remove(path_);
}

bool FileStream::get_line(std::string &line) {
    if (!std::getline(file_, line)) {
        return false;
    }
    return true;
}

IoDevice &FileStream::write(const std::string &data) {
    if (!data.empty() && data.back() != '\n') {
        file_ << data << "\n";
    } else {
        file_ << data;
    }
    return *this;
}


void FileStream::reset_cursor() {
    file_.clear();
    file_.seekg(0, std::ios::beg);
    file_.seekp(0, std::ios::beg);
}

bool FileStream::is_open() {
    return file_.is_open();
}

void FileStream::clear() {
    file_.open(path_, mode_ | std::ios_base::trunc);
    if (!file_.is_open()) {
        throw std::runtime_error("Could not open file " + path_ + " in mode " + to_string(mode_) + " .");
    }
}

void FileStream::close() {
    file_.close();
}

void FileStream::flush() {
    file_.flush();
}

bool FileStream::is_end() {
    const int c = file_.peek();
    return c == EOF;
}

void FileStream::copy_contents_to(const std::string &path) {
    this->close();
    std::filesystem::copy(path_, path, std::filesystem::copy_options::overwrite_existing);
}

bool FileStream::peek(std::string &line) {
    const auto pos = file_.tellg();
    const bool result = this->get_line(line);
    file_.seekg(pos);
    return result;
}

void FileStream::skip(const std::size_t bytes) {
    file_.seekg(bytes, std::ios::cur);
}
