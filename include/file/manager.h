//
// Created by dutov on 10/3/2025.
//

#ifndef EXTERNALSORTINGLAB1_MANAGER_H
#define EXTERNALSORTINGLAB1_MANAGER_H
#include <memory>
#include <vector>
#include <string>

struct FileManager {
    virtual ~FileManager() = default;

    virtual void clear() = 0;

    virtual void close() = 0;

    virtual void delete_file() = 0;

    virtual int get_fd() const = 0;

    virtual bool is_empty() = 0;
};

class UnixFileManager : public FileManager {
public:
    explicit UnixFileManager(std::string path, int flags, mode_t mode);

    void clear() override;

    void close() override;

    void delete_file() override;

    int get_fd() const override;

    bool is_empty() override;

private:
    int fd_;
    std::string path_;
    int flags_;
    mode_t mode_;
};

void initialize_merge_files(std::vector<std::unique_ptr<FileManager> > &files, const std::string &prefix);

#endif //EXTERNALSORTINGLAB1_MANAGER_H
