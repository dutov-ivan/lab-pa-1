//
// Created by dutov on 9/9/2025.
//

#include "ExternalSortTest.h"

#include <gtest/gtest.h>
#include "sort.h"

std::vector<std::string> split(const std::string &s, const char delim) {
    std::vector<std::string> elems;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

class DummyStream final : public IoDevice {
public:
    explicit DummyStream(std::vector<int> keys) {
        is_open_ = true;
        keys_ = std::move(keys);
        lines.resize(keys_.size());
        std::transform(keys_.begin(), keys_.end(), lines.begin(),
                       [](const int k) { return std::to_string(k); });
    }

    bool get_line(std::string &line) override {
        if (pointer >= lines.size()) {
            return false;
        }
        line = lines[pointer];
        pointer++;
        return true;
    };

    IoDevice &write(const std::string &data) override {
        std::vector<std::string> new_lines = split(data, '\n');
        std::vector<int> new_keys(new_lines.size());

        std::transform(new_lines.begin(), new_lines.end(), new_keys.begin(),
                       [](const std::string &line) { return std::stoi(line); });

        // Ensure capacity if you're inserting inside the vector
        if (pointer < lines.size()) {
            // Replace existing entries, as far as possible
            const int replace_count = static_cast<int>(std::min(new_lines.size(), lines.size() - pointer));
            std::copy_n(new_lines.begin(), replace_count, lines.begin() + pointer);
            std::copy_n(new_keys.begin(), replace_count, keys_.begin() + pointer);

            // Append any remaining
            if (replace_count < new_lines.size()) {
                lines.insert(lines.end(), new_lines.begin() + replace_count, new_lines.end());
                keys_.insert(keys_.end(), new_keys.begin() + replace_count, new_keys.end());
            }
        } else {
            // Pointer is past current size, just append
            lines.insert(lines.end(), new_lines.begin(), new_lines.end());
            keys_.insert(keys_.end(), new_keys.begin(), new_keys.end());
        }

        pointer += static_cast<int>(new_lines.size());
        return *this;
    }

    void reset_cursor() override {
        pointer = 0;
    }

    bool is_open() override {
        return is_open_;
    }

    void clear() override {
        is_open_ = true;
        lines.clear();
        keys_.clear();
        pointer = 0;
    }

    void close() override {
        is_open_ = false;
    }

    void flush() override {
    };

    std::vector<int> &keys() {
        return keys_;
    }

    bool is_end() override {
        return pointer >= lines.size()
               && pointer >= keys_.size();
    }

    std::vector<int> next_keys() {
        std::vector<int> slice(keys_.begin() + pointer, keys_.end());
        return slice;
    }

    void skip(std::size_t bytes) override {
        pointer++;
    };

    bool peek(std::string &line) override {
        if (pointer >= lines.size()) {
            return false;
        }
        line = lines[pointer];
        return true;
    };

private:
    std::vector<std::string> lines;
    std::vector<int> keys_;
    size_t pointer = 0;
    bool is_open_;
};


std::unique_ptr<IoDevice> mkFile(const std::vector<int> &contents) {
    return std::make_unique<DummyStream>(contents);
};

void check_keys_against(const std::unique_ptr<IoDevice> &s, const std::vector<int> &keys) {
    ASSERT_NE(s, nullptr);
    auto *dummy_ptr = dynamic_cast<DummyStream *>(s.get());
    ASSERT_NE(nullptr, dummy_ptr);
    ASSERT_EQ(dummy_ptr->keys(), keys);
}

void check_next_keys_against(const std::unique_ptr<IoDevice> &s, const std::vector<int> &keys) {
    ASSERT_NE(s, nullptr);
    auto *dummy_ptr = dynamic_cast<DummyStream *>(s.get());
    ASSERT_NE(nullptr, dummy_ptr);
    ASSERT_EQ(dummy_ptr->next_keys(), keys);
}

TEST(LoadInitialSeries, LoadsProperly) {
    constexpr int FILE_COUNT = 3;
    const std::unique_ptr<IoDevice> in = mkFile({2, 7, 5, 1, 2});

    std::vector<std::unique_ptr<IoDevice> > source;
    source.push_back(mkFile({}));
    source.push_back(mkFile({}));
    source.push_back(mkFile({}));


    load_initial_series(in, source);

    check_keys_against(source[0], {2});
    check_keys_against(source[1], {7, 5, 1});
    check_keys_against(source[2], {2});
}

TEST(MergingKIntoOne, MergesLast) {
    std::vector<std::unique_ptr<IoDevice> > source;
    source.push_back(mkFile({2}));
    source.push_back(mkFile({7, 5, 1}));
    source.push_back(mkFile({2}));

    std::unique_ptr<IoDevice> dummy = mkFile({});
    uint32_t active_files = (1 << source.size()) - 1;
    merge_many_into_one(source, dummy, active_files);

    ASSERT_TRUE(source[0]->is_end());
    ASSERT_TRUE(source[1]->is_end());
    ASSERT_TRUE(source[2]->is_end());
    ASSERT_EQ(active_files, 0);

    check_keys_against(dummy, {7, 5, 2, 2, 1});
}

TEST(MergingKIntoOne, ProperMerge) {
    std::vector<std::unique_ptr<IoDevice> > source;
    source.push_back(mkFile({9, 2, 3, 7, 2}));
    source.push_back(mkFile({7, 5, 1, 10, 2}));
    source.push_back(mkFile({2, 9}));

    std::unique_ptr<IoDevice> dummy = mkFile({});
    uint32_t INITIAL_FILES = (1 << source.size()) - 1;
    uint32_t active_files = INITIAL_FILES;
    merge_many_into_one(source, dummy, active_files);

    ASSERT_EQ(active_files, INITIAL_FILES);

    check_next_keys_against(source[0], {3, 7, 2});
    check_next_keys_against(source[1], {10, 2});
    check_next_keys_against(source[2], {9});

    check_keys_against(dummy, {9, 7, 5, 2, 2, 1});

    std::unique_ptr<IoDevice> dummy2 = mkFile({});
    merge_many_into_one(source, dummy2, active_files);
    ASSERT_EQ(active_files, INITIAL_FILES - (1 << 1) - (1 << 2));
    check_next_keys_against(source[0], {7, 2});
    check_next_keys_against(source[1], {});
    check_next_keys_against(source[2], {});

    check_keys_against(dummy2, {10, 9, 3, 2});


    const std::unique_ptr<IoDevice> dummy3 = mkFile({});
    merge_many_into_one(source, dummy3, active_files);
    ASSERT_EQ(active_files, 0);
    ASSERT_TRUE(source[0]->is_end());
    ASSERT_TRUE(source[1]->is_end());
    ASSERT_TRUE(source[2]->is_end());

    check_keys_against(dummy3, {7, 2});
}

TEST(MergeKIntoM, ProperMerge) {
    std::vector<std::unique_ptr<IoDevice> > source;
    source.push_back(mkFile({9, 2, 3, 7, 2}));
    source.push_back(mkFile({7, 5, 1, 10, 2}));
    source.push_back(mkFile({2, 9}));

    std::vector<std::unique_ptr<IoDevice> > target;
    target.push_back(mkFile({}));
    target.push_back(mkFile({}));
    target.push_back(mkFile({}));

    merge_many_into_many(&source, &target);
    ASSERT_TRUE(source[0]->is_end());
    ASSERT_TRUE(source[1]->is_end());
    ASSERT_TRUE(source[2]->is_end());

    check_keys_against(target[0], {9, 7, 5, 2, 2, 1});
    check_keys_against(target[1], {10, 9, 3, 2});
    check_keys_against(target[2], {7, 2});
}

TEST(MergeKIntoM, LastMerge) {
    std::vector<std::unique_ptr<IoDevice> > source;
    source.push_back(mkFile({9, 7, 5, 2, 2, 1}));
    source.push_back(mkFile({10, 9, 3, 2}));
    source.push_back(mkFile({7, 2}));

    std::vector<std::unique_ptr<IoDevice> > target;
    target.push_back(mkFile({}));
    target.push_back(mkFile({}));
    target.push_back(mkFile({}));

    merge_many_into_many(&source, &target);
    ASSERT_TRUE(source[0]->is_end());
    ASSERT_TRUE(source[1]->is_end());
    ASSERT_TRUE(source[2]->is_end());

    check_keys_against(target[0], {10, 9, 9, 7, 7, 5, 3, 2, 2, 2, 2, 1});
    ASSERT_TRUE(target[1]->is_end());
    ASSERT_TRUE(target[2]->is_end());
}

TEST(ExternalSort, Sorts) {
    std::vector<std::unique_ptr<IoDevice> > source;
    source.push_back(mkFile({9, 2, 3, 7, 2}));
    source.push_back(mkFile({7, 5, 1, 10, 2}));
    source.push_back(mkFile({2, 9}));

    std::vector<std::unique_ptr<IoDevice> > target;
    target.push_back(mkFile({}));
    target.push_back(mkFile({}));
    target.push_back(mkFile({}));

    external_sort(source, target);
    check_keys_against(source[0], {10, 9, 9, 7, 7, 5, 3, 2, 2, 2, 2, 1});
    ASSERT_TRUE(source[1]->is_end());
    ASSERT_TRUE(source[2]->is_end());

    ASSERT_TRUE(target[0]->is_end());
    ASSERT_TRUE(target[1]->is_end());
    ASSERT_TRUE(target[2]->is_end());
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
