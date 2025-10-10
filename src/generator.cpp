// generate_big_file.cpp
// Compile: g++ -O3 -march=native -pipe -std=c++20 -pthread generate_big_file.cpp -o gen
// Example run: ./gen input.txt 1073741824 1048576
// (args: filename, total_bytes, chunk_size)

#include <charconv>
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>
#include <thread>
#include <vector>
#include <deque>
#include <chrono>

struct Chunk {
    std::vector<char> data;
};

class ChunkQueue {
    std::deque<std::unique_ptr<Chunk>> dq;
    std::mutex m;
    std::condition_variable cv;
    bool finished = false;
public:
    void push(std::unique_ptr<Chunk> c) {
        {
            std::lock_guard<std::mutex> lk(m);
            dq.push_back(std::move(c));
        }
        cv.notify_one();
    }
    std::unique_ptr<Chunk> pop_or_wait() {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&]{ return !dq.empty() || finished; });
        if (dq.empty()) return nullptr;
        auto c = std::move(dq.front());
        dq.pop_front();
        return c;
    }
    void set_finished() {
        {
            std::lock_guard<std::mutex> lk(m);
            finished = true;
        }
        cv.notify_all();
    }
};

static inline bool is_leap(int y) noexcept {
    return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
}

void produce_chunks(ChunkQueue &q,
                    std::atomic<uint64_t> &produced_bytes,
                    uint64_t target_bytes,
                    size_t chunk_size,
                    unsigned thread_index,
                    unsigned threads_count)
{
    // Per-thread RNG seeded from hardware + time + thread id
    std::random_device rd;
    uint64_t seed = uint64_t(rd()) ^ (uint64_t(std::chrono::steady_clock::now().time_since_epoch().count()) + (thread_index * 0x9e3779b97f4a7c15ULL));
    std::mt19937 gen(static_cast<uint32_t>(seed));

    std::uniform_int_distribution<int> numDist(0, 1 << 30);
    std::uniform_int_distribution<int> lineLenDist(1, 20);
    std::uniform_int_distribution<int> charDist('a', 'z');
    std::uniform_int_distribution<int> yearDist(2000, 2025);
    std::uniform_int_distribution<int> monthDist(1, 12);

    // Produce until we've signaled enough production (writer will stop when file is full)
    while (true) {
        uint64_t prev = produced_bytes.load(std::memory_order_relaxed);
        if (prev >= target_bytes) break;

        auto chunk = std::make_unique<Chunk>();
        chunk->data.reserve(chunk_size);

        while (chunk->data.size() + 128 < chunk_size) { // keep margin for line
            // quick check if overall target reached
            uint64_t now = produced_bytes.load(std::memory_order_relaxed);
            if (now >= target_bytes) break;

            // Generate line fields
            int num = numDist(gen);
            int strLen = lineLenDist(gen);

            // number -> ascii
            char tmp[32];
            auto append_number = [&](int v) {
                auto p = tmp;
                auto res = std::to_chars(tmp, tmp + sizeof(tmp), v);
                size_t n = res.ptr - tmp;
                chunk->data.insert(chunk->data.end(), tmp, tmp + n);
            };
            append_number(num);
            chunk->data.push_back('-');

            // random alpha string
            chunk->data.resize(chunk->data.size() + strLen);
            for (int i = 0; i < strLen; ++i)
                chunk->data[chunk->data.size() - strLen + i] = static_cast<char>(charDist(gen));
            chunk->data.push_back('-');

            // Date YYYY/MM/DD
            int year = yearDist(gen);
            int month = monthDist(gen);
            int daysInMonth[] = {31,28,31,30,31,30,31,31,30,31,30,31};
            int maxd = daysInMonth[month-1];
            if (month == 2 && is_leap(year)) maxd = 29;
            std::uniform_int_distribution<int> dayDist(1, maxd);
            int day = dayDist(gen);

            // append year (4 digits)
            auto res = std::to_chars(tmp, tmp + sizeof(tmp), year);
            size_t n = res.ptr - tmp;
            // pad if needed
            for (size_t i = 0; i < 4 - n; ++i) chunk->data.push_back('0');
            chunk->data.insert(chunk->data.end(), tmp, tmp + n);
            chunk->data.push_back('/');
            // month with two digits
            res = std::to_chars(tmp, tmp + sizeof(tmp), month);
            n = res.ptr - tmp;
            if (n == 1) chunk->data.push_back('0');
            chunk->data.insert(chunk->data.end(), tmp, tmp + n);
            chunk->data.push_back('/');
            // day with two digits
            res = std::to_chars(tmp, tmp + sizeof(tmp), day);
            n = res.ptr - tmp;
            if (n == 1) chunk->data.push_back('0');
            chunk->data.insert(chunk->data.end(), tmp, tmp + n);

            chunk->data.push_back('\n');

            // break if we are large enough
            if (chunk->data.size() >= chunk_size - 128) break;
        }

        // push chunk to queue (writer will cap writes to target_bytes)
        if (!chunk->data.empty()) {
            produced_bytes.fetch_add(chunk->data.size(), std::memory_order_relaxed);
            q.push(std::move(chunk));
        } else {
            break;
        }
    }
}

void writer_thread_func(ChunkQueue &q, const char *filename, std::atomic<uint64_t> &written_bytes, uint64_t target_bytes) {
    FILE *f = std::fopen(filename, "wb");
    if (!f) {
        std::perror("fopen");
        q.set_finished();
        return;
    }

    while (written_bytes.load(std::memory_order_relaxed) < target_bytes) {
        auto chunk = q.pop_or_wait();
        if (!chunk) break; // queue finished and empty
        size_t to_write = chunk->data.size();
        uint64_t remaining = target_bytes - written_bytes.load(std::memory_order_relaxed);
        if (remaining == 0) break;
        if (to_write > remaining) to_write = static_cast<size_t>(remaining);

        size_t written = std::fwrite(chunk->data.data(), 1, to_write, f);
        if (written != to_write) {
            std::perror("fwrite");
            break;
        }
        written_bytes.fetch_add(written, std::memory_order_relaxed);

        if (written_bytes.load(std::memory_order_relaxed) >= target_bytes) break;
    }

    std::fflush(f);
    std::fclose(f);
    q.set_finished(); // in case producers or consumers are still waiting
}

int main(int argc, char** argv) {
    const char *filename = "input.txt";
    uint64_t target_bytes = (1ULL << 28); // default 1 GB
    size_t chunk_size = (1ULL << 20); // default 1 MB

    if (argc >= 2) filename = argv[1];
    if (argc >= 3) target_bytes = std::stoull(argv[2]);
    if (argc >= 4) chunk_size = static_cast<size_t>(std::stoull(argv[3]));

    unsigned hw = std::thread::hardware_concurrency();
    unsigned producer_count = hw > 1 ? hw - 1 : 1;

    ChunkQueue queue;
    std::atomic<uint64_t> produced_bytes{0};
    std::atomic<uint64_t> written_bytes{0};

    // Launch writer
    std::thread writer([&]{ writer_thread_func(queue, filename, written_bytes, target_bytes); });

    // Launch producers
    std::vector<std::thread> producers;
    for (unsigned i = 0; i < producer_count; ++i) {
        producers.emplace_back([&, i]{
            produce_chunks(queue, produced_bytes, target_bytes, chunk_size, i, producer_count);
        });
    }

    // Wait producers
    for (auto &t : producers) if (t.joinable()) t.join();

    // No more production; notify writer to finish once queue drained
    queue.set_finished();

    if (writer.joinable()) writer.join();

    std::cout << "Requested bytes: " << target_bytes << ", written: " << written_bytes.load() << '\n';
    return 0;
}
