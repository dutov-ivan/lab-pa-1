#include <string>
#include <random>
#include <fstream>
#include <iostream>
using namespace std;

int main() {
    const size_t FILE_SIZE = 1 << 20; // 1 GB example
    const size_t BUFFER_SIZE = 1 << 20; // 1 MB buffer

    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<int> intDist(0, 100);
    uniform_int_distribution<int> lineLengthDist(1, 20);
    uniform_int_distribution<int> charDist('a', 'z');
    uniform_int_distribution<int> yearDist(2000, 2025);
    uniform_int_distribution<int> monthDist(1, 12);

    auto isLeap = [](int year) {
        return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    };

    ofstream out("input.txt");

    size_t currentSize = 0;
    while (currentSize < FILE_SIZE) {
        string buffer;
        buffer.reserve(BUFFER_SIZE);

        while (buffer.size() < BUFFER_SIZE) {
            // Generate row
            int num = intDist(gen);
            int length = lineLengthDist(gen);

            // Random string
            string line;
            line.reserve(length);
            for (int i = 0; i < length; ++i)
                line.push_back(static_cast<char>(charDist(gen)));

            // Date
            int year = yearDist(gen);
            int month = monthDist(gen);
            int daysInMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
            int maxDay = daysInMonth[month - 1];
            if (month == 2 && isLeap(year))
                maxDay = 29;
            uniform_int_distribution<int> dayDist(1, maxDay);
            int day = dayDist(gen);

            char dateBuf[11];
            snprintf(dateBuf, sizeof(dateBuf), "%04d/%02d/%02d", year, month, day);

            buffer += to_string(num) + "-" + line + "-" + string(dateBuf) + "\n";

            if (buffer.size() + 50 >= BUFFER_SIZE)
                break;
        }

        out.write(buffer.data(), buffer.size());
        currentSize += buffer.size();
    }

    out.close();
    return 0;
}
