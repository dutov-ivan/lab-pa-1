cmake_minimum_required(VERSION 3.10)
project(ExternalSortingLab1)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Set default build type
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif ()

# === Core library ===
add_library(ExternalSortLib
        src/io/reader.cpp
        include/io/manager.h
        src/io/manager.cpp
        include/io/writer.h
        src/io/writer.cpp
        include/io/fast_writer.h
)

target_include_directories(ExternalSortLib PUBLIC include)

# === Executables ===
add_executable(generator src/generator.cpp)
target_link_libraries(generator PRIVATE ExternalSortLib)

# main.cpp is reused, which class gets included depends on SOLUTION_TYPE
add_executable(std_main src/main.cpp src/solutions/standard.cpp)
target_link_libraries(std_main PRIVATE ExternalSortLib)
target_compile_definitions(std_main PRIVATE SOLUTION_TYPE=1)

add_executable(modified_main src/main.cpp src/solutions/modified.cpp)
target_link_libraries(modified_main PRIVATE ExternalSortLib)
target_compile_definitions(modified_main PRIVATE SOLUTION_TYPE=2)

add_executable(ai_main src/main.cpp src/solutions/ai.cpp
        include/io/buffered_writer.h)
target_link_libraries(ai_main PRIVATE ExternalSortLib)
target_compile_definitions(ai_main PRIVATE SOLUTION_TYPE=3)

# === GoogleTest ===
include(FetchContent)
FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest
        GIT_TAG 52eb8108c5bdec04579160ae17225d66034bd723
)
FetchContent_MakeAvailable(googletest)

# === Test executable ===
add_executable(tests test/ExternalSortTest.cpp)
target_link_libraries(tests PRIVATE ExternalSortLib gtest_main gmock_main)

enable_testing()
add_test(NAME ExternalSortTests COMMAND tests)

# === Build flags ===
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
