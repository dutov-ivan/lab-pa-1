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
        src/streams.cpp
        include/standard.h
        src/standard.cpp
)

target_include_directories(ExternalSortLib PUBLIC include)

# === Executables ===
add_executable(generator src/generator.cpp)
target_link_libraries(generator PRIVATE ExternalSortLib)

add_executable(main_exec src/main.cpp)
target_link_libraries(main_exec PRIVATE ExternalSortLib)

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
