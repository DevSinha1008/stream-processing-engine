cmake_minimum_required(VERSION 3.20)
project(stream_engine CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG")

add_executable(bench bench/bench_main.cpp)
target_include_directories(bench PRIVATE include)
target_link_libraries(bench PRIVATE pthread)

add_executable(tests tests/tests.cpp)
target_include_directories(tests PRIVATE include)
target_link_libraries(tests PRIVATE pthread)

# Sanitizer builds: cmake -DSANITIZE=thread .. (or =address)
# Backs the "validated under TSan/ASan" claim — run the tests target under both.
if(SANITIZE STREQUAL "thread")
  target_compile_options(tests PRIVATE -fsanitize=thread -O1 -g)
  target_link_options(tests PRIVATE -fsanitize=thread)
elseif(SANITIZE STREQUAL "address")
  target_compile_options(tests PRIVATE -fsanitize=address -O1 -g)
  target_link_options(tests PRIVATE -fsanitize=address)
endif()
