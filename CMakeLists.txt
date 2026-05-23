cmake_minimum_required(VERSION 3.14)
project(spsc_ring_buffer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# All targets share the header-only library
include_directories(include)

# ── IMU demo (1 kHz sample ingestion) ────────────────────────────────────────
add_executable(imu_demo src/imu_demo.cpp)
target_compile_options(imu_demo PRIVATE -O2)
target_link_libraries(imu_demo pthread)

# ── Correctness tests (no sanitisers — fast CI check) ────────────────────────
add_executable(test_spsc tests/test_spsc.cpp)
target_compile_options(test_spsc PRIVATE -O2 -g)
target_link_libraries(test_spsc pthread)

# ── ThreadSanitizer stress test (50M ops) ────────────────────────────────────
# NOTE: TSan requires GCC or Clang on Linux / macOS.
#       MinGW on Windows does not support -fsanitize=thread.
#       Build on Linux: cmake .. && make test_spsc_tsan && ./test_spsc_tsan
add_executable(test_spsc_tsan tests/test_spsc_tsan.cpp)
target_compile_options(test_spsc_tsan PRIVATE
    -fsanitize=thread
    -fno-omit-frame-pointer
    -g
    -O1)
target_link_options(test_spsc_tsan PRIVATE -fsanitize=thread)
target_link_libraries(test_spsc_tsan pthread)

# ── Throughput + write-latency benchmark ─────────────────────────────────────
add_executable(bench bench/bench.cpp)
target_compile_options(bench PRIVATE -O3 -march=native)
target_link_libraries(bench pthread)

# ── CTest integration ─────────────────────────────────────────────────────────
enable_testing()
add_test(NAME spsc_correctness COMMAND test_spsc)
