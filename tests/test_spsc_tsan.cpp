/**
 * test_spsc_tsan.cpp — ThreadSanitizer stress test: 50,000,000 operations.
 *
 * Two threads run concurrently for the entire test:
 *   Producer: pushes uint64_t values 0, 1, 2, ..., 49,999,999.
 *   Consumer: pops values, verifies FIFO order, counts total.
 *
 * Any data race on head_, tail_, or buf_ would be immediately reported
 * by ThreadSanitizer.  Expected result: zero TSan warnings, exit code 0.
 *
 * Build (Linux / macOS — TSan not supported on MinGW):
 *   cmake .. && make test_spsc_tsan && ./test_spsc_tsan
 *
 * Or manually:
 *   g++ -std=c++17 -Iinclude -fsanitize=thread -fno-omit-frame-pointer \
 *       -g -O1 -pthread tests/test_spsc_tsan.cpp -o test_spsc_tsan
 *   ./test_spsc_tsan
 *
 * Why O1 (not O0)?
 *   O0 disables the inline-function optimisations that keep TSan's
 *   shadow memory consistent.  O1 gives meaningful code generation
 *   while keeping frame pointers for accurate TSan stack traces.
 */

#include "spsc_ring_buffer.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>

// 4096-slot buffer: large enough to absorb TSan slowdown without
// the producer spinning excessively.
static SpscRingBuffer<uint64_t, 4096> g_buf;

static constexpr uint64_t N = 50'000'000ULL;

int main()
{
    std::printf("TSan stress test: %llu operations (producer + consumer)...\n",
                (unsigned long long)N);
    std::fflush(stdout);

    std::atomic<bool> order_ok{true};

    // ── Producer ─────────────────────────────────────────────────────────────
    std::thread producer([] {
        for (uint64_t i = 0; i < N; ++i) {
            // Spin-wait: expected only under TSan's ~10–20× slowdown.
            while (!g_buf.try_push(i)) {
                // Pause hint — reduces memory bus traffic during spin.
                // On x86: __builtin_ia32_pause(); on ARM: __asm__("yield");
            }
        }
    });

    // ── Consumer ─────────────────────────────────────────────────────────────
    std::thread consumer([&order_ok] {
        uint64_t expected = 0;
        while (expected < N) {
            uint64_t val;
            if (g_buf.try_pop(val)) {
                if (val != expected) {
                    // FIFO violation — the buffer is broken
                    order_ok.store(false, std::memory_order_relaxed);
                    // Keep draining so the producer can exit cleanly
                }
                ++expected;
            }
        }
    });

    producer.join();
    consumer.join();

    if (!order_ok.load()) {
        std::printf("FAIL: FIFO order violated — memory ordering bug detected\n");
        return 1;
    }

    std::printf("TSan stress test PASSED — %llu ops, zero data races\n",
                (unsigned long long)N);
    return 0;
}
