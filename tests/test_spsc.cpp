/**
 * test_spsc.cpp — Correctness tests for SpscRingBuffer.
 *
 * Five tests, single- and multi-threaded:
 *   1. FIFO ordering
 *   2. Full-buffer rejection
 *   3. Empty-buffer rejection
 *   4. Index wrap-around across power-of-2 boundary
 *   5. Concurrent 1M-operation integrity check
 *
 * Compile without -fsanitize=thread for fast CI runs.
 * The dedicated 50M TSan stress test lives in test_spsc_tsan.cpp.
 *
 * Build: cmake .. && make test_spsc && ./test_spsc
 * Exit code 0 = all tests passed.
 */

#include "spsc_ring_buffer.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <thread>

// ── Minimal test harness (no external deps) ───────────────────────────────────
static int g_passed = 0;
static int g_failed = 0;

#define CHECK(cond, msg)                                                \
    do {                                                                \
        if (cond) {                                                     \
            std::printf("  [PASS] %s\n", (msg)); ++g_passed;          \
        } else {                                                        \
            std::printf("  [FAIL] %s  (line %d)\n", (msg), __LINE__); \
            ++g_failed;                                                 \
        }                                                               \
    } while (0)

// ── Test 1: FIFO ordering ─────────────────────────────────────────────────────
static void test_fifo_order()
{
    std::printf("Test 1: FIFO ordering\n");
    SpscRingBuffer<int, 64> buf;

    for (int i = 0; i < 50; ++i)
        buf.try_push(i);

    bool ok = true;
    for (int i = 0; i < 50; ++i) {
        int v = -1;
        buf.try_pop(v);
        if (v != i) { ok = false; break; }
    }
    CHECK(ok, "push 0..49, pop returns 0..49 in order");
}

// ── Test 2: Full-buffer rejection ─────────────────────────────────────────────
static void test_full_buffer()
{
    std::printf("Test 2: Full-buffer rejection\n");
    // Capacity=8 → holds 7 items
    SpscRingBuffer<int, 8> buf;

    int i = 0;
    while (buf.try_push(i++)) {}          // fill completely

    const bool rejected = !buf.try_push(99);
    CHECK(rejected, "try_push returns false when buffer is full");
}

// ── Test 3: Empty-buffer rejection ────────────────────────────────────────────
static void test_empty_buffer()
{
    std::printf("Test 3: Empty-buffer rejection\n");
    SpscRingBuffer<int, 16> buf;

    int v = -999;
    const bool empty_pop = !buf.try_pop(v);
    CHECK(empty_pop,  "try_pop returns false on empty buffer");
    CHECK(v == -999,  "try_pop does not modify item when buffer is empty");
}

// ── Test 4: Wrap-around ───────────────────────────────────────────────────────
static void test_wraparound()
{
    std::printf("Test 4: Index wrap-around\n");
    // Three fill/drain cycles force the index past the power-of-2 boundary.
    SpscRingBuffer<uint32_t, 16> buf;  // holds 15 items

    bool ok = true;
    for (int cycle = 0; cycle < 3 && ok; ++cycle) {
        for (uint32_t i = 0; i < 15u; ++i)
            buf.try_push(i * 10u);

        for (uint32_t i = 0; i < 15u; ++i) {
            uint32_t v = 0;
            buf.try_pop(v);
            if (v != i * 10u) { ok = false; break; }
        }
    }
    CHECK(ok, "3 fill/drain cycles maintain FIFO across index wrap");
}

// ── Test 5: Concurrent 1M-operation integrity check ──────────────────────────
static void test_concurrent_1m()
{
    std::printf("Test 5: Concurrent 1M ops (producer + consumer threads)\n");

    static SpscRingBuffer<uint64_t, 1024> buf;
    static constexpr uint64_t N = 1'000'000ULL;

    std::atomic<uint64_t> sum_pushed{0};
    std::atomic<uint64_t> sum_popped{0};

    std::thread prod([&] {
        for (uint64_t i = 0; i < N; ++i) {
            while (!buf.try_push(i)) {}        // spin acceptable in test context
            sum_pushed.fetch_add(i, std::memory_order_relaxed);
        }
    });

    std::thread cons([&] {
        uint64_t popped = 0;
        while (popped < N) {
            uint64_t v;
            if (buf.try_pop(v)) {
                sum_popped.fetch_add(v, std::memory_order_relaxed);
                ++popped;
            }
        }
    });

    prod.join();
    cons.join();

    // Expected sum: 0 + 1 + ... + (N-1) = N*(N-1)/2
    const uint64_t expected = N * (N - 1) / 2;
    CHECK(sum_pushed == expected, "Producer pushed correct checksum");
    CHECK(sum_popped == expected, "Consumer received correct checksum (no loss/duplicate)");
}

int main()
{
    std::printf("╔══════════════════════════════════════════════╗\n");
    std::printf("║   SpscRingBuffer Correctness Tests           ║\n");
    std::printf("╚══════════════════════════════════════════════╝\n\n");

    test_fifo_order();
    std::printf("\n");
    test_full_buffer();
    std::printf("\n");
    test_empty_buffer();
    std::printf("\n");
    test_wraparound();
    std::printf("\n");
    test_concurrent_1m();

    std::printf("\n──────────────────────────────────────────────\n");
    std::printf("  Passed: %d / %d\n", g_passed, g_passed + g_failed);
    std::printf("──────────────────────────────────────────────\n");
    return (g_failed == 0) ? 0 : 1;
}
