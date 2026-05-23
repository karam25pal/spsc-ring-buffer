/**
 * imu_demo.cpp — 1 kHz IMU sample ingestion via SpscRingBuffer.
 *
 * Demonstrates deterministic, low-jitter sensor data transfer —
 * a pattern directly applicable to avionics, navigation, and
 * signal-processing pipelines.
 *
 * Architecture:
 *   Producer thread  ──push──►  SpscRingBuffer<ImuSample, 256>  ──pop──►  Consumer thread
 *   (1 kHz, sleep_until)         (wait-free, lock-free)                   (yield-based)
 *
 * The buffer holds 255 slots at 1 kHz, giving ~255 ms of scheduling-jitter
 * headroom before any sample is dropped.
 *
 * Build: cmake .. && make imu_demo && ./imu_demo
 */

#include "spsc_ring_buffer.hpp"

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <thread>

using Clock     = std::chrono::steady_clock;
using Micros    = std::chrono::microseconds;
using namespace std::chrono_literals;

// ── IMU sample payload ────────────────────────────────────────────────────────
struct ImuSample {
    uint64_t timestamp_us;  ///< Capture time in microseconds
    float    ax, ay, az;    ///< Linear acceleration (m/s²)
    float    gx, gy, gz;    ///< Angular velocity    (rad/s)
};

// ── Shared state ──────────────────────────────────────────────────────────────
static SpscRingBuffer<ImuSample, 256> g_buf;
static std::atomic<bool>              g_running{true};
static std::atomic<uint64_t>          g_pushed {0};
static std::atomic<uint64_t>          g_dropped{0};

// ── Producer — simulates 77 GHz FMCW radar IMU at 1 kHz ──────────────────────
static void producer_thread()
{
    const auto period = 1000us;          // 1 ms  →  1 kHz
    auto next = Clock::now();
    uint64_t seq = 0;

    while (g_running.load(std::memory_order_relaxed)) {
        // Synthesise sinusoidal IMU data (gyro + accel)
        const float t = static_cast<float>(seq) * 1e-3f;  // seconds
        ImuSample s{};
        s.timestamp_us = seq * 1000u;
        s.ax = 0.10f * std::sin(2.0f * 3.14159f * 10.0f * t);
        s.ay = 0.10f * std::cos(2.0f * 3.14159f * 10.0f * t);
        s.az = 9.81f + 0.01f * std::sin(2.0f * 3.14159f * 0.5f * t);
        s.gx = 0.01f * std::sin(2.0f * 3.14159f *  5.0f * t);
        s.gy = 0.01f * std::cos(2.0f * 3.14159f *  5.0f * t);
        s.gz = 0.001f;

        if (g_buf.try_push(s))
            g_pushed.fetch_add(1, std::memory_order_relaxed);
        else
            g_dropped.fetch_add(1, std::memory_order_relaxed);  // buffer full

        ++seq;
        next += period;
        std::this_thread::sleep_until(next);   // deterministic 1 kHz cadence
    }
}

// ── Consumer — processes samples as fast as they arrive ──────────────────────
static void consumer_thread()
{
    uint64_t consumed = 0;
    double   sum_az   = 0.0;
    ImuSample s{};

    while (g_running.load(std::memory_order_relaxed) || !g_buf.empty()) {
        if (g_buf.try_pop(s)) {
            sum_az += static_cast<double>(s.az);
            ++consumed;
        } else {
            // Yield prevents burning a core while buffer is momentarily empty.
            // A real embedded consumer would block on a semaphore instead.
            std::this_thread::yield();
        }
    }

    const double mean_az = consumed ? sum_az / static_cast<double>(consumed) : 0.0;
    std::printf("[consumer] consumed=%-6llu  mean_az=%.4f m/s²\n",
                (unsigned long long)consumed, mean_az);
}

int main()
{
    std::printf("╔══════════════════════════════════════════╗\n");
    std::printf("║   SPSC IMU Demo  ·  1 kHz  ·  5 seconds ║\n");
    std::printf("╚══════════════════════════════════════════╝\n\n");

    std::thread prod(producer_thread);
    std::thread cons(consumer_thread);

    std::this_thread::sleep_for(5s);
    g_running.store(false, std::memory_order_relaxed);

    prod.join();
    cons.join();

    const uint64_t pushed  = g_pushed .load();
    const uint64_t dropped = g_dropped.load();
    const uint64_t total   = pushed + dropped;
    const double   drop_pct = total
        ? 100.0 * static_cast<double>(dropped) / static_cast<double>(total)
        : 0.0;

    std::printf("\n╔══════════════════════════════════════════╗\n");
    std::printf("║               Results                    ║\n");
    std::printf("╠══════════════════════════════════════════╣\n");
    std::printf("║  Total attempted  : %5llu samples        ║\n", (unsigned long long)total);
    std::printf("║  Pushed (success) : %5llu samples        ║\n", (unsigned long long)pushed);
    std::printf("║  Dropped (full)   : %5llu samples        ║\n", (unsigned long long)dropped);
    std::printf("║  Drop rate        : %6.3f %%             ║\n", drop_pct);
    std::printf("╚══════════════════════════════════════════╝\n");
    return 0;
}
