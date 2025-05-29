#include <iostream>
#include <chrono>
#include <fstream>
#include <numeric>
#include <cmath>
#include <vector>
#include <string>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include "uWaveServer/packet.hpp"
#include "uWaveServer/testFuturePacket.hpp"
#include "uWaveServer/testExpiredPacket.hpp"
#include "uWaveServer/testDuplicatePacket.hpp"

TEST_CASE("UWaveServer::TestFuturePacket")
{
    UWaveServer::Packet packet;
    packet.setNetwork("UU");
    packet.setStation("MOUT");
    packet.setChannel("HHZ");
    packet.setLocationCode("01");
    packet.setSamplingRate(1); // 1 sps helps with subsequent test
    packet.setData(std::vector<int> {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    constexpr std::chrono::microseconds maxFutureTime{1000};
    constexpr std::chrono::seconds logBadDataInterval{-1};
    UWaveServer::TestFuturePacket futurePacketTester{maxFutureTime, logBadDataInterval};
    SECTION("ValidData")
    {
        packet.setStartTime(0.0);
        REQUIRE(futurePacketTester.allow(packet));
    }
    auto now = std::chrono::high_resolution_clock::now();
    auto nowMuSeconds
        = std::chrono::time_point_cast<std::chrono::microseconds>
          (now).time_since_epoch();
    SECTION("FutureData")
    {
        packet.setStartTime(nowMuSeconds - std::chrono::microseconds {100});
        REQUIRE(!futurePacketTester.allow(packet));
    }
    SECTION("Copy")
    {
        auto testerCopy = futurePacketTester;
        packet.setStartTime(nowMuSeconds - std::chrono::microseconds {100});
        REQUIRE(!testerCopy.allow(packet));
    }
}

TEST_CASE("UWaveServer::TestExpiredPacket")
{
    UWaveServer::Packet packet;
    packet.setNetwork("UU");
    packet.setStation("EKU");
    packet.setChannel("HHZ");
    packet.setLocationCode("01");
    packet.setSamplingRate(100.0);
    packet.setData(std::vector<int64_t> {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    constexpr std::chrono::microseconds maxExpiredTime{1000};
    constexpr std::chrono::seconds logBadDataInterval{-1};
    UWaveServer::TestExpiredPacket expiredPacketTester{maxExpiredTime,
                                                       logBadDataInterval};
    SECTION("ValidData")
    {
        auto now = std::chrono::high_resolution_clock::now();
        auto nowMuSeconds
            = std::chrono::time_point_cast<std::chrono::microseconds>
              (now).time_since_epoch();
        packet.setStartTime(std::chrono::microseconds {nowMuSeconds});
        REQUIRE(expiredPacketTester.allow(packet)); 
    }
    SECTION("ExpiredData")
    {
        auto now = std::chrono::high_resolution_clock::now();
        auto nowMuSeconds
            = std::chrono::time_point_cast<std::chrono::microseconds>
              (now).time_since_epoch();
        // Sometimes it executes too fast so we need to subtract a little
        // tolerance 
        packet.setStartTime(std::chrono::microseconds {nowMuSeconds}
                          - maxExpiredTime
                          - std::chrono::microseconds{1});
        REQUIRE(!expiredPacketTester.allow(packet));
    }
    SECTION("Copy")
    {
        auto testerCopy = expiredPacketTester;
        auto now = std::chrono::high_resolution_clock::now();
        auto nowMuSeconds
            = std::chrono::time_point_cast<std::chrono::microseconds>
              (now).time_since_epoch();
        packet.setStartTime(std::chrono::microseconds {nowMuSeconds}
                          - maxExpiredTime
                          - std::chrono::microseconds{1});
        REQUIRE(!testerCopy.allow(packet));
    }
}
