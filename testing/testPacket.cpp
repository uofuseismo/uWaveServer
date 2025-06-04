#include <iostream>
#include <random>
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

TEST_CASE("UWaveServer::TestDuplicatePacket")
{
    // Random packet sizes
    std::random_device randomDevice;
    std::mt19937 generator(188382);//randomDevice());
    std::uniform_int_distribution<> uniformDistribution(250, 350);

    // Define a base packet
    UWaveServer::Packet packet;
    packet.setNetwork("UU");
    packet.setStation("CTU");
    packet.setChannel("HHZ");
    packet.setLocationCode("01");
    const double samplingRate{100};
    packet.setSamplingRate(samplingRate); 

    // Define a start time
    auto now = std::chrono::high_resolution_clock::now();
    auto nowSeconds
        = std::chrono::time_point_cast<std::chrono::seconds>
          (now).time_since_epoch();
    double startTime = nowSeconds.count() - 600; // Don't mess with future
    packet.setStartTime(startTime);
     
    // Business as usual - all data comes in on time and subsequently
    SECTION("All good data")
    {
        const std::chrono::seconds logBadDataInterval{0};
        const int circularBufferSize{15};

        UWaveServer::TestDuplicatePacket
            tester{circularBufferSize, logBadDataInterval}; 
        int cumulativeSamples{0}; 
        int nExamples = 2*circularBufferSize;
        for (int iPacket = 0; iPacket < nExamples; iPacket++)
        {
            auto packetStartTime = startTime + cumulativeSamples/samplingRate;
            std::vector<int> data(uniformDistribution(generator), 0);
            cumulativeSamples
                = cumulativeSamples + static_cast<int> (data.size()); 
            packet.setStartTime(packetStartTime);
            packet.setData(data);
            REQUIRE(tester.allow(packet));
        }
    }

    SECTION("Every other is a duplicate")
    {
        const std::chrono::seconds logBadDataInterval{-1};
        const int circularBufferSize{15};

        UWaveServer::TestDuplicatePacket
            tester{circularBufferSize, logBadDataInterval}; 
        int cumulativeSamples{0}; 
        int nExamples = 2*circularBufferSize;
        for (int iPacket = 0; iPacket < nExamples; iPacket++)
        {
            auto packetStartTime = startTime + cumulativeSamples/samplingRate;
            std::vector<int> data(uniformDistribution(generator), 0); 
            cumulativeSamples
                = cumulativeSamples + static_cast<int> (data.size()); 
            packet.setStartTime(packetStartTime);
            packet.setData(data);
            CHECK(tester.allow(packet));
            CHECK(!tester.allow(packet));
        }
    }

    SECTION("Out of order with duplicates")
    {
        const std::chrono::seconds logBadDataInterval{-1};
        const int circularBufferSize{15};

        std::vector<UWaveServer::Packet> packets;
        int cumulativeSamples{0}; 
        for (int iPacket = 0; iPacket < circularBufferSize; iPacket++)
        {
            auto packetStartTime = startTime + cumulativeSamples/samplingRate;
            std::vector<int> data(uniformDistribution(generator), 0); 
            cumulativeSamples
                = cumulativeSamples + static_cast<int> (data.size()); 
            packet.setStartTime(packetStartTime);
            packet.setData(data);
            packets.push_back(packet);
        }
        std::shuffle(packets.begin(), packets.end(), generator);

        UWaveServer::TestDuplicatePacket
            tester{circularBufferSize, logBadDataInterval}; 
        for (const auto &outOfOrderPacket : packets)
        {
            //std::cout << std::setprecision(16) << "hey " << outOfOrderPacket.getStartTime().count()*1.e-6 << std::endl;
            REQUIRE(tester.allow(outOfOrderPacket));
            CHECK(!tester.allow(outOfOrderPacket));
        }
    }

    SECTION("Timing slips")
    {
        const std::chrono::seconds logBadDataInterval{-1};
        const int circularBufferSize{15};

        UWaveServer::TestDuplicatePacket
            tester{circularBufferSize, logBadDataInterval}; 
        int cumulativeSamples{0}; 
        // Load it
        int nExamples = circularBufferSize;
        std::vector<UWaveServer::Packet> packets;
        for (int iPacket = 0; iPacket < nExamples; iPacket++)
        {
            auto packetStartTime = startTime + cumulativeSamples/samplingRate;
            std::vector<int> data(uniformDistribution(generator), 0);
            cumulativeSamples
                = cumulativeSamples + static_cast<int> (data.size());
            packet.setStartTime(packetStartTime);
            packet.setData(data);
            CHECK(tester.allow(packet));
            packets.push_back(packet);
        }

        // Throw some timing slips in there
        auto firstPacket = packets.front();
        firstPacket.setStartTime(firstPacket.getStartTime().count()*1.e-6
                              - (firstPacket.size() - 1)/firstPacket.getSamplingRate()/2.);
        CHECK(!tester.allow(firstPacket));
        for (int iPacket = 0; iPacket < nExamples; iPacket++)
        {
            auto thisPacket = packets.at(iPacket);
            double packetStartTime = thisPacket.getStartTime().count()*1.e-6
                                   + (thisPacket.size() - 1)
                                     /thisPacket.getSamplingRate()/2;
            thisPacket.setStartTime(packetStartTime);
            CHECK(!tester.allow(thisPacket));
        }
    }
}
