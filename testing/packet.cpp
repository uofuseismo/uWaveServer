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
#include "private/toMiniSEED.hpp"

TEST_CASE("uWaveServer::Packet", "[class]")
{
    std::string network{"UU"};
    std::string station{"VRUT"};
    std::string channel{"HHZ"};
    std::string locationCode{"01"};
    double startTime{101};
    std::chrono::microseconds startTimeMuS{101*1000000};
    double samplingRate{100};
    UWaveServer::Packet packet;
    REQUIRE_NOTHROW(packet.setNetwork(network));
    REQUIRE_NOTHROW(packet.setStation(station));
    REQUIRE_NOTHROW(packet.setChannel(channel));
    REQUIRE_NOTHROW(packet.setLocationCode(locationCode));
    packet.setStartTime(startTime);
    REQUIRE_NOTHROW(packet.setSamplingRate(samplingRate));
    
    REQUIRE(packet.getNetwork() == network);
    REQUIRE(packet.getStation() == station);
    REQUIRE(packet.getChannel() == channel);
    REQUIRE(packet.getLocationCode() == locationCode);
    REQUIRE(packet.getSamplingRate() == Catch::Approx(samplingRate));

    SECTION("int")
    {
        std::vector<int> data{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        packet.setData(data);
        REQUIRE(packet.size() == data.size());
        auto endTime = startTime + (data.size() - 1)/samplingRate;
        auto iEndTime = static_cast<int64_t> (std::round(endTime*1.e6));
        REQUIRE(packet.getEndTime().count() == iEndTime);
        auto dPtr = static_cast<const int *> (packet.data());
        for (int i = 0; i < static_cast<int> (data.size()); ++i)
        {
            REQUIRE(data.at(i) == dPtr[i]);
        }
    }

    SECTION("int64_t")
    {   
        std::vector<int64_t> data{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
        packet.setData(data);
        REQUIRE(packet.size() == data.size());
        auto endTime = startTime + (data.size() - 1)/samplingRate;
        auto iEndTime = static_cast<int64_t> (std::round(endTime*1.e6));
        REQUIRE(packet.getEndTime().count() == iEndTime);
        auto dPtr = static_cast<const int64_t *> (packet.data());
        for (int i = 0; i < static_cast<int> (data.size()); ++i)
        {
            REQUIRE(data.at(i) == dPtr[i]);
        }
    }

    SECTION("float")
    {   
        std::vector<float> data{-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6};
        packet.setData(data);
        REQUIRE(packet.size() == data.size());
        auto endTime = startTime + (data.size() - 1)/samplingRate;
        auto iEndTime = static_cast<int64_t> (std::round(endTime*1.e6));
        REQUIRE(packet.getEndTime().count() == iEndTime);
        auto dPtr = static_cast<const float *> (packet.data());
        for (int i = 0; i < static_cast<int> (data.size()); ++i)
        {
            REQUIRE(dPtr[i] == Catch::Approx(data.at(i)));
        }
    }   

    SECTION("double")
    {
        std::vector<double> data{-12, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6}; 
        packet.setData(data);
        REQUIRE(packet.size() == data.size());
        auto endTime = startTime + (data.size() - 1)/samplingRate;
        auto iEndTime = static_cast<int64_t> (std::round(endTime*1.e6));
        REQUIRE(packet.getEndTime().count() == iEndTime);
        auto dPtr = static_cast<const double *> (packet.data());
        for (int i = 0; i < static_cast<int> (data.size()); ++i)
        {
            REQUIRE(dPtr[i] == Catch::Approx(data.at(i)));
        }
    }   

    SECTION("trim ends")
    {
        int nSamples{100};
        std::vector<int> data(nSamples, 0);
        std::iota(data.begin(), data.end(), 0);
        packet.setData(data);

        // Edge case
        auto t1 = packet.getStartTime();
        auto t2 = packet.getEndTime();
        packet.trim(t1, t2);
        REQUIRE(packet.size() == nSamples);
        REQUIRE(packet.getStartTime() == t1);
        REQUIRE(packet.getEndTime() == t2);

        // Should be a layup (definitely outside)
        auto t1Outside = t1 - std::chrono::microseconds {1};
        auto t2Outside = t2 + std::chrono::microseconds {1};
        packet.trim(t1Outside, t2Outside);
        packet.setData(data);
        REQUIRE(packet.size() == nSamples);
        REQUIRE(packet.getStartTime() == t1);
        REQUIRE(packet.getEndTime() == t2);

        // Trim inside (this is harder)
        auto iDtMuSec
            = static_cast<int64_t>
              (std::round(1000000/packet.getSamplingRate()));
        std::chrono::microseconds samplingPeriodMuSec{ iDtMuSec };

        // Remove first 4 samples and last 9 samples (exactly)
        int nInStart = 5;
        auto t1Inside = t1 + (nInStart - 1)*samplingPeriodMuSec;
        auto nInEnd = 10;
        auto t2Inside = t2 - (nInEnd - 1)*samplingPeriodMuSec;
        packet.setData(data);
        packet.trim(t1Inside, t2Inside);
        REQUIRE(packet.getStartTime() == t1Inside);
        REQUIRE(packet.getEndTime() == t2Inside);
        auto nNewSamples = nSamples
                         - std::max(0, (nInStart - 1))
                         - std::max(0, (nInEnd - 1));
        REQUIRE(nNewSamples == packet.size());
        auto dataBack = packet.getData<int> ();
        for (int i = 0; i < nNewSamples; ++i)
        {
            CHECK(dataBack.at(i) == data.at(nInStart - 1 + i));
        }

        // Last test is fun with rounding - about as bad as it gets.
        // Here I'm starting a 1/4 of a sample after the exact time
        // so my new start time should be:
        //   t1 + (nInStart - 1)*samplingPeriodMuSec
        // because this gets rounded down 
        // and 1/4 of a sample before the exact end time so my
        // so my new end time should be:
        //   t2 - (nInEnd - 1)*samplingPeriodMuSec
        // because this gets rounded up
        auto quarterSample = iDtMuSec/4;
        auto t1Half = t1 + (nInStart - 1)*samplingPeriodMuSec
                    + std::chrono::microseconds {quarterSample};
        auto t2Half = t2 - (nInEnd - 1)*samplingPeriodMuSec;
                    - std::chrono::microseconds {quarterSample};
        packet.setStartTime(t1);
        packet.setData(data); 
        packet.trim(t1Half, t2Half);
        REQUIRE(packet.getStartTime() <= t1Half); // Real start time precedes desired
        REQUIRE(t2Half <= packet.getEndTime()); // Real end time exceeds desired end time
        // This works because I round the start time down and end time up
        nNewSamples = nSamples
                    - std::max(0, (nInStart - 1))
                    - std::max(0, (nInEnd - 1));
        REQUIRE(nNewSamples == packet.size());
        dataBack = packet.getData<int> ();
        for (int i = 0; i < nNewSamples; ++i)
        {
            CHECK(dataBack.at(i) == data.at(nInStart - 1 + i));
        }
    }
}

TEST_CASE("UWaveServer::Packet", "[miniSEED]")
{
    const std::string network{"UU"};
    const std::string station{"SVWY"};
    const std::string channel{"HHZ"};
    const std::string locationCode{"01"};
    const double samplingRate{100};
    const double startTime{1747326000};
    std::vector<UWaveServer::Packet> packets;
    int nPackets{5};
    int nTotalSamples{0};
    double thisStartTime{startTime};
    for (int iPacket = 0; iPacket < nPackets; ++iPacket)
    {
        thisStartTime = startTime + std::max(0, (nTotalSamples - 1))/samplingRate;
        UWaveServer::Packet packet;
        packet.setNetwork(network);
        packet.setStation(station);
        packet.setChannel(channel); 
        packet.setLocationCode(locationCode);
        packet.setSamplingRate(samplingRate);
        packet.setStartTime(thisStartTime);
        auto nSamples = 20 + iPacket%5;
        std::vector<int> data(nSamples, 0);
        auto sign = iPacket%2 == 0 ? +1 : -1;
        std::iota(data.begin(), data.end(), sign*nTotalSamples);
        packet.setData(data);
        packets.push_back(packet);

        nTotalSamples = nTotalSamples + nSamples;
    }
    // Let's try to serialize this thing
    constexpr bool useMiniSEED3 = true;
    try
    {
        auto result = ::toMiniSEED(packets, 512, useMiniSEED3);
        std::ofstream outFile{"test.mseed", std::ios::binary};
        outFile.write(result.c_str(), result.size());
        //outFile << result;
        outFile.close();

    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
    }
}

TEST_CASE("UWaveServer::Packet", "[json]")
{
    
}

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
