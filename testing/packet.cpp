#include <iostream>
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

