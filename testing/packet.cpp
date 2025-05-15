#include <iostream>
#include <cmath>
#include <vector>
#include <string>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include "uWaveServer/packet.hpp"

TEST_CASE("uWaveServer::Packet")
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

