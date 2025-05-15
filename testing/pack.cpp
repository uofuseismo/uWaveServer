#include <iostream>
#include <cmath>
#include <cstdint>
#include <vector>
#include "private/pack.hpp"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>


TEST_CASE("uWaveServer::pack::hex", "[int]")
{
    int iMin{std::numeric_limits<int>::lowest()};
    int iMax{std::numeric_limits<int>::max()};
    std::vector<int> data{305419896, iMin, -10, -5, 0, 5, 10, iMax};
    auto nSamples = static_cast<int> (data.size());
    auto encodedHex = ::hexRepresentation(data);
    auto decodedHex = ::unpackHexRepresentation<int>(encodedHex, nSamples);
    REQUIRE(nSamples == static_cast<int> (decodedHex.size())); 
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(data.at(i) == decodedHex.at(i));
        //std::cout << "int: " << data.at(i) << " " << decodedHex.at(i) << std::endl;
    }
}

TEST_CASE("uWaveServer::pack::hex", "[float]")
{
    float fMin{std::numeric_limits<float>::lowest()};
    float fMax{std::numeric_limits<float>::max()};
    std::vector<float> data{fMin, -10, -5.07, -0.74, 0, 4, 5, 10.2, fMax};
    auto nSamples = static_cast<float> (data.size());
    auto encodedHex = ::hexRepresentation(data);
    auto decodedHex = ::unpackHexRepresentation<float> (encodedHex, nSamples);
    REQUIRE(nSamples == static_cast<int> (decodedHex.size()));
    for (int i = 0; i < nSamples; ++i)
    {   
        REQUIRE(decodedHex.at(i) == Catch::Approx(data.at(i)));
        //std::cout << "float: " << data.at(i) << " " << decodedHex.at(i) << std::endl;
    }   
}

TEST_CASE("uWaveServer::pack::hex", "[int64_t]")
{
    int64_t iMin{std::numeric_limits<int64_t>::lowest()};
    int64_t iMax{std::numeric_limits<int64_t>::max()};
    std::vector<int64_t> data{305419896, iMin, -10, -5, 0, 5, 10, 99, iMax};
    auto nSamples = static_cast<int> (data.size());
    auto encodedHex = ::hexRepresentation(data);
    auto decodedHex = ::unpackHexRepresentation<int64_t> (encodedHex, nSamples);
    REQUIRE(nSamples == static_cast<int> (decodedHex.size()));
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(data.at(i) == decodedHex.at(i));
        //std::cout << "int64: " << data.at(i) << " " << decodedHex.at(i) << std::endl;
    }   
}

TEST_CASE("uWaveServer::pack::hex", "[double]")
{
    double dMin{std::numeric_limits<double>::lowest()};
    double dMax{std::numeric_limits<double>::max()};
    std::vector<double> data{dMin, -11.912, -5.07, -0.74, 0, 4, 5, 10.2, 1332.998933234, dMax};
    auto nSamples = static_cast<int> (data.size());
    auto encodedHex = ::hexRepresentation(data);
    //std::cout << encodedHex<< std::endl;
    auto decodedHex = ::unpackHexRepresentation<double> (encodedHex, nSamples);
    REQUIRE(nSamples == static_cast<int> (decodedHex.size()));
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(decodedHex.at(i) == Catch::Approx(data.at(i)));
        //std::cout << std::setprecision(15) << "double: " << data.at(i) << " " << decoded.at(i) << std::endl;
    }   
}

