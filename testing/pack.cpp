#include <iostream>
#include <cstddef>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>
#include <random>
#include "private/pack.hpp"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

namespace
{
template<typename T>
std::string packAndCompress(const std::vector<T> &data,
                            const int compressionLevel,
                            const bool swapBytes)
{
    return ::packAndCompress<T> (data.size(), data.data(),
                                 compressionLevel, swapBytes);
}

template<typename T>
std::vector<T>
    decompressAndUnpack(const int nSamples,
                        std::string &data,
                        const bool amLittleEndian,
                        const bool packedAsLittleEndian,
                        const bool isCompressed)
{
    std::basic_string<std::byte> byteStringData;//(data.size());
    auto dataPtr = reinterpret_cast<const std::byte *> (data.data());
    byteStringData.resize(data.size());
    std::copy(dataPtr, dataPtr + data.size(), byteStringData.data());
    //std::copy(data.begin(), data.end(), byteStringData.begin());
    return ::decompressAndUnpack<T> (nSamples,
                                     byteStringData,
                                     amLittleEndian,
                                     packedAsLittleEndian,
                                     isCompressed);
}

}

TEST_CASE("uWaveServer::pack::uncompressed", "[int]")
{
    const int compressionLevel{Z_NO_COMPRESSION};
    bool amLittleEndian{std::endian::native == std::endian::little ? true : false};
    bool swapBytes = false;
    bool packedAsLittleEndian = true;
    constexpr bool isCompressed{false};
    int iMin{std::numeric_limits<int>::lowest()};
    int iMax{std::numeric_limits<int>::max()};
    std::vector<int> data{305419896, iMin, -10, -5, 0, 5, 10, iMax};
    auto nSamples = static_cast<int> (data.size());

    auto packedData = ::packAndCompress(data, compressionLevel, swapBytes);
    REQUIRE(packedData.size() == data.size()*sizeof(int));
    auto unpackedData = ::decompressAndUnpack<int> (nSamples,
                                                    packedData, 
                                                    amLittleEndian,
                                                    packedAsLittleEndian,
                                                    isCompressed);
    REQUIRE(nSamples == static_cast<int> (unpackedData.size())); 
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(data.at(i) == unpackedData.at(i));
    }
}

TEST_CASE("uWaveServer::pack::uncompressed", "[int64_t]")
{
    const int compressionLevel{Z_NO_COMPRESSION};
    bool amLittleEndian{std::endian::native == std::endian::little ? true : false};
    bool swapBytes = false;
    bool packedAsLittleEndian = true;
    constexpr bool isCompressed{false};
    int64_t iMin{std::numeric_limits<int64_t>::lowest()};
    int64_t iMax{std::numeric_limits<int64_t>::max()};
    std::vector<int64_t> data{305419896, iMin, -10, -5, 0, 5, 10, iMax};
    auto nSamples = static_cast<int> (data.size());

    auto packedData = ::packAndCompress(data, compressionLevel, swapBytes);
    REQUIRE(packedData.size() == data.size()*sizeof(int64_t));
    auto unpackedData = ::decompressAndUnpack<int64_t> (nSamples,
                                                        packedData,
                                                        amLittleEndian,
                                                        packedAsLittleEndian,
                                                        isCompressed);
    REQUIRE(nSamples == static_cast<int> (unpackedData.size()));
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(data.at(i) == unpackedData.at(i));
    }
}

TEST_CASE("uWaveServer::pack::uncompressed", "[float]")
{
    const int compressionLevel{Z_NO_COMPRESSION};
    bool amLittleEndian{std::endian::native == std::endian::little ? true : false};
    bool swapBytes = false;
    bool packedAsLittleEndian = true;
    constexpr bool isCompressed{false};
    float fMin{std::numeric_limits<float>::lowest()};
    float fMax{std::numeric_limits<float>::max()};
    std::vector<float> data{fMin, -10, -5.07, -0.74, 0, 4, 5, 10.2, fMax};
    auto nSamples = static_cast<int> (data.size());

    auto packedData = ::packAndCompress(data, compressionLevel, swapBytes);
    REQUIRE(packedData.size() == data.size()*sizeof(float));
    auto unpackedData = ::decompressAndUnpack<float> (nSamples,
                                                      packedData,
                                                      amLittleEndian,
                                                      packedAsLittleEndian,
                                                      isCompressed);
    REQUIRE(nSamples == static_cast<int> (unpackedData.size()));
    for (int i = 0; i < nSamples; ++i)
    {   
        REQUIRE(unpackedData.at(i) == Catch::Approx(data.at(i)));
    }   
}

TEST_CASE("uWaveServer::pack::uncompressed", "[double]")
{
    const int compressionLevel{Z_NO_COMPRESSION};
    bool amLittleEndian{std::endian::native == std::endian::little ? true : false};
    bool swapBytes = false;
    bool packedAsLittleEndian = true;
    constexpr bool isCompressed{false};
    double dMin{std::numeric_limits<double>::lowest()};
    double dMax{std::numeric_limits<double>::max()};
    std::vector<double> data{dMin, -11.912, -5.07, -0.74, 0, 4, 5, 10.2, 1332.998933234, dMax};
    auto nSamples = static_cast<int> (data.size());

    auto packedData = ::packAndCompress(data, compressionLevel, swapBytes);
    REQUIRE(packedData.size() == data.size()*sizeof(double));
    auto unpackedData = ::decompressAndUnpack<double> (nSamples,
                                                       packedData,
                                                       amLittleEndian,
                                                       packedAsLittleEndian,
                                                       isCompressed);
    REQUIRE(nSamples == static_cast<int> (unpackedData.size()));
    for (int i = 0; i < nSamples; ++i)
    {   
        REQUIRE(unpackedData.at(i) == Catch::Approx(data.at(i)));
    }   
}

TEST_CASE("uWaveServer::pack::compressed", "[int]")
{
    const int compressionLevel{Z_BEST_COMPRESSION};
    bool amLittleEndian{std::endian::native == std::endian::little ? true : false};
    bool swapBytes = false;
    bool packedAsLittleEndian = true;
    constexpr bool isCompressed{true};
    int iMin{std::numeric_limits<int>::lowest()};
    int iMax{std::numeric_limits<int>::max()};
    std::vector<int> data{305419896, iMin, -10, -5, 0, 5, 10, iMax};
    std::mt19937 generator(26342);
    std::uniform_int_distribution<int> intDistribution{-10000, 10000};
    for (int i = 0; i < 400; ++i)
    {
        data.push_back(intDistribution(generator));
    }
    auto nSamples = static_cast<int> (data.size());

    auto packedData = ::packAndCompress(data, compressionLevel, swapBytes);
    auto unpackedData = ::decompressAndUnpack<int> (nSamples,
                                                    packedData,
                                                    amLittleEndian,
                                                    packedAsLittleEndian,
                                                    isCompressed);
    auto unpackedSize = static_cast<double> (data.size()*sizeof(int));
    auto packedPct = 100*packedData.size()/unpackedSize;
    REQUIRE(packedPct <= 100);
    std::cout << "int deflated is " << packedPct
              << " pct of original size" << std::endl;
    REQUIRE(nSamples == static_cast<int> (unpackedData.size()));
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(data.at(i) == unpackedData.at(i));
    }
}

TEST_CASE("uWaveServer::pack::compressed", "[int64_t]")
{
    const int compressionLevel{Z_BEST_COMPRESSION};
    bool amLittleEndian{std::endian::native == std::endian::little ? true : false};
    bool swapBytes = false;
    bool packedAsLittleEndian = true;
    constexpr bool isCompressed{true};
    int64_t iMin{std::numeric_limits<int64_t>::lowest()};
    int64_t iMax{std::numeric_limits<int64_t>::max()};
    std::vector<int64_t> data{305419896, iMin, -10, -5, 0, 5, 10, iMax};
    std::mt19937 generator(26342);
    std::uniform_int_distribution<int64_t> intDistribution{-10000, 10000};
    for (int i = 0; i < 400; ++i)
    {   
        data.push_back(intDistribution(generator));
    }   
    auto nSamples = static_cast<int> (data.size());

    auto packedData = ::packAndCompress(data, compressionLevel, swapBytes);
    auto unpackedData = ::decompressAndUnpack<int64_t> (nSamples,
                                                        packedData,
                                                        amLittleEndian,
                                                        packedAsLittleEndian,
                                                        isCompressed);
    auto unpackedSize = static_cast<double> (data.size()*sizeof(int64_t));
    auto packedPct = 100*packedData.size()/unpackedSize;
    REQUIRE(packedPct <= 100);
    std::cout << "int64_t deflated is " << packedPct
              << " pct of original size" << std::endl;
    REQUIRE(nSamples == static_cast<int> (unpackedData.size()));
    for (int i = 0; i < nSamples; ++i)
    {   
        REQUIRE(data.at(i) == unpackedData.at(i));
    }   
}

TEST_CASE("uWaveServer::pack::compressed", "[float]")
{
    const int compressionLevel{Z_BEST_COMPRESSION};
    bool amLittleEndian{std::endian::native == std::endian::little ? true : false};
    bool swapBytes = false;
    bool packedAsLittleEndian = true;
    constexpr bool isCompressed{true};
    float fMin{std::numeric_limits<float>::lowest()};
    float fMax{std::numeric_limits<float>::max()};
    std::vector<float> data{fMin, -11.912, -5.07, -0.74, 0, 4, 5, 10.2, 1332.9, fMax};
    std::mt19937 generator(26342);
    std::uniform_real_distribution<float> realDistribution{-10000, 10000};
    for (int i = 0; i < 400; ++i)
    {   
        data.push_back(realDistribution(generator));
    }   
    auto nSamples = static_cast<int> (data.size());

    auto packedData = ::packAndCompress(data, compressionLevel, swapBytes);
    auto unpackedData = ::decompressAndUnpack<float> (nSamples,
                                                      packedData,
                                                      amLittleEndian,
                                                      packedAsLittleEndian,
                                                      isCompressed);
    auto unpackedSize = static_cast<double> (data.size()*sizeof(float));
    auto packedPct = 100*packedData.size()/unpackedSize;
    REQUIRE(packedPct <= 100);
    std::cout << "float deflated is " << packedPct
              << " pct of original size" << std::endl;
    REQUIRE(nSamples == static_cast<int> (unpackedData.size()));
    for (int i = 0; i < nSamples; ++i)
    {   
        REQUIRE(unpackedData.at(i) == Catch::Approx(data.at(i)));
    }   
}

TEST_CASE("uWaveServer::pack::compressed", "[double]")
{
    const int compressionLevel{Z_BEST_COMPRESSION};
    bool amLittleEndian{std::endian::native == std::endian::little ? true : false};
    bool swapBytes = false;
    bool packedAsLittleEndian = true;
    constexpr bool isCompressed{true};
    double dMin{std::numeric_limits<double>::lowest()};
    double dMax{std::numeric_limits<double>::max()};
    std::vector<double> data{dMin, -11.912, -5.07, -0.74, 0, 4, 5, 10.2, 1332.998933234, dMax};
    std::mt19937 generator(26342);
    std::uniform_real_distribution<double> realDistribution{-10000, 10000};
    for (int i = 0; i < 400; ++i)
    {   
        data.push_back(realDistribution(generator));
    }   
    auto nSamples = static_cast<int> (data.size());

    auto packedData = ::packAndCompress(data, compressionLevel, swapBytes);
    auto unpackedData = ::decompressAndUnpack<double> (nSamples,
                                                       packedData,
                                                       amLittleEndian,
                                                       packedAsLittleEndian,
                                                       isCompressed);
    auto unpackedSize = static_cast<double> (data.size()*sizeof(double));
    auto packedPct = 100*packedData.size()/unpackedSize;
    REQUIRE(packedPct <= 100);
    std::cout << "double deflated is " << packedPct
              << " pct of original size" << std::endl;
    REQUIRE(nSamples == static_cast<int> (unpackedData.size()));
    for (int i = 0; i < nSamples; ++i)
    {   
        REQUIRE(unpackedData.at(i) == Catch::Approx(data.at(i)));
    }   
}


/*
TEST_CASE("uWaveServer::pack::hex", "[int]")
{
    const int compressionLevel{Z_NO_COMPRESSION};
    constexpr bool usePrefix{false};
    constexpr bool swapBytes{false};
    int iMin{std::numeric_limits<int>::lowest()};
    int iMax{std::numeric_limits<int>::max()};
    std::vector<int> data{305419896, iMin, -10, -5, 0, 5, 10, iMax};
    auto nSamples = static_cast<int> (data.size());
    auto encodedHex
        = ::hexRepresentation(data, usePrefix, swapBytes,
                              compressionLevel);
    auto decodedHex
        = ::unpackHexRepresentation<int>(encodedHex, nSamples, swapBytes);
    REQUIRE(nSamples == static_cast<int> (decodedHex.size())); 
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(data.at(i) == decodedHex.at(i));
        //std::cout << "int: " << data.at(i) << " " << decodedHex.at(i) << std::endl;
    }
}

TEST_CASE("uWaveServer::pack::hex", "[float]")
{
    constexpr int compressionLevel{Z_NO_COMPRESSION};
    constexpr bool usePrefix{false};
    constexpr bool swapBytes{false};
    float fMin{std::numeric_limits<float>::lowest()};
    float fMax{std::numeric_limits<float>::max()};
    std::vector<float> data{fMin, -10, -5.07, -0.74, 0, 4, 5, 10.2, fMax};
    auto nSamples = static_cast<float> (data.size());
    auto encodedHex
        = ::hexRepresentation(data, usePrefix, swapBytes,
                              compressionLevel);
    auto decodedHex
        = ::unpackHexRepresentation<float> (encodedHex, nSamples, swapBytes);
    REQUIRE(nSamples == static_cast<int> (decodedHex.size()));
    for (int i = 0; i < nSamples; ++i)
    {   
        REQUIRE(decodedHex.at(i) == Catch::Approx(data.at(i)));
        //std::cout << "float: " << data.at(i) << " " << decodedHex.at(i) << std::endl;
    }   
}

TEST_CASE("uWaveServer::pack::hex", "[int64_t]")
{
    constexpr int compressionLevel{Z_NO_COMPRESSION};
    constexpr bool usePrefix{false};
    constexpr bool swapBytes{false};
    int64_t iMin{std::numeric_limits<int64_t>::lowest()};
    int64_t iMax{std::numeric_limits<int64_t>::max()};
    std::vector<int64_t> data{305419896, iMin, -10, -5, 0, 5, 10, 99, iMax};
    auto nSamples = static_cast<int> (data.size());
    auto encodedHex
         = ::hexRepresentation(data, usePrefix, swapBytes, compressionLevel);
    auto decodedHex = ::unpackHexRepresentation<int64_t> (encodedHex, nSamples, swapBytes);
    REQUIRE(nSamples == static_cast<int> (decodedHex.size()));
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(data.at(i) == decodedHex.at(i));
        //std::cout << "int64: " << data.at(i) << " " << decodedHex.at(i) << std::endl;
    }   
}

TEST_CASE("uWaveServer::pack::hex", "[double]")
{
    constexpr int compressionLevel{Z_NO_COMPRESSION};
    constexpr bool usePrefix{false};
    constexpr bool swapBytes{false};
    double dMin{std::numeric_limits<double>::lowest()};
    double dMax{std::numeric_limits<double>::max()};
    std::vector<double> data{dMin, -11.912, -5.07, -0.74, 0, 4, 5, 10.2, 1332.998933234, dMax};
    auto nSamples = static_cast<int> (data.size());
    auto encodedHex
        = ::hexRepresentation(data, usePrefix, swapBytes,
                              compressionLevel);
    //std::cout << encodedHex<< std::endl;
    auto decodedHex
        = ::unpackHexRepresentation<double> (encodedHex, nSamples, swapBytes);
    REQUIRE(nSamples == static_cast<int> (decodedHex.size()));
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(decodedHex.at(i) == Catch::Approx(data.at(i)));
        //std::cout << std::setprecision(15) << "double: " << data.at(i) << " " << decoded.at(i) << std::endl;
    }   
}

TEST_CASE("uWaveServer::pack::hexCompressed", "[int]")
{
    const int compressionLevel{Z_BEST_COMPRESSION};
    constexpr bool usePrefix{false};
    constexpr bool swapBytes{false};
    int iMin{std::numeric_limits<int>::lowest()};
    int iMax{std::numeric_limits<int>::max()};
    std::vector<int> data{305419896, iMin, -10, -5, 0, 5, 10, iMax};
    std::mt19937 generator(26342);
    std::uniform_int_distribution<int> intDistribution{-10, 10};
    for (int i = 0; i < 400; ++i)
    {
        data.push_back(intDistribution(generator));
    }
    auto nSamples = static_cast<int> (data.size());
    auto encodedHex
        = ::hexRepresentation(data, usePrefix, swapBytes,
                              compressionLevel);
    auto encodedHexNoCompression
        = ::hexRepresentation(data, usePrefix, swapBytes,
                              Z_NO_COMPRESSION);
    std::cout << "int32 compression: " << static_cast<double> (encodedHex.size())/encodedHexNoCompression.size() << std::endl;

    constexpr bool wasCompressed{true};
    auto decodedHex
        = ::decompressAndUnpackHexRepresentation<int>(encodedHex, nSamples,
                                                      swapBytes, wasCompressed);
    REQUIRE(nSamples == static_cast<int> (decodedHex.size()));
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(data.at(i) == decodedHex.at(i));
        //std::cout << "int: " << data.at(i) << " " << decodedHex.at(i) << std::endl;
    }
}

TEST_CASE("uWaveServer::pack::hexCompressed", "[float]")
{
    constexpr int compressionLevel{Z_BEST_COMPRESSION};
    constexpr bool usePrefix{false};
    constexpr bool swapBytes{false};
    float fMin{std::numeric_limits<float>::lowest()};
    float fMax{std::numeric_limits<float>::max()};
    std::vector<float> data{fMin, -11.912, -5.07, -0.74, 0, 4, 5, 10.2, 1332.99, fMax};
    std::mt19937 generator(26342);
    std::uniform_real_distribution<float> realDistribution{-1500, 1500};
    for (int i = 0; i < 397; ++i)
    {
        data.push_back(realDistribution(generator));
    }
    auto nSamples = static_cast<int> (data.size());
    auto encodedHex
        = ::hexRepresentation(data, usePrefix, swapBytes,
                              compressionLevel);
    auto encodedHexNoCompression 
        = ::hexRepresentation(data, usePrefix, swapBytes,
                              Z_NO_COMPRESSION);
    std::cout << "float compression: " << static_cast<double> (encodedHex.size())/encodedHexNoCompression.size() << std::endl;

    constexpr bool wasCompressed{true};
    auto decodedHex
        = ::decompressAndUnpackHexRepresentation<float>
          (encodedHex, nSamples, swapBytes, wasCompressed);
    REQUIRE(nSamples == static_cast<int> (decodedHex.size()));
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(decodedHex.at(i) == Catch::Approx(data.at(i)));
    }
}   

TEST_CASE("uWaveServer::pack::hexCompressed", "[int64_t]")
{
    const int compressionLevel{Z_BEST_COMPRESSION};
    constexpr bool usePrefix{false};
    constexpr bool swapBytes{false};
    int64_t iMin{std::numeric_limits<int64_t>::lowest()};
    int64_t iMax{std::numeric_limits<int64_t>::max()};
    std::vector<int64_t> data{305419896, iMin, -10, -5, 0, 5, 10, iMax};
    std::mt19937 generator(26342);
    std::uniform_int_distribution<int64_t> intDistribution{-1500, 1500};
    for (int i = 0; i < 400; ++i)
    {
        data.push_back(intDistribution(generator));
    }
    auto nSamples = static_cast<int> (data.size());
    auto encodedHex
        = ::hexRepresentation(data, usePrefix, swapBytes,
                              compressionLevel);
    auto encodedHexNoCompression
        = ::hexRepresentation(data, usePrefix, swapBytes,
                              Z_NO_COMPRESSION);
    std::cout << "int64 compression: " << static_cast<double> (encodedHex.size())/encodedHexNoCompression.size() << std::endl;

    constexpr bool wasCompressed{true};
    auto decodedHex
        = ::decompressAndUnpackHexRepresentation<int64_t>
          (encodedHex, nSamples, swapBytes, wasCompressed);
    REQUIRE(nSamples == static_cast<int> (decodedHex.size()));
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(data.at(i) == decodedHex.at(i));
        //std::cout << "int: " << data.at(i) << " " << decodedHex.at(i) << std::endl;
    }
}

TEST_CASE("uWaveServer::pack::hexCompressed", "[double]")
{
    constexpr int compressionLevel{Z_BEST_COMPRESSION};
    constexpr bool usePrefix{false};
    constexpr bool swapBytes{false};
    double dMin{std::numeric_limits<double>::lowest()};
    double dMax{std::numeric_limits<double>::max()};
    std::vector<double> data{dMin, -11.912, -5.07, -0.74, 0, 4, 5, 10.2, 1332.998933234, dMax};
    std::mt19937 generator(26342);
    std::uniform_real_distribution<double> realDistribution{-1500, 1500};
    for (int i = 0; i < 403; ++i)
    {
        data.push_back(realDistribution(generator));
    }
    auto nSamples = static_cast<int> (data.size());
    auto encodedHex
        = ::hexRepresentation(data, usePrefix, swapBytes,
                              compressionLevel);
    auto encodedHexNoCompression
        = ::hexRepresentation(data, usePrefix, swapBytes,
                              Z_NO_COMPRESSION);
    std::cout << "Double compression: " << static_cast<double> (encodedHex.size())/encodedHexNoCompression.size() << std::endl;

    constexpr bool wasCompressed{true};
    auto decodedHex
        = ::decompressAndUnpackHexRepresentation<double>
          (encodedHex, nSamples, swapBytes, wasCompressed);
    REQUIRE(nSamples == static_cast<int> (decodedHex.size()));
    for (int i = 0; i < nSamples; ++i)
    {
        REQUIRE(decodedHex.at(i) == Catch::Approx(data.at(i)));
    }
}
*/
