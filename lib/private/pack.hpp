#ifndef PRIVATE_PACK_HPP
#define PRIVATE_PACK_HPP
#include <bit>
#include <string>
#include <cstddef>
#include <algorithm>
#ifndef NDEBUG
#include <cassert>
#endif
#include "private/compression.hpp"
namespace
{

template<typename T>
std::string pack(const size_t n, const T *data, const bool swapBytes)
{
    std::string result;
    constexpr auto dataTypeSize{sizeof(T)};
    if (std::is_same<T, char>::value)
    {
        result.resize(n);
        std::copy(data, data + n, result.data());
        return result;
    }
    if (n == 0){return result;}
#ifndef NDEBUG
    assert(data != nullptr);
#endif
    // Pack it up
    result.resize(dataTypeSize*n);
    union CharacterValueUnion
    {
        unsigned char cArray[dataTypeSize];
        T value;
    };
    CharacterValueUnion cvUnion;
    if (!swapBytes)
    {
        for (int i = 0; i < static_cast<int> (n); ++i)
        {
            cvUnion.value = data[i]; 
            std::copy(cvUnion.cArray, cvUnion.cArray + dataTypeSize,
                      result.data() + dataTypeSize*i);
        }
    }
    else
    {
        for (int i = 0; i < static_cast<int> (n); ++i)
        {
            cvUnion.value = data[i];     
            std::reverse_copy(cvUnion.cArray, cvUnion.cArray + dataTypeSize,
                              result.data() + dataTypeSize*i);
        }
    }
    return result;
}

template<typename T>
std::string packAndCompress(const size_t n,
                            const T *data,
                            const int compressionLevel,
                            const bool swapBytes)
{
    std::string result;
    if (n == 0){return result;}
    if (data == nullptr)
    {
        throw std::invalid_argument("Data is null");
    } 
    if (compressionLevel == Z_NO_COMPRESSION)
    {
        result = ::pack<T> (n, data, swapBytes);
    }
    else
    {
        auto workSpace = ::pack<T> (n, data, swapBytes);
        result = ::compressString(workSpace, compressionLevel);
    }
    return result;
}

template<typename T>
std::vector<T> unpack(const int nSamples,
                      const unsigned char *dataPtr,
                      const size_t dataPtrSize,
                      const bool swapBytes)
{
    std::vector<T> result;
    constexpr auto dataTypeSize{sizeof(T)};
    result.resize(nSamples);
    if (std::is_same<T, char>::value)
    {
        std::copy(dataPtr, dataPtr + nSamples, result.data());
        return result;
    }
    if (nSamples == 0){return result;}
#ifndef NDEBUG
    assert(dataPtrSize == dataTypeSize*nSamples);
#endif
    // Unpack it
    union CharacterValueUnion
    {
        unsigned char cArray[dataTypeSize];
        T value;
    };
    CharacterValueUnion cvUnion;
    if (!swapBytes)
    {
        for (int i = 0; i < nSamples; ++i)
        {
            auto i0 = dataTypeSize*i;
            auto i1 = i0 + dataTypeSize;
            std::copy(dataPtr + i0, dataPtr + i1, cvUnion.cArray);
            result[i] = cvUnion.value;
        }
    }
    else
    {
        for (int i = 0; i < nSamples; ++i)
        {
            auto i0 = dataTypeSize*i;
            auto i1 = i0 + dataTypeSize;
            std::reverse_copy(dataPtr + i0, dataPtr + i1, cvUnion.cArray);
            result[i] = cvUnion.value;
        }
    }
    return result; 
}

template<typename T>
std::vector<T> decompressAndUnpack(const int nSamples,
                                   std::basic_string<std::byte> &data,
                                   const bool packedAsLittleEndian,
                                   const bool amLittleEndian,
                                   const bool isCompressed)
{
    bool swapBytes
        = (packedAsLittleEndian == amLittleEndian) ? false : true;
    if (isCompressed)
    {
        auto decompressedData = ::decompressByteString(data);
        auto dataPtr 
            = reinterpret_cast<const unsigned char *> (decompressedData.data());
        return ::unpack<T> (nSamples,
                            dataPtr,
                            decompressedData.size(),
                            swapBytes);

    }
    else
    {
        auto dataPtr = reinterpret_cast<const unsigned char *> (data.data());
        return ::unpack<T> (nSamples, dataPtr, data.size(), swapBytes);
    }
}


/*
/// @brief Performs a byte swap on a value.
/// @todo In the future C++ may add primitives for this.  Currently,
///       reverse_byte is limited to an int.
template<typename T>
T reverseBytes(const T value)
{
    union
    {
        char c[sizeof(T)];
        T localValue{0};
    };
    localValue = value;
    std::reverse(c, c + sizeof(T));
    return localValue;
}

template<typename T>
std::vector<T> reverseBytes(const std::vector<T> &input)
{
    std::vector<T> result(input.size());
    for (int i = 0; i < static_cast<int> (input.size()); ++i)
    {
        result[i] = ::reverseBytes(input[i]);
    }
    return result;
}

template<typename T>
void reverseBytes(std::vector<T> &inputOutput)
{
    for (int i = 0; i < static_cast<int> (inputOutput.size()); ++i)
    {
        auto temp = inputOutput[i];
        inputOutput[i] = ::reverseBytes(temp);
    }
}

/// @brief Creates a hex representation of the input data vector
///        and forces the byte order as if this machine were little
///        endian.
template<typename T>
std::string hexRepresentation(const T *v, const int n,
                              const bool usePrefix,
                              const bool swapBytes,
                              const int
#ifdef WITH_ZLIB
                              compressionLevel
#endif
                              )
{
    std::stringstream stream;
    if (usePrefix){stream << "0x";}
    if constexpr (std::same_as<int, T> || std::same_as<int64_t, T>) 
    {
        if (!swapBytes)
        {
            for (int i = 0; i < n; ++i)
            {
                stream << std::hex
                       << std::setw(2*sizeof(T))
                       << std::setfill('0')
                       << v[i];
            }
        }
        else
        {
            for (int i = 0; i < n; ++i)
            {
                stream << std::hex
                       << std::setw(2*sizeof(T))
                       << std::setfill('0')
                       << ::reverseBytes(v[i]);
            }
        }
    }
    else if constexpr (std::same_as<float, T>)
    {
        if (!swapBytes)
        {
            for (int i = 0; i < n; ++i)
            {
                auto value = std::bit_cast<int32_t> (v[i]);
                stream << std::hex
                       << std::setw(2*sizeof(T))
                       << std::setfill('0')
                       << value;
            }
        }
        else
        {
            for (int i = 0; i < n; ++i)
            {   
                auto value = std::bit_cast<int32_t> (::reverseBytes(v[i]));
                stream << std::hex
                       << std::setw(2*sizeof(T))
                       << std::setfill('0')
                       << value;
            }   
        }
    }
    else if constexpr (std::same_as<double, T>)
    {
        if (!swapBytes)
        {
            for (int i = 0; i < n; ++i)
            {
                auto value = std::bit_cast<int64_t> (v[i]);
                stream << std::hex
                       << std::setw(2*sizeof(T))
                       << std::setfill('0')
                       << value;
            }
        }
        else
        {
            for (int i = 0; i < n; ++i)
            {
                auto value = std::bit_cast<int64_t> (::reverseBytes(v[i]));
                stream << std::hex
                       << std::setw(2*sizeof(T))
                       << std::setfill('0')
                       << value;
            }
        }
    }
    else
    {
        throw std::runtime_error("Unhandled precision in hex packer");
    }
#ifdef WITH_ZLIB
    if (compressionLevel != Z_NO_COMPRESSION)
    {
        return ::compressStringStream(stream, compressionLevel); 
    }
#endif
    return stream.str();
}

/// @brief Writes a vector to a hex representation and forces the 
///        the writing to happen as if this machine were little endian.
template<typename T>
std::string hexRepresentation(const std::vector<T> &v,
                              const bool usePrefix,
                              const bool swapBytes,
                              const int compressionLevel)
{
    return ::hexRepresentation(v.data(), static_cast<int> (v.size()),
                               usePrefix, swapBytes, compressionLevel); 
}

template<typename T>
[[nodiscard]] inline int getNonZeroStartIndex(const std::string &s,
                                              const int i)
{
    constexpr int stepSize = 2*sizeof(T);
    // We pre-padded with 0 - now it's time to remove that
    const char *cStr = s.c_str();
    int i1 = i*stepSize;
#ifndef NDEBUG
    assert(i1 + stepSize <= s.size());
#endif
    for (int iCopy = 0; iCopy < stepSize; ++iCopy)
    {
        if (cStr[i1 + iCopy] != '0')
        {
            return i1 + iCopy;
        }
    }
    return i1; 
}

template<typename T>
std::vector<T> unpackHexRepresentation(const std::string &s, 
                                       const int nSamples,
                                       const bool swapBytes)
{
    std::vector<T> result;
    if (s.empty() || nSamples < 1){return result;}
    result.resize(nSamples, 0);
    constexpr int stepSize = 2*sizeof(T); // N.B. assumed in getNonZeroStartIndex
    int stringPosition{0};
    if constexpr (std::same_as<T, int>)
    {
        for (int i = 0; i < nSamples; ++i)
        {
            auto i2 = (i + 1)*stepSize;
            auto i1 = ::getNonZeroStartIndex<int> (s, i);
            std::istringstream buffer(s.substr(i1, i2 - i1));
            uint32_t value;
            buffer >> std::hex >> value;
            result[i] = static_cast<int> (value);
        }
    }
    else if constexpr (std::same_as<T, int64_t>)
    {
        for (int i = 0; i < nSamples; ++i)
        {
            auto i2 = (i + 1)*stepSize;
            auto i1 = ::getNonZeroStartIndex<int64_t> (s, i);
            std::istringstream buffer(s.substr(i1, i2 - i1));
            uint64_t value;
            buffer >> std::hex >> value;
            result[i] = static_cast<int64_t> (value);
        }
    }
    else if constexpr (std::same_as<T, float>)
    {
        for (int i = 0; i < nSamples; ++i)
        {
            auto i2 = (i + 1)*stepSize;
            auto i1 = ::getNonZeroStartIndex<float> (s, i);
            std::istringstream buffer(s.substr(i1, i2 - i1));
            uint32_t value;
            buffer >> std::hex >> value;
            result[i] = std::bit_cast<float> (value);
        }
    }
    else if constexpr (std::same_as<T, double>)
    {
        for (int i = 0; i < nSamples; ++i)
        {
            auto i2 = (i + 1)*stepSize;
            auto i1 = ::getNonZeroStartIndex<double> (s, i);
            std::istringstream buffer(s.substr(i1, i2 - i1));
            uint64_t value;
            buffer >> std::hex >> value;
            result[i] = std::bit_cast<double> (value);
        }
    }
    else
    {
        throw std::runtime_error("Unhandled data type");
    }
    if (swapBytes){::reverseBytes(result);}
    return result;
}

template<typename T>
std::vector<T> decompressAndUnpackHexRepresentation(
    const std::string &hexString,
    const int nSamples,
    const bool swapBytes,
    const bool wasCompressed)
{
    if (!wasCompressed)
    {   
        return ::unpackHexRepresentation<T>(hexString, nSamples, swapBytes);
    }   
    else
    {   
        auto decompressedHexString = ::decompressString(hexString);
        return ::unpackHexRepresentation<T>(decompressedHexString,
                                            nSamples, swapBytes);
    }   
}
*/

}
#endif
