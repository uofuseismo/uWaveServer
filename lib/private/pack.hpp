#ifndef PRIVATE_PACK_HPP
#define PRIVATE_PACK_HPP
#include <bit>
#include <sstream>
#include <iomanip>
#ifndef NDEBUG
#include <cassert>
#endif
namespace
{

void packi4(const int i, char c[])
{
    union
    {    
        char c4[4];
        int32_t i4{0};
    };
    i4 = i;
    if constexpr (std::endian::native == std::endian::little)
    {    
        std::copy(c4, c4 + 4, c);
    }    
    else 
    {    
        std::reverse_copy(c4, c4 + 4, c);
    }    
}

[[nodiscard]] int32_t unpacki4(const char c[], const bool swap = false)
{
    union
    {
        char c4[4];
        int32_t i4{0};
    };
    if (!swap)
    {
        std::copy(c, c + 4, c4);
    }
    else 
    {
        std::reverse_copy(c, c + 4, c4);
    } 
    return i4;
}

void packf4(const float f, char c[])
{
    union
    {   
        char c4[4];
        float f4{0};
    };   
    f4 = f;
    if constexpr (std::endian::native == std::endian::little)
    {    
        std::copy(c4, c4 + 4, c);
    }    
    else 
    {    
        std::reverse_copy(c4, c4 + 4, c);
    }    
}

[[nodiscard]] float unpackf4(const char c[], const bool swap = false)
{
    union
    {
        char c4[4];
        float f4{0};
    };
    if (!swap)
    {
        std::copy(c, c + 4, c4);
    }
    else
    {
        std::reverse_copy(c, c + 4, c4);
    }
    return f4;
}


void packf8(const double f, char c[])
{
    union
    {   
        char c8[8];
        double f8{0};
    };   
    f8 = f;
    if constexpr (std::endian::native == std::endian::little)
    {   
        std::copy(c8, c8 + 8, c);
    }    
    else 
    {    
        std::reverse_copy(c8, c8 + 8, c);
    }    
}

[[nodiscard]] double unpackf8(const char c[], const bool swap = false)
{
    union
    {
        char c8[8];
        double f8{0};
    };   
    if (!swap)
    {
        std::copy(c, c + 8, c8);
    }   
    else
    {    
        std::reverse_copy(c, c + 8, c8);
    }
    return f8;
}


void packi8(const int64_t i, char c[])
{
    union
    {   
        char c8[8];
        int64_t i8{0};
    };  
    i8 = i;
    if constexpr (std::endian::native == std::endian::little)
    {   
        std::copy(c8, c8 + 8, c);
    }
    else
    {   
        std::reverse_copy(c8, c8 + 8, c);
    }   
}

[[nodiscard]] int64_t unpacki8(const char c[], const bool swap)
{
    union
    {
        char c8[8];
        int64_t i8{0};
    };
    if (!swap)
    {
        std::copy(c, c + 8, c8);
    }
    else
    {
        std::reverse_copy(c, c + 8, c8);
    }
    return i8;
}


template<typename T>
std::string pack(const int n, const T *values)
{
    std::string result;
    if constexpr (std::same_as<T, int>)
    {
        result.resize(4*n, ' ');
        for (int i = 0; i < n; ++i)
        {
            packi4(values[i], result.data() + 4*i);
        }
        return result;
    }
    else if constexpr (std::same_as<T, float>)
    {
        result.resize(4*n, '\0');
        for (int i = 0; i < n; ++i)
        {
            packf4(values[i], result.data() + 4*i);
        }
        return result;
    }
    else if constexpr (std::same_as<T, double>)
    {
        result.resize(8*n, '\0');
        for (int i = 0; i < n; ++i)
        {   
            packf8(values[i], result.data() + 8*i);
        }
        return result;
    }
    else if constexpr (std::same_as<T, int64_t>)
    {
        result.resize(8*n, '\0');
        for (int i = 0; i < n; ++i)
        {
            packi8(values[i], result.data() + 8*i);
        }
        return result;
    }
#ifndef NDEBUG
    assert(false);
#endif
    return result;
}

template<typename T>
std::vector<T> unpack(const int nValues, const char *packedValues, const bool swap)
{
    std::vector<T> result(nValues, 0);
    if constexpr (std::same_as<T, int>)
    {
        if (!swap)
        {
            for (int i = 0; i < nValues; ++i)
            {
                result[i] = unpacki4(packedValues + 4*i, false);
            } 
        }
        else
        {
            for (int i = 0; i < nValues; ++i)
            {
                result[i] = unpacki4(packedValues + 4*i, true);
            }
        }
    }
    else if constexpr (std::same_as<T, float>)
    {
        if (!swap)
        {
            for (int i = 0; i < nValues; ++i)
            {
                result[i] = unpackf4(packedValues + 4*i, false);
            }
        }
        else
        {
            for (int i = 0; i < nValues; ++i)
            {
                result[i] = unpackf4(packedValues + 4*i, true);
            }
        }
    }
    else if constexpr (std::same_as<T, double>)
    {
        if (!swap)
        {
            for (int i = 0; i < nValues; ++i)
            {
                result[i] = unpackf8(packedValues + 8*i, false);
            }
        }
        else
        {
            for (int i = 0; i < nValues; ++i)
            {
                result[i] = unpackf8(packedValues + 8*i, true);
            }
        }
    }
    else if constexpr (std::same_as<T, int64_t>)
    {
        if (!swap)
        {
            for (int i = 0; i < nValues; ++i)
            {
                result[i] = unpacki8(packedValues + 8*i, false);
            }
        }
        else
        {
            for (int i = 0; i < nValues; ++i)
            {
                result[i] = unpacki8(packedValues + 8*i, true);
            }
        }
    }
    return result;
}

template<typename T>
std::string hexRepresentation(const std::vector<T> &v, 
                              bool usePrefix = false)
{
    std::stringstream stream;
    if (usePrefix){stream << "0x";}
    if constexpr (std::same_as<int, T> || std::same_as<int64_t, T>) 
    {
        for (const auto &num : v)
        {
            stream << std::hex
                   << std::setw(2*sizeof(T))
                   << std::setfill('0')
                   << num;
        }
    }
    else if constexpr (std::same_as<float, T>)
    {
        for (const auto &num : v)
        {
            auto value = std::bit_cast<int32_t> (num);
            stream << std::hex
                   << std::setw(2*sizeof(T))
                   << std::setfill('0')
                   << value;
        }
    }
    else if constexpr (std::same_as<double, T>)
    {
        for (const auto &num : v)
        {
            auto value = std::bit_cast<int64_t> (num);
            stream << std::hex
                   << std::setw(2*sizeof(T))
                   << std::setfill('0')
                   << value;
        }
    }
    else
    {
        throw std::runtime_error("Unhandled precision in hex packer");
    }
    return stream.str();
}

[[nodiscard]] int getNonZeroStartIndex(const std::string &s,
                                       const int i,
                                       const int stepSize)
{
    // We pre-pad with 0
    auto i1 = i*stepSize;
    for (int iCopy = 0; iCopy < stepSize; ++iCopy)
    {
#ifndef NDEBUG
        if (s.at(i1 + iCopy) != '0')
        {
            i1 = i1 + iCopy;
            break;
        }
#else
        if (s[i1 + iCopy] != '0')
        {
            i1 = i1 + iCopy;
            break;
        }
#endif
    }
    return i1; 
}

template<typename T>
std::vector<T> unpackHexRepresentation(const std::string &s, 
                                       const int nSamples)
{
    std::vector<T> result;
    if (s.empty() || nSamples < 1){return result;}
    result.reserve(nSamples);
    int stepSize = 2*sizeof(T);
    int stringPosition{0};
    if constexpr (std::same_as<T, int>)
    {
        for (int i = 0; i < nSamples; ++i)
        {
            auto i2 = (i + 1)*stepSize;
            auto i1 = ::getNonZeroStartIndex(s, i, stepSize);
            std::istringstream buffer(
                std::string {s.data() + i1, s.data() + i2});
            uint32_t value;
            buffer >> std::hex >> value;
            result.push_back(static_cast<int> (value));
        }
    }
    else if constexpr (std::same_as<T, int64_t>)
    {
        for (int i = 0; i < nSamples; ++i)
        {
            auto i2 = (i + 1)*stepSize;
            auto i1 = ::getNonZeroStartIndex(s, i, stepSize);
            std::istringstream buffer(
                std::string {s.data() + i1, s.data() + i2});
            uint64_t value;
            buffer >> std::hex >> value;
            result.push_back(static_cast<int64_t> (value));
        }
    }
    else if constexpr (std::same_as<T, float>)
    {
        for (int i = 0; i < nSamples; ++i)
        {
            auto i2 = (i + 1)*stepSize;
            auto i1 = ::getNonZeroStartIndex(s, i, stepSize);
            std::istringstream buffer(
                std::string {s.data() + i1, s.data() + i2});
            uint32_t value;
            buffer >> std::hex >> value;
            result.push_back(std::bit_cast<float> (value));
        }
    }
    else if constexpr (std::same_as<T, double>)
    {
        for (int i = 0; i < nSamples; ++i)
        {
            auto i2 = (i + 1)*stepSize;
            auto i1 = ::getNonZeroStartIndex(s, i, stepSize);
            std::istringstream buffer(
                std::string {s.data() + i1, s.data() + i2});
            uint64_t value;
            buffer >> std::hex >> value;
            result.push_back(std::bit_cast<double> (value));
        }
    }
    else
    {
        throw std::runtime_error("Unhandled data type");
    }
    return result;
}

}
#endif
