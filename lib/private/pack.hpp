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

/// @brief Creates a hex representation of the input data vector
///        and forces the byte order as if this machine were little
///        endian.
template<typename T>
std::string hexRepresentation(const T *v, const int n,
                              bool usePrefix = false)
{
    std::stringstream stream;
    if (usePrefix){stream << "0x";}
    if constexpr (std::same_as<int, T> || std::same_as<int64_t, T>) 
    {
        if constexpr (std::endian::native == std::endian::little)
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
        if constexpr (std::endian::native == std::endian::little)
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
        if constexpr (std::endian::native == std::endian::little)
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
    return stream.str();
}

/// @brief Writes a vector to a hex representation and forces the 
///        the writing to happen as if this machine were little endian.
template<typename T>
std::string hexRepresentation(const std::vector<T> &v,
                              const bool usePrefix = false)
{
    return ::hexRepresentation(v.data(), static_cast<int> (v.size()), usePrefix); 
}

template<typename T>
[[nodiscard]] inline int getNonZeroStartIndex(const std::string &s,
                                              const int i)
{
    constexpr int stepSize = 2*sizeof(T);
/*
    const int offset = i*stepSize;
    const char *cStr = s.c_str() + offset;
    if constexpr (std::same_as<T, int> || std::same_as<T, float>)
    {
        if (cStr[0] != '0'){return offset;}
        if (cStr[1] != '0'){return offset + 1;}
        if (cStr[2] != '0'){return offset + 2;}
        if (cStr[3] != '0'){return offset + 3;}
        if (cStr[4] != '0'){return offset + 4;}
        if (cStr[5] != '0'){return offset + 5;}
        if (cStr[6] != '0'){return offset + 6;}
        if (cStr[7] != '0'){return offset + 7;}
        return offset;
    }
    else if constexpr (std::same_as<T, int64_t> || std::same_as<T, double>)
    {
        if (cStr[0]  != '0'){return offset;}
        if (cStr[1]  != '0'){return offset + 1;}
        if (cStr[2]  != '0'){return offset + 2;}
        if (cStr[3]  != '0'){return offset + 3;}
        if (cStr[4]  != '0'){return offset + 4;}
        if (cStr[5]  != '0'){return offset + 5;}
        if (cStr[6]  != '0'){return offset + 6;}
        if (cStr[7]  != '0'){return offset + 7;}

        if (cStr[8]  != '0'){return offset + 8;} 
        if (cStr[9]  != '0'){return offset + 9;} 
        if (cStr[10] != '0'){return offset + 10;}
        if (cStr[11] != '0'){return offset + 11;}
        if (cStr[12] != '0'){return offset + 12;}
        if (cStr[13] != '0'){return offset + 13;}
        if (cStr[14] != '0'){return offset + 14;}
        if (cStr[15] != '0'){return offset + 15;}
        return offset;
    }
    return offset;
*/
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
            //i1 = i1 + iCopy;
            //break;
        }
    }
    return i1; 
}

template<typename T>
std::vector<T> unpackHexRepresentation(const std::string &s, 
                                       const int nSamples)
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
    return result;
}

}
#endif
