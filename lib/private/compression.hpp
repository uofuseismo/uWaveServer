#ifndef COMPRESSION_HPP
#define COMPRESSION_HPP
#ifndef WITH_ZLIB
#include <string>
#include <sstream>
#define Z_NO_COMPRESSION         0
#define Z_BEST_SPEED             1
#define Z_BEST_COMPRESSION       9
#define Z_DEFAULT_COMPRESSION  (-1)
namespace
{
std::string compressStringStream(
    const std::stringstream &inputStringStream,
    int compressionLevel = Z_BEST_COMPRESSION)
{
    return inputStringStream.str();
}

std::string decompressStringStream(const std::string &inputData)
{
    return inputData; 
}

}
#else
#include <string>
#include <sstream>
#include <cstring>
#include <array>
#include <zlib.h>
namespace
{
std::string compressString(std::string &inputString,
                           int compressionLevel)
{
    z_stream zs;
    std::memset(&zs, 0, sizeof(zs));

    if (deflateInit(&zs, compressionLevel) != Z_OK)
    {
        throw std::runtime_error("deflateInit failed");
    }

    zs.next_in = reinterpret_cast<Bytef *> (inputString.data());
    zs.avail_in = inputString.size();

    int returnCode{Z_OK};
    std::array<char, 16384> outBuffer;
    std::string outString;

    // Retrieve the compressed bytes blockwise
    do 
    {
        zs.next_out = reinterpret_cast<Bytef *> (outBuffer.data());
        zs.avail_out = outBuffer.size();

        returnCode = deflate(&zs, Z_FINISH);

        if (outString.size() < zs.total_out) 
        {
            // append the block to the output string
            outString.append(outBuffer.data(),
                             zs.total_out - outString.size());
        }
    } while (returnCode == Z_OK);

    deflateEnd(&zs);

    // Deal with an error
    if (returnCode != Z_STREAM_END) 
    {          
        throw std::runtime_error("Exception during zlib compression: ("
                               + std::to_string(returnCode) 
                               + ") " + std::string {zs.msg});
    }
    return outString;
}

std::string decompressByteString(std::byte *data, const size_t dataSize) 
{
    z_stream zs;
    std::memset(&zs, 0, sizeof(zs));

    if (inflateInit(&zs) != Z_OK)
    {
        throw std::runtime_error("inflateInit failed");
    }
    //auto copy = compressedString;
    zs.next_in = reinterpret_cast<Bytef *> (data); //copy.data());
    zs.avail_in = dataSize; //copy.size();

    int returnCode{Z_OK};
    std::array<char, 16384> outBuffer;
    std::string outString;

    // get the decompressed bytes blockwise using repeated calls to inflate
    do 
    {
        zs.next_out = reinterpret_cast<Bytef*> (outBuffer.data());
        zs.avail_out = outBuffer.size();

        returnCode = inflate(&zs, 0);

        if (outString.size() < zs.total_out) 
        {
            outString.append(outBuffer.data(),
                             zs.total_out - outString.size());
        }
    } while (returnCode == Z_OK);

    inflateEnd(&zs);

    // Deal with an error
    if (returnCode != Z_STREAM_END)
    {
        throw std::runtime_error("Exception during zlib decompression: ("
                               + std::to_string(returnCode)
                               + ") " + std::string {zs.msg});
    }
    return outString;
}

std::string decompressByteString(std::basic_string<std::byte> &data)
{
    return ::decompressByteString(data.data(), data.size());
}

}
#endif
#endif
