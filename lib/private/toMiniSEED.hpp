#ifndef PRIVATE_TO_MINISEED_HPP
#define PRIVATE_TO_MINISEED_HPP
#include <string>
#include <vector>
#include <cmath>
#include <libmseed.h>
#include <spdlog/spdlog.h>
#ifndef NDEBUG
#include <cassert>
#endif
#include "uWaveServer/packet.hpp"

#define INT_ENCODING 0
#define FLOAT_ENCODING 1
#define INT64_ENCODING 2
#define DOUBLE_ENCODING 3

namespace
{

void msRecordHandler(char *record, int recordLength, void *outputBuffer)
{
    auto buffer = reinterpret_cast<std::string *> (outputBuffer);
    buffer->append(record, recordLength);
    //std::cout << buffer->size() << std::endl;
}

[[nodiscard]] 
int encodingInteger(const UWaveServer::Packet::DataType dataType)
{
    if (dataType == UWaveServer::Packet::DataType::Integer32)
    {
        return INT_ENCODING;
    }
    else if (dataType == UWaveServer::Packet::DataType::Float)
    {
        return FLOAT_ENCODING;
    }
    else if (dataType == UWaveServer::Packet::DataType::Integer64)
    {
        return INT64_ENCODING;
    }
    else if (dataType == UWaveServer::Packet::DataType::Double)
    {
        return DOUBLE_ENCODING;
    }
    throw std::invalid_argument("Unhandled data type");    
}

[[nodiscard]]
int getMiniSEEDEncoding(const int encoding)
{
    if (encoding == INT_ENCODING)
    {
        return DE_INT32;
    }
    else if (encoding == FLOAT_ENCODING)
    {
        return DE_FLOAT32;
    }
    else if (encoding == DOUBLE_ENCODING || encoding == INT64_ENCODING)
    {
        return DE_FLOAT64;
    }
    throw std::runtime_error("Unhandled encoding " + std::to_string(encoding));
} 

[[nodiscard]]
std::string toMiniSEED(const std::vector<UWaveServer::Packet> &packets,
                       const int maxRecordLength = 512, //-1 results in default which is 4096
                       const bool useMiniSEED3 = true)
{
    std::string outputBuffer;
    if (packets.empty()){return outputBuffer;}
    int maxEncodingInteger{-1};
    for (const auto &packet : packets)
    {
        if (!packet.empty() &&
            packet.haveNetwork() &&
            packet.haveStation() &&
            packet.haveChannel())
        {
            auto encodingInteger = ::encodingInteger(packet.getDataType());
            maxEncodingInteger = std::max(encodingInteger, maxEncodingInteger);
        }
    }
    if (maxEncodingInteger ==-1)
    {
        throw std::invalid_argument("Appears to be no data to pack");
    }
    // Package things up 
    MS3TraceList *msTraceList{nullptr};
    MS3Tolerance *msTolerance{nullptr};
    MS3TraceSeg *msTraceSegment{nullptr};
    MS3Record *msRecord{nullptr};
    msRecord = msr3_init(msRecord);
    msTraceList = mstl3_init(msTraceList); 
    int64_t expectedNumberOfSamplesToPack{0};
    int64_t packedSamplesCount{0};
    if (msTraceList == nullptr)
    {
        throw std::runtime_error("Failed to initialize mseed trace list");
    }
    for (const auto &packet : packets)
    { 
        if (!packet.empty() &&
            packet.haveNetwork() &&
            packet.haveStation() &&
            packet.haveChannel())
        {
            if (msRecord)
            {
                // Pack the sid
                auto network = packet.getNetwork();
                auto station = packet.getStation();
                auto channel = packet.getChannel();
                std::string locationCode;
                if (packet.haveLocationCode())
                {
                    locationCode = packet.getLocationCode();
                }
                auto sidLength
                    = ms_nslc2sid(msRecord->sid, LM_SIDLEN, 0,
                                  const_cast<char *> (network.c_str()),
                                  const_cast<char *>(station.c_str()),
                                  const_cast<char *>(locationCode.c_str()),
                                  const_cast<char *>(channel.c_str()));
                if (sidLength < 1)
                {
                    spdlog::error("Failed to pack SID");
                    continue;
                }
                msRecord->datasamples = nullptr;
                // Pack the data
                std::vector<double> i64Data;
                msRecord->reclen = maxRecordLength;
                msRecord->pubversion = 1;
                // Microseconds to nanoseconds
                msRecord->starttime
                    = static_cast<int64_t> (packet.getStartTime().count()*1000);
                //char timestr[30];
                //ms_nstime2timestr(msRecord->starttime, timestr, ISOMONTHDAY, NANO_MICRO_NONE);
                //std::cout << msRecord->starttime << " " << timestr << std::endl;
                msRecord->samprate = packet.getSamplingRate();
                msRecord->numsamples = packet.size();
                auto encodingInteger = ::encodingInteger(packet.getDataType());
                if (encodingInteger == INT_ENCODING)
                {
                    msRecord->encoding = DE_INT32;
                    msRecord->sampletype = 'i';
                    msRecord->datasamples = const_cast<void *> (packet.data());
                }
                else if (encodingInteger == FLOAT_ENCODING)
                {
                    msRecord->encoding = DE_FLOAT32;
                    msRecord->sampletype = 'f';
                    msRecord->datasamples = const_cast<void *> (packet.data());
                }
                else if (encodingInteger == DOUBLE_ENCODING)
                {
                    msRecord->encoding = DE_FLOAT64;
                    msRecord->sampletype = 'd';
                    msRecord->datasamples = const_cast<void *> (packet.data());
                }
                else if (encodingInteger == INT64_ENCODING)
                {
                    i64Data.resize(packet.size());
                    auto dataPtr = static_cast<const int64_t *> (packet.data());
                    // Pack an int64_t into a double
                    std::copy(dataPtr, dataPtr + packet.size(), i64Data.data());
                    msRecord->datasamples = i64Data.data(); 
                }
                else
                {
                    throw std::runtime_error("Unhandled data type");
                } 
                msRecord->samplecnt = msRecord->numsamples;
                expectedNumberOfSamplesToPack
                    = expectedNumberOfSamplesToPack + msRecord->numsamples;
                // Add the record to the trace list
                constexpr uint32_t unusedFlags{0};
                msTraceSegment = mstl3_addmsr_recordptr(msTraceList,
                                                        msRecord, 
                                                        nullptr,
                                                        0, // split version
                                                        1, // auto heal
                                                        unusedFlags, // flags 
                                                        msTolerance); // NULL is default tolerance
                if (msTraceSegment == nullptr)
                {
                    spdlog::warn("Failed to add miniSEED record to trace list");
                    msRecord->datasamples = nullptr;
                    //msr3_free(&msRecord);
                    continue;
                }
                
                msRecord->datasamples = nullptr;
                //msr3_free(&msRecord);
            }
            else
            {
                spdlog::warn("MiniSEED record pointer is null");
            }
        }
        else
        {
            spdlog::warn("Empty packet");
        }
    }
    // Write the data to a string buffer
    uint32_t writeToBufferFlags{0};
    writeToBufferFlags |= MSF_FLUSHDATA;
    writeToBufferFlags |= MSF_MAINTAINMSTL; // Do not modify while packing
    if (!useMiniSEED3){writeToBufferFlags |= MSF_PACKVER2;}
    auto mseedEncoding = ::getMiniSEEDEncoding(maxEncodingInteger);
    char *extraHeaders{nullptr};
    constexpr int8_t verbose{0};
    auto nRecordsPacked = mstl3_pack(msTraceList,
                                     &msRecordHandler,
                                     &outputBuffer,
                                     maxRecordLength,
                                     mseedEncoding, 
                                     &packedSamplesCount,
                                     writeToBufferFlags,
                                     verbose,
                                     extraHeaders);
    if (nRecordsPacked > 0)
    {
        if (packedSamplesCount == expectedNumberOfSamplesToPack)
        {
            spdlog::debug("Packed "
                        + std::to_string(packedSamplesCount)
                        + " samples into " 
                        + std::to_string(nRecordsPacked)
                        + " records");
        }
        else
        {
            spdlog::warn("Only packed "
                       + std::to_string(packedSamplesCount)
                       + " of " 
                       + std::to_string(expectedNumberOfSamplesToPack)
                       + " samples into "
                       + std::to_string(nRecordsPacked)
                       + " records");
        }
    }
    else
    {
        spdlog::warn("No records packed");
    }

    /*
    auto res = mstl3_writemseed(msTraceList, "./temp.mseed", 1,
                                maxRecordLength, mseedEncoding, writeToBufferFlags, verbose); 
    std::cout << res << std::endl;
    */

    msr3_free(&msRecord);
    if (msTraceList){mstl3_free(&msTraceList, 1);}
    return outputBuffer; 
}


}
#endif
