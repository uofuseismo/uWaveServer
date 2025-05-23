#include <iostream>
#include <vector>
#include <chrono>
#include <cmath>
#include <algorithm>
#ifndef NDEBUG
#include <cassert>
#endif
#include <libmseed.h>
#include "uWaveServer/packet.hpp"
#include "private/packetToJSON.hpp"

using namespace UWaveServer;

namespace
{

/// @brief Converts an input string to an upper-case string with no blanks.
/// @param[in] s  The string to convert.
/// @result The input string without blanks and in all capital letters.
std::string convertString(const std::string &s) 
{
    auto temp = s;
    temp.erase(std::remove(temp.begin(), temp.end(), ' '), temp.end());
    std::transform(temp.begin(), temp.end(), temp.begin(), ::toupper);
    return temp;
}

static void msRecordHandler(char *record, int recordLength, void *outputBuffer)
{
    auto buffer = reinterpret_cast<std::string *> (outputBuffer);
    buffer->append(record, recordLength);
}

[[nodiscard]] int encodingInteger(const Packet::DataType dataType)
{
    if (dataType == Packet::DataType::Integer32)
    {
        return 0;
    }
    else if (dataType == Packet::DataType::Float)
    {
        return 1;
    }
    else if (dataType == Packet::DataType::Integer64)
    {
        return 2;
    }
    else if (dataType == Packet::DataType::Double)
    {
        return 3;
    }
    throw std::invalid_argument("Unhandled data type");    
}


std::string toMiniSEED(const std::vector<Packet> &packets,
                       const int maxRecordLength = 512, //-1 results in default which is 4096
                       const bool useMiniSEED3 = true)
{
    std::string outputBuffer;
    if (packets.empty()){return outputBuffer;}
    auto maxEncodingInteger = ::encodingInteger(packets.at(0).getDataType());
    bool variableData{false};
    for (int i = 1; i < static_cast<int> (packets.size()); ++i)
    {
        auto encodingInteger = ::encodingInteger(packets.at(i).getDataType());
        if (encodingInteger != maxEncodingInteger)
        {
            variableData = true;
        }
        encodingInteger = std::max(encodingInteger, maxEncodingInteger);
    }
 
    MS3TraceList *msTraceList{nullptr};
    msTraceList = mstl3_init(msTraceList); 
    for (const auto &packet : packets)
    { 
        MS3Record *msRecord{NULL};
        msRecord = msr3_init(msRecord);
        if (msRecord)
        {
            std::vector<double> i64Data;
            msRecord->reclen = maxRecordLength;
            msRecord->pubversion = 1;
            // Microseconds to nanoseconds
            msRecord->starttime
                = static_cast<int64_t> (packet.getStartTime().count()*1.e3);
            auto endTime = static_cast<int64_t> (packet.getEndTime().count()*1.e-3);
            msRecord->samprate = packet.getSamplingRate();
            msRecord->numsamples = packet.size();
            if (packet.getDataType() == Packet::DataType::Integer32)
            {
                msRecord->encoding = DE_INT32;
                msRecord->sampletype = 'i';
                msRecord->datasamples = const_cast<void *> (packet.data());
            }
            else if (packet.getDataType() == Packet::DataType::Float)
            {
                msRecord->encoding = DE_FLOAT32;
                msRecord->sampletype = 'f';
                msRecord->datasamples = const_cast<void *> (packet.data());
            }
            else if (packet.getDataType() == Packet::DataType::Double)
            {
                msRecord->encoding = DE_FLOAT64;
                msRecord->sampletype = 'd';
                msRecord->datasamples = const_cast<void *> (packet.data());
            }
            else if (packet.getDataType() == Packet::DataType::Integer64)
            {
                i64Data.resize(packet.size());
                auto dataPtr = static_cast<const int64_t *> (packet.data());
                std::copy(dataPtr, dataPtr + + packet.size(), i64Data.data());
                msRecord->datasamples = i64Data.data(); 
            }
            else
            {
                throw std::runtime_error("Unhandled data type");
            } 
            uint32_t flags{0};
            constexpr int8_t verbose{0};
            flags |= MSF_FLUSHDATA;
            if (!useMiniSEED3){flags |= MSF_PACKVER2;}
            // Don't modify trace list while packing
            // flags |= MSF_MAINTAINMSTL;
            // Pack all the data
            flags |= MSF_FLUSHDATA;
//          auto nRecordsPacked =  mstl3_pack(msTraceList, &msRecordHandler, &outputBuffer, maxRecordLength, encoding, nullptr, flags, verbose, nullptr); 
            constexpr uint32_t unusedFlags{0};
            if (mstl3_addmsr(msTraceList,
                             msRecord, 
                             0, 1, unusedFlags, 
                             nullptr)  // NULL is default toelrance
               )
            {
                msr3_free(&msRecord);
                mstl3_free(&msTraceList, 1); //("Failed to append to ms trace list");
            }
            auto nRecordsPacked = msr3_pack(msRecord, 
                                            &msRecordHandler,
                                            &outputBuffer,
                                            nullptr, 
                                            flags,
                                            verbose);
            msRecord->datasamples = nullptr;
            if (nRecordsPacked < 1)
            {
                msr3_free(&msRecord);
                throw std::runtime_error("Failed to pack data");
            }
        }
        else
        {
            throw std::runtime_error("Failed to initialize MS3Record");
        }
        msr3_free(&msRecord);
    }
    mstl3_free(&msTraceList, 1);
    return outputBuffer; 
}

}

class Packet::PacketImpl
{
public:
    [[nodiscard]] int size() const
    {
        if (mDataType == Packet::DataType::Unknown)
        {
            return 0;
        }
        else if (mDataType == Packet::DataType::Integer32)
        {
            return static_cast<int> (mInteger32Data.size());
        }
        else if (mDataType == Packet::DataType::Float)
        {
            return static_cast<int> (mFloatData.size());
        }
        else if (mDataType == Packet::DataType::Double)
        {
            return static_cast<int> (mDoubleData.size());
        }
        else if (mDataType == Packet::DataType::Integer64)
        {
            return static_cast<int> (mInteger64Data.size());
        }
#ifndef NDEBUG
        assert(false);
#else
        throw std::runtime_error("Unhandled data type in packet impl size");
#endif
    }
    [[nodiscard]] bool isEmpty() const
    {
        if (mDataType == Packet::DataType::Unknown)
        {
            return true;
        }
        else if (mDataType == Packet::DataType::Integer32)
        {
            return mInteger32Data.empty();
        }
        else if (mDataType == Packet::DataType::Float)
        {
            return mFloatData.empty();
        }
        else if (mDataType == Packet::DataType::Double)
        {
            return mDoubleData.empty();
        }
        else if (mDataType == Packet::DataType::Integer64)
        {
            return mInteger64Data.empty();
        }
#ifndef NDEBUG
        assert(false);
#else
        throw std::runtime_error("Unhandled data type in packet impl isEmpty");
#endif
    }
    void clearData()
    {
        mInteger32Data.clear();
        mInteger64Data.clear();
        mFloatData.clear();
        mDoubleData.clear();
        mDataType = Packet::DataType::Unknown;
    }
    void setData(const std::vector<int> &&data)
    {
        if (data.empty()){return;}
        clearData();
        mInteger32Data = std::move(data); 
        mDataType = Packet::DataType::Integer32;
        updateEndTime();
    }
    void setData(const std::vector<float> &&data)
    {   
        if (data.empty()){return;}
        clearData();
        mFloatData = std::move(data); 
        mDataType = Packet::DataType::Float;
        updateEndTime();
    }
    void setData(const std::vector<double> &&data)
    {   
        if (data.empty()){return;}
        clearData();
        mDoubleData = std::move(data); 
        mDataType = Packet::DataType::Double;
        updateEndTime();
    }
    void setData(const std::vector<int64_t> &&data)
    {   
        if (data.empty()){return;}
        clearData();
        mInteger64Data = std::move(data); 
        mDataType = Packet::DataType::Integer64;
        updateEndTime();
    }   
    void updateEndTime()
    {
        mEndTimeMicroSeconds = mStartTimeMicroSeconds;
        auto nSamples = size();
        if (nSamples > 0 && mSamplingRate > 0)
        {
            auto traceDuration
                = std::round( ((nSamples - 1)/mSamplingRate)*1000000 );
            auto iTraceDuration = static_cast<int64_t> (traceDuration);
            std::chrono::microseconds traceDurationMuS{iTraceDuration};
            mEndTimeMicroSeconds = mStartTimeMicroSeconds + traceDurationMuS;
        }
    }
    std::string mNetwork;
    std::string mStation;
    std::string mChannel;
    std::string mLocationCode;
    std::vector<int> mInteger32Data;
    std::vector<int64_t> mInteger64Data;
    std::vector<float> mFloatData;
    std::vector<double> mDoubleData;
    std::chrono::microseconds mStartTimeMicroSeconds{0};
    std::chrono::microseconds mEndTimeMicroSeconds{0};
    double mSamplingRate{0};
    Packet::DataType mDataType{Packet::DataType::Unknown};
    bool mHaveLocationCode{false};
};

/// Constructor
Packet::Packet() :
    pImpl(std::make_unique<PacketImpl> ())
{
}

/// Copy constructor
Packet::Packet(const Packet &packet)
{
    *this = packet;
}

/// Move constructor
Packet::Packet(Packet &&packet) noexcept
{
    *this = std::move(packet);
}

/// Copy assignment
Packet& Packet::operator=(const Packet &packet)
{
    if (&packet == this){return *this;}
    pImpl = std::make_unique<PacketImpl> (*packet.pImpl);
    return *this;
}

/// Reset class
void Packet::clear() noexcept
{
    pImpl = std::make_unique<PacketImpl> ();
}

/// Move assignment
Packet& Packet::operator=(Packet &&packet) noexcept
{
    if (&packet == this){return *this;}
    pImpl = std::move(packet.pImpl);
    return *this;
}

/// Network
void Packet::setNetwork(const std::string &stringIn)
{
    auto s = ::convertString(stringIn); 
    if (s.empty()){throw std::invalid_argument("Network is empty");}
    pImpl->mNetwork = s;
}

std::string Packet::getNetwork() const
{
    if (!haveNetwork())
    {
        throw std::runtime_error("Network code not set");
    }
    return pImpl->mNetwork;
}

bool Packet::haveNetwork() const noexcept
{
    return !pImpl->mNetwork.empty();
}

/// Station
void Packet::setStation(const std::string &stringIn)
{
    auto s = ::convertString(stringIn);
    if (s.empty()){throw std::invalid_argument("Station is empty");}
    pImpl->mStation = s;
}

std::string Packet::getStation() const
{
    if (!haveStation())
    {
        throw std::runtime_error("Station name not set");
    }
    return pImpl->mStation;
}

bool Packet::haveStation() const noexcept
{
    return !pImpl->mStation.empty();
}

/// Channel
void Packet::setChannel(const std::string &stringIn)
{
    auto s = ::convertString(stringIn); 
    if (s.empty()){throw std::invalid_argument("Channel is empty");}
    pImpl->mChannel = s;
}

std::string Packet::getChannel() const
{
    if (!haveChannel())
    {
        throw std::runtime_error("Channel code not set");
    }
    return pImpl->mChannel;
}

bool Packet::haveChannel() const noexcept
{
    return !pImpl->mChannel.empty();
}

/// Location code
void Packet::setLocationCode(const std::string &stringIn)
{
    pImpl->mLocationCode = ::convertString(stringIn); 
    pImpl->mHaveLocationCode = true;
}

std::string Packet::getLocationCode() const
{
    if (!haveLocationCode())
    {
        throw std::runtime_error("Location code not set");
    }
    return pImpl->mLocationCode;
}

bool Packet::haveLocationCode() const noexcept
{
    return pImpl->mHaveLocationCode;
}

/// Sampling rate
void Packet::setSamplingRate(const double samplingRate) 
{
    if (samplingRate <= 0)
    {
        throw std::invalid_argument("samplingRate = "
                                  + std::to_string(samplingRate)
                                  + " must be positive");
    }
    pImpl->mSamplingRate = samplingRate;
    pImpl->updateEndTime();
}

double Packet::getSamplingRate() const
{   
    if (!haveSamplingRate()){throw std::runtime_error("Sampling rate not set");}
    return pImpl->mSamplingRate;
}       

bool Packet::haveSamplingRate() const noexcept
{
    return (pImpl->mSamplingRate > 0);
}

void Packet::setStartTime(const double startTime) noexcept
{
    auto iStartTimeMuS = static_cast<int64_t> (std::round(startTime*1.e6));
    setStartTime(std::chrono::microseconds {iStartTimeMuS});
}

void Packet::setStartTime(
    const std::chrono::microseconds &startTime) noexcept
{
    pImpl->mStartTimeMicroSeconds = startTime;
    pImpl->updateEndTime();
}

std::chrono::microseconds Packet::getStartTime() const noexcept
{
    return pImpl->mStartTimeMicroSeconds;
}

std::chrono::microseconds Packet::getEndTime() const
{
    if (!haveSamplingRate())
    {
        throw std::runtime_error("Sampling rate note set");
    }
    if (empty())
    {
        throw std::runtime_error("No samples in signal");
    }
    return pImpl->mEndTimeMicroSeconds;
}

bool Packet::empty() const noexcept
{
    return pImpl->isEmpty();
}

/// Get data
template<typename U>
std::vector<U> Packet::getData() const
{
    std::vector<U> result;
    if (empty()){return result;}
    result.resize(size());
    auto dataType = getDataType();
    if (dataType == Packet::DataType::Integer32)
    {
        std::copy(pImpl->mInteger32Data.begin(),
                  pImpl->mInteger32Data.end(),
                  result.begin());
    }
    else if (dataType == Packet::DataType::Float)
    {
        std::copy(pImpl->mFloatData.begin(),
                  pImpl->mFloatData.end(),
                  result.begin());
    }
    else if (dataType == Packet::DataType::Double)
    {
        std::copy(pImpl->mDoubleData.begin(),
                  pImpl->mDoubleData.end(),
                  result.begin());
    }
    else if (dataType == Packet::DataType::Integer64)
    {
        std::copy(pImpl->mInteger64Data.begin(),
                  pImpl->mInteger64Data.end(),
                  result.begin());
    }
    else
    {
#ifndef NDEBUG
        assert(false);
#endif
        constexpr U zero{0};
        std::fill(result.begin(), result.end(), zero); 
    }
    return result;
}

const void* Packet::data() const noexcept 
{
    if (empty()){return nullptr;}
    auto dataType = getDataType();
    if (dataType == Packet::DataType::Integer32)
    {
        return pImpl->mInteger32Data.data();
    }
    else if (dataType == Packet::DataType::Float)
    {
        return pImpl->mFloatData.data();
    }
    else if (dataType == Packet::DataType::Double)
    {
        return pImpl->mDoubleData.data();
    }
    else if (dataType == Packet::DataType::Integer64)
    {
        return pImpl->mInteger64Data.data();
    }
    else if (dataType  == Packet::DataType::Unknown)
    {
        return nullptr;
    }
#ifndef NDEBUG
    else 
    {
        assert(false);
    }
#endif
    return nullptr;
}

/// Data type
Packet::DataType Packet::getDataType() const noexcept
{
    return pImpl->mDataType;
}

/// Size
int Packet::size() const noexcept
{
    return pImpl->size();
}

/// Set data
template<typename U>
void Packet::setData(const int nSamples, const U *data)
{
    if (nSamples < 1)
    {
        throw std::invalid_argument("No data samples");
    }
    if (data == nullptr){throw std::invalid_argument("data is null");}
    std::vector<U> dataVector{data, data + nSamples};
    pImpl->setData(std::move(dataVector));
}

template<typename U>
void Packet::setData(std::vector<U> &&data)
{
    if (data.empty()){throw std::invalid_argument("No data samples");}
    pImpl->setData(std::move(data));
}

template<typename U>
void Packet::setData(const std::vector<U> &data)
{
    if (data.empty()){throw std::invalid_argument("No data samples");}
    auto dataCopy = data;
    setData(std::move(dataCopy));
}

/// Destructor
Packet::~Packet() = default;

/// Trim
void Packet::trim(const double startTime, const double endTime)
{
    auto iStartTimeMuS = static_cast<int64_t> (std::round(startTime*1.e6));
    auto iEndTimeMuS = static_cast<int64_t> (std::round(endTime*1.e6));
    trim(std::chrono::microseconds {iStartTimeMuS},
         std::chrono::microseconds {iEndTimeMuS});
}

void Packet::trim(const std::chrono::microseconds &startTime,
                  const std::chrono::microseconds &endTime)
{
    if (startTime >= endTime)
    {
        throw std::invalid_argument("Start time must be less than end time");
    }
    if (!haveSamplingRate()){return;}
    if (empty()){return;} 
    // Typically we don't have to do anything
    if (pImpl->mStartTimeMicroSeconds >= startTime &&
        pImpl->mEndTimeMicroSeconds <= endTime){return;}
    // Clear the data: packet ends before desired start
    if (startTime > pImpl->mEndTimeMicroSeconds)
    {
        pImpl->clearData();
        return; 
    }
    // Packet starts before desired end
    if (endTime < pImpl->mStartTimeMicroSeconds)
    {
        pImpl->clearData();
        return;
    }
    // Okay, time to go to work
    auto nSamples = static_cast<int> (size());
    auto samplingPeriodMuS = std::round(1000000/getSamplingRate());
    int iStart{0};
    if (pImpl->mStartTimeMicroSeconds < startTime)
    {
        auto deltaTime = startTime - pImpl->mStartTimeMicroSeconds;
        iStart
            = static_cast<int>(std::floor(deltaTime.count()/samplingPeriodMuS));
        iStart = std::min(std::max(0, iStart), nSamples - 1);
    }
    auto iEnd{nSamples}; 
    if (pImpl->mEndTimeMicroSeconds > endTime)
    {
        auto deltaTime = endTime - pImpl->mStartTimeMicroSeconds;
        iEnd
            = static_cast<int> (std::ceil(deltaTime.count()/samplingPeriodMuS))
            + 1; // Exclusive
        iEnd = std::max(std::min(nSamples, iEnd), iStart);
    }
    if (iStart > 0 || iEnd < nSamples)
    {
        if (iStart < iEnd)
        {
            if (getDataType() == Packet::DataType::Integer32)
            {
                std::vector<int> work(pImpl->mInteger32Data.data() + iStart, 
                                      pImpl->mInteger32Data.data() + iEnd);
                setData(std::move(work));
            }
            else if (getDataType() == Packet::DataType::Integer64)
            {
                std::vector<int64_t> work(pImpl->mInteger64Data.data() + iStart,
                                          pImpl->mInteger64Data.data() + iEnd);
                setData(std::move(work));
            }
            else if (getDataType() == Packet::DataType::Double)
            {
                std::vector<double> work(pImpl->mDoubleData.data() + iStart,
                                         pImpl->mDoubleData.data() + iEnd);
                setData(std::move(work));
            }
            else if (getDataType() == Packet::DataType::Float)
            {
                std::vector<float> work(pImpl->mFloatData.data() + iStart,
                                        pImpl->mFloatData.data() + iEnd);
                setData(std::move(work));
            }
            else
            {
#ifndef NDEBUG
                assert(false);
#else
                throw std::runtime_error("Unhandled precision in trim");
#endif
            }
        }
        else
        {
            pImpl->clearData();
        }
        // Make the new start relative to the current start time plus
        // however many samples I copied
        if (iStart > 0)
        {
            auto iSamplingPeriodMuS = static_cast<int64_t> (samplingPeriodMuS);
            auto newStartTime = pImpl->mStartTimeMicroSeconds
                              + iStart
                               *std::chrono::microseconds {iSamplingPeriodMuS};
            setStartTime(newStartTime);
        }
    }
}

/// Swap times
void UWaveServer::swap(UWaveServer::Packet &lhs, UWaveServer::Packet &rhs)
{
    std::swap(lhs.pImpl, rhs.pImpl);
}


///--------------------------------------------------------------------------///
///                             Template Instantiation                       ///
///--------------------------------------------------------------------------///
template void Packet::setData(const int nSamples, const int *data);
template void Packet::setData(const int nSamples, const float *data);
template void Packet::setData(const int nSamples, const double *data);
template void Packet::setData(const int nSamples, const int64_t *data);
template void Packet::setData(std::vector<int> &&data);
template void Packet::setData(std::vector<float> &&data);
template void Packet::setData(std::vector<double> &&data);
template void Packet::setData(std::vector<int64_t> &&data);
template void Packet::setData(const std::vector<int> &data);
template void Packet::setData(const std::vector<float> &data);
template void Packet::setData(const std::vector<double> &data);
template void Packet::setData(const std::vector<int64_t> &data);
template std::vector<int> Packet::getData() const;
template std::vector<float> Packet::getData() const;
template std::vector<double> Packet::getData() const;
template std::vector<int64_t> Packet::getData() const;
