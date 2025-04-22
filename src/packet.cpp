#include <vector>
#include <chrono>
#include <cmath>
#include <algorithm>
#ifndef NDEBUG
#include <cassert>
#endif
#include "uWaveServer/packet.hpp"

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
#endif
    }
    [[nodiscard]] bool empty() const
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
#endif
    }
    void clearData()
    {
        mInteger32Data.clear();
        mInteger64Data.clear();
        mFloatData.clear();
        mDoubleData.clear();
    }
    void setData(const std::vector<int> &&data)
    {
        if (data.empty()){return;}
        clearData();
        mInteger32Data = std::move(data); 
        updateEndTime();
    }
    void setData(const std::vector<float> &&data)
    {   
        if (data.empty()){return;}
        clearData();
        mFloatData = std::move(data); 
        updateEndTime();
    }
    void setData(const std::vector<double> &&data)
    {   
        if (data.empty()){return;}
        clearData();
        mDoubleData = std::move(data); 
        updateEndTime();
    }
    void setData(const std::vector<int64_t> &&data)
    {   
        if (data.empty()){return;}
        clearData();
        mInteger64Data = std::move(data); 
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
    return pImpl->empty();
}

/// Get data
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

/// Destructor
Packet::~Packet() = default;

///--------------------------------------------------------------------------///
///                             Template Instantiation                       ///
///--------------------------------------------------------------------------///
template void Packet::setData(const int nSamples, const int *data);
template void Packet::setData(const int nSamples, const float *data);
template void Packet::setData(const int nSamples, const double *data);
template void Packet::setData(const int nSamples, const int64_t *data);
