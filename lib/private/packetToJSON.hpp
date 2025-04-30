#ifndef PACKET_TO_JSON_HPP
#define PACKET_TO_JSON_HPP
#include <vector>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include "uWaveServer/packet.hpp"
namespace
{

/// @brief Converts a packet to JSON.
nlohmann::json toJSON(const UWaveServer::Packet &packet)
{
    nlohmann::json result;
    result["network"] = packet.getNetwork();
    result["station"] = packet.getStation();
    result["channel"] = packet.getChannel();
    result["locationCode"] = packet.getLocationCode();
    result["samplingRate"] = packet.getSamplingRate();
    result["startTimeMuSeconds"] = packet.getStartTime().count();
    auto nSamples = packet.size(); 
    
    if (!packet.empty())
    {
        auto nSamples = packet.size();
        auto dataType = packet.getDataType();
        if (dataType == UWaveServer::Packet::DataType::Integer32)
        {
            auto dataPtr = reinterpret_cast<const int *> (packet.data());
            std::vector<int> data(dataPtr, dataPtr + nSamples);
            result["dataType"] = "integer32";
            result["data"] = std::move(data); 
        } 
        else if (dataType == UWaveServer::Packet::DataType::Integer64)
        {
            auto dataPtr = reinterpret_cast<const int64_t *> (packet.data());
            std::vector<int64_t> data(dataPtr, dataPtr + nSamples);
            result["dataType"] = "integer64";
            result["data"] = std::move(data); 
        }
        else if (dataType == UWaveServer::Packet::DataType::Double)
        {
            auto dataPtr = reinterpret_cast<const double *> (packet.data());
            std::vector<double> data(dataPtr, dataPtr + nSamples);
            result["dataType"] = "float64";
            result["data"] = std::move(data); 
        }
        else if (dataType == UWaveServer::Packet::DataType::Float)
        {
            auto dataPtr = reinterpret_cast<const float *> (packet.data());
            std::vector<float> data(dataPtr, dataPtr + nSamples);
            result["dataType"] = "float32";
            result["data"] = std::move(data); 
        }
#ifndef NDEBUG
        else
        {
            assert(false);
        }
#else
        else
        {
            result["dataType"] = nullptr;
            result["data"] = nullptr;
        }
#endif
    }
    else
    {
        result["dataType"] = nullptr;
        result["data"] = nullptr;

    } 
    return result;
}

/// @brief Converts a collection of packets to JSON.
nlohmann::json toJSON(const std::vector<UWaveServer::Packet> &packets)
{
    nlohmann::json result;
    for (const auto &packet : packets)
    {
        try
        {
            result.push_back(std::move(::toJSON(packet)));
        }
        catch (const std::exception &e)
        {
            spdlog::warn("Failed to append to JSON structure because "
                       + std::string {e.what()});
        }
    } 
    return result;
}

}
#endif
