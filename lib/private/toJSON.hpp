#ifndef PRIVATE_TO_JSON_HPP
#define PRIVATE_TO_JSON_HPP
#include <vector>
#include <cmath>
#include <chrono>
#include <set>
#include <crow/json.h>
#include <spdlog/spdlog.h>
#include "uWaveServer/packet.hpp"
#include "lib/private/toName.hpp"
namespace
{

crow::json::wvalue packetToCrowJSON(const UWaveServer::Packet &packet)
{
    crow::json::wvalue result;
    result["samplingRate"] = packet.getSamplingRate();
    result["startTimeMuSec"] = packet.getStartTime().count();
    auto dataType = packet.getDataType(); 
    if (!packet.empty())
    {
        if (dataType == UWaveServer::Packet::DataType::Integer32)
        {
            result["dataType"] = "int32_t";
            result["samples"] = packet.getData<int> (); 
        }
        else if (dataType == UWaveServer::Packet::DataType::Integer64)
        {
            result["dataType"] = "int64_t";
            result["samples"] = packet.getData<int64_t> (); 
        }
        else if (dataType == UWaveServer::Packet::DataType::Double)
        {
            result["dataType"] = "double";
            result["samples"] = packet.getData<double> (); 
        }
        else if (dataType == UWaveServer::Packet::DataType::Float)
        {
            result["dataType"] = "float";
            result["samples"] = packet.getData<float> (); 
        }
        else if (dataType == UWaveServer::Packet::DataType::Text)
        {
            result["dataType"] = "text";
            result["samples"] = packet.getData<char> (); 
        }
        else
        {
            spdlog::warn("Undefined data type");
            result["dataType"] = nullptr;
            result["samples"] = nullptr;
        }
    }
    return result;    
}

crow::json::wvalue 
    packetsToCrowJSON(const std::vector<UWaveServer::Packet> &packets)
{
    crow::json::wvalue result;
    if (packets.empty()){return result;}
    // Get the unique sensor names
    std::set<std::string> sensors;
    for (const auto &packet : packets)
    {   
        auto name = ::toName(packet);
        if (!sensors.contains(name))
        {
            sensors.insert(name);
        }
    }
    // Package each packet
    crow::json::wvalue::list jsonSensors;
    for (const auto &sensor : sensors)
    {
        std::vector<std::pair<std::chrono::microseconds, int>> workSpace;
        workSpace.reserve(packets.size());
        std::string network;
        std::string station;
        std::string channel;
        std::string locationCode;
        for (int i = 0; i < static_cast<int> (packets.size()); ++i)
        {
            auto name = ::toName(packets[i]);
            if (name == sensor)
            {
                if (network.empty())
                {
                    network = packets[i].getNetwork();
                    station = packets[i].getStation();
                    channel = packets[i].getChannel();
                    locationCode = packets[i].getLocationCode();
                }
                workSpace.push_back(std::pair {packets[i].getStartTime(), i});
            }
        }
        if (!workSpace.empty())
        {
            crow::json::wvalue sensorData;
            sensorData["network"] = network;
            sensorData["station"] = station;
            sensorData["channel"] = channel;
            if (locationCode.empty()){locationCode = "--";}
            sensorData["locationCode"] = locationCode;
            // Argsort 
            std::sort(workSpace.begin(), workSpace.end(),
                      [](const auto &lhs, const auto &rhs)
                      {
                          return lhs.first < rhs.first;
                      });
            // Pack data in order
            crow::json::wvalue::list packetsJSON;
            for (const auto &index : workSpace)
            {
                try
                {
                    auto jsonPacket
                        = ::packetToCrowJSON(packets.at(index.second));
                    packetsJSON.push_back(std::move(jsonPacket));
                    //packetsJSON.push_back(::toJSON(packets.at(index.second)));
                }
                catch (const std::exception &e)
                {
                     spdlog::warn("Skipping packet because "
                                + std::string {e.what()});
                }
            }
            sensorData["packets"] = std::move(packetsJSON);
            jsonSensors.push_back(std::move(sensorData));
        }
    }
    result["data"] = std::move(jsonSensors);
    return result;
}

/*
nlohmann::json toJSON(const UWaveServer::Packet &packet)
{
    nlohmann::json result;
    result["samplingRate"] = packet.getSamplingRate();
    result["startTimeMuSec"] = packet.getStartTime().count();
    auto dataType = packet.getDataType(); 
    if (!packet.empty())
    {
        if (dataType == UWaveServer::Packet::DataType::Integer32)
        {
            result["dataType"] = "int32_t";
            result["samples"] = packet.getData<int> ();
        }
        else if (dataType == UWaveServer::Packet::DataType::Integer64)
        {
            result["dataType"] = "int64_t";
            result["samples"] = packet.getData<int64_t> ();
        }
        else if (dataType == UWaveServer::Packet::DataType::Double)
        {
            result["dataType"] = "double";
            result["samples"] = packet.getData<double> ();
        }
        else if (dataType == UWaveServer::Packet::DataType::Float)
        {
            result["dataType"] = "float";
            result["samples"] = packet.getData<float> ();
        }
        else
        {
            spdlog::warn("Undefined data type");
            result["dataType"] = nullptr;
            result["samples"] = nullptr;
        }
    }
    else
    {
        result["dataType"] = nullptr;
        result["samples"] = nullptr;
    }
    return result;
}

nlohmann::json toJSON(const std::vector<UWaveServer::Packet> &packets)
{
    nlohmann::json result;
    if (packets.empty()){return result;}
    // Get the unique sensor names
    std::set<std::string> sensors;
    for (const auto &packet : packets)
    {
        auto name = ::toName(packet);
        if (!sensors.contains(name))
        {
            sensors.insert(name);
        }
    }
    // Package each packet
    for (const auto &sensor : sensors)
    {
        std::vector<std::pair<std::chrono::microseconds, int>> workSpace;
        workSpace.reserve(packets.size());
        std::string network;
        std::string station;
        std::string channel;
        std::string locationCode;
        for (int i = 0; i < static_cast<int> (packets.size()); ++i)
        {
            auto name = ::toName(packets[i]);
            if (name == sensor)
            {
                if (network.empty())
                {
                    network = packets[i].getNetwork();
                    station = packets[i].getStation();
                    channel = packets[i].getChannel();
                    locationCode = packets[i].getLocationCode();
                }
                workSpace.push_back(std::pair {packets[i].getStartTime(), i});
                //::toJSON(packet); 
            }
        }
        if (!workSpace.empty())
        {
            nlohmann::json sensorData;
            sensorData["network"] = network;
            sensorData["station"] = station;
            sensorData["channel"] = channel;
            if (locationCode.empty()){locationCode = "--";}
            sensorData["locationCode"] = locationCode;
            // Argsort 
            std::sort(workSpace.begin(), workSpace.end(),
                      [](const auto &lhs, const auto &rhs)
                      {
                          return lhs.first < rhs.first;
                      });
            // Pack data in order
            nlohmann::json packetsJSON;
            for (const auto &index : workSpace)
            {
                try
                {
                     packetsJSON.push_back(::toJSON(packets.at(index.second)));
                }
                catch (const std::exception &e)
                {
                     spdlog::warn(e.what());
                }
            }
            sensorData["packets"] = packetsJSON;
            result.push_back(sensorData);
        }
    }
    return result;
}
*/

}
#endif
