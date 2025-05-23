#include <iostream>
#include <iomanip>
#include <bit>
#include <limits>
#include <algorithm>
#include <mutex>
#include <string>
#include <vector>
#ifndef NDEBUG
#include <cassert>
#endif
#include <boost/algorithm/string.hpp>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <soci/soci.h>
#include "uWaveServer/database/client.hpp"
#include "uWaveServer/packet.hpp"
#include "uWaveServer/database/connection/postgresql.hpp"
#include "private/pack.hpp"

#define BATCHED_QUERY
#define PACKET_BASED_SCHEMA
#define USE_BYTEA

using namespace UWaveServer::Database;

namespace
{

/*
std::string pack(const int n, const int *values)
{
    std::string result(4*n, '\0');
    for (int i = 0; i < n; ++i)
    {
        packi4(values[i], result.data() + 4*i);
    }
    return result;
}

std::string pack(const int n, const float *values)
{
    std::string result(4*n, '\0');
    for (int i = 0; i < n; ++i)
    {
        packf4(values[i], result.data() + 4*i);
    }
    return result;
}

std::string pack(const int n, const double *values)
{
    std::string result(8*n, '\0');
    for (int i = 0; i < n; ++i)
    {
        packf8(values[i], result.data() + 8*i);
    }
    return result;
}

std::string pack(const int n, const int64_t *values)
{
    std::string result(8*n, '\0');
    for (int i = 0; i < n; ++i)
    {
        packi8(values[i], result.data() + 8*i);
    }
    return result;
}
*/

UWaveServer::Packet
    queryRowToPacket(const double queryStartTime,
                     const double queryEndTime,
                     const std::string &network,
                     const std::string &station,
                     const std::string &channel,
                     const std::string &locationCode,
                     const double packetStartTime,
                     const double samplingRate,
                     const int nSamples,
                     const int8_t endiannes,
                     const char dataType,
                     std::string &hexStringData,
                     const bool swapBytes)
{
    UWaveServer::Packet packet;
    packet.setNetwork(network);
    packet.setStation(station);
    packet.setChannel(channel);
    packet.setLocationCode(locationCode);
    packet.setSamplingRate(samplingRate);
    packet.setStartTime(packetStartTime);
    if (nSamples > 0 && !hexStringData.empty())
    {
        if (hexStringData.find("\\x") == 0)
        {
            hexStringData.erase(0, 2);
        } 
        if (dataType == 'i')
        {
            auto data = ::unpackHexRepresentation<int> (hexStringData, nSamples, swapBytes);
            packet.setData(std::move(data));
        }
        else if (dataType == 'f')
        {
            auto data = ::unpackHexRepresentation<float> (hexStringData, nSamples, swapBytes);
            packet.setData(std::move(data));
        }
        else if (dataType == 'l')
        {
            auto data = ::unpackHexRepresentation<int64_t> (hexStringData, nSamples, swapBytes);
            packet.setData(std::move(data));
        }
        else if (dataType == 'd')
        {
            auto data = ::unpackHexRepresentation<double> (hexStringData, nSamples, swapBytes);
            packet.setData(std::move(data));
        }
        else
        {
            throw std::invalid_argument("Unhandled precision");
        }
        if (packet.empty()){throw std::runtime_error("Packet has no data samples");}
        packet.trim(queryStartTime, queryEndTime);
    }
    return packet;
}


UWaveServer::Packet
    queryRowToPacket(const double queryStartTime,
                     const double queryEndTime,
                     const std::string &network,
                     const std::string &station,
                     const std::string &channel,
                     const std::string &locationCode,
                     const double packetStartTime,
                     const double samplingRate,
                     const std::string &jsonStringData)
{
    UWaveServer::Packet packet;
    packet.setNetwork(network);
    packet.setStation(station);
    packet.setChannel(channel);
    packet.setLocationCode(locationCode);
    packet.setSamplingRate(samplingRate);
    packet.setStartTime(packetStartTime);
    auto jsonData = nlohmann::json::parse(jsonStringData);
    if (jsonData.contains("dataType") && jsonData.contains("samples"))
    {
        auto dataType = jsonData["dataType"].template get<std::string> ();
        if (dataType == "integer")
        {
            auto data = jsonData["samples"].template get<std::vector<int>> ();
            if (!data.empty()){packet.setData(std::move(data));}
        }
        else if (dataType == "double")
        {
            auto data
                = jsonData["samples"].template get<std::vector<double>> ();
            if (!data.empty()){packet.setData(std::move(data));}
        }
        else if (dataType == "integer64")
        {
            auto data
                = jsonData["samples"].template get<std::vector<int64_t>> ();
            if (!data.empty()){packet.setData(std::move(data));}
        }
        else if (dataType == "float")
        {
            auto data
                = jsonData["samples"].template get<std::vector<float>> ();
            if (!data.empty()){packet.setData(std::move(data));}
        }
        else
        {
            throw std::runtime_error("Unhandled data type " + dataType);
        }
        packet.trim(queryStartTime, queryEndTime);
    }
    else
    {
        throw std::runtime_error(
            "JSON packet missing dataType or samples field");
    }
    return packet;
}

std::vector<UWaveServer::Packet>
    queryRowsToPackets(const double queryStartTime,
                       const double queryEndTime,
                       const std::string &network,
                       const std::string &station,
                       const std::string &channel,
                       const std::string &locationCode,
                       const std::vector<double> &packetStartTimes,
                       const std::vector<double> &samplingRates,
                       const std::vector<int> &packetSizes,
                       const std::vector<int8_t> &endians,
                       const std::vector<char> &dataTypes,
                       std::vector<std::string> &hexStringDatas,
                       const bool swapBytes)
{
    std::vector<UWaveServer::Packet> result;
    int nPackets = static_cast<int> (packetStartTimes.size());
#ifndef NDEBUG
    assert(packetStartTimes.size() == samplingRates.size());
    assert(packetStartTimes.size() == packetSizes.size());
    assert(packetStartTimes.size() == endians.size());
    assert(packetStartTimes.size() == dataTypes.size());
    assert(packetStartTimes.size() == hexStringDatas.size());
#endif 
    if (nPackets < 1){return result;}
    result.reserve(nPackets);
    for (int i = 0; i < nPackets; ++i)
    {
        try
        {
            //std::cout << std::setprecision(16) << queryStartTime << " " << packetStartTimes[i] << queryEndTime << std::endl;
            auto packet = ::queryRowToPacket(queryStartTime,
                                             queryEndTime,
                                             network,
                                             station,
                                             channel,
                                             locationCode,
                                             packetStartTimes[i],
                                             samplingRates[i],
                                             packetSizes[i],
                                             endians[i], 
                                             dataTypes[i],
                                             hexStringDatas[i],
                                             swapBytes);
            if (!packet.empty()){result.push_back(std::move(packet));}
        }
        catch (const std::exception &e)
        {
            spdlog::warn("Failed to unpack packet because "
                       + std::string {e.what()});
        }
    }
    std::sort(result.begin(), result.end(),
              [](const auto &lhs, const auto &rhs)
              {
                  return lhs.getStartTime() < rhs.getStartTime();
              });
    return result;
}

std::vector<UWaveServer::Packet> 
    queryRowsToPackets(const double queryStartTime,
                       const double queryEndTime,
                       const std::string &network,
                       const std::string &station,
                       const std::string &channel,
                       const std::string &locationCode,
                       const std::vector<double> &packetStartTimes,
                       const std::vector<double> &samplingRates,
                       const std::vector<std::string> &jsonStringDatas)
{
    std::vector<UWaveServer::Packet> result;
    int nPackets = static_cast<int> (packetStartTimes.size());
#ifndef NDEBUG
    assert(packetStartTimes.size() == samplingRates.size());
    assert(packetStartTimes.size() == jsonStringDatas.size());
#endif 
    if (nPackets < 1){return result;}
    result.reserve(nPackets);
    for (int i = 0; i < nPackets; ++i)
    {
        try
        {
            //std::cout << std::setprecision(16) << queryStartTime << " " << packetStartTimes[i] << queryEndTime << std::endl;
            auto packet = ::queryRowToPacket(queryStartTime,
                                             queryEndTime,
                                             network,
                                             station,
                                             channel,
                                             locationCode,
                                             packetStartTimes[i],
                                             samplingRates[i],
                                             jsonStringDatas[i]);
            if (!packet.empty()){result.push_back(std::move(packet));}
        }
        catch (const std::exception &e)
        {
            spdlog::warn("Failed to unpack packet because " + std::string {e.what()});
        }
    }
    std::sort(result.begin(), result.end(),
              [](const auto &lhs, const auto &rhs)
              {
                  return lhs.getStartTime() < rhs.getStartTime();
              });
    return result;
}

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

std::string toName(const std::string &network,
                   const std::string &station,
                   const std::string &channel,
                   const std::string &locationCode)
{
    auto name = network + "." + station + "." + channel;
    if (!locationCode.empty())
    {   
        name = name + "." + locationCode;
    }   
    return name;
}

std::string toName(const UWaveServer::Packet &packet)
{
    auto network = packet.getNetwork();
    auto station = packet.getStation();
    auto channel = packet.getChannel();
    auto locationCode = packet.getLocationCode();
    return ::toName(network, station, channel, locationCode);
}

template<typename T, typename U>
void fill(const int nFill,
          const int offset,
          const double startTime,
          const double samplingPeriod,
          const T *valuesIn,
          std::vector<double> &times,
          std::vector<U> &values)
{
#ifndef NDEBUG
    assert(nFill == static_cast<int> (values.size()));
    assert(nFill == static_cast<int> (times.size()));
#endif
    std::copy(valuesIn + offset,
              valuesIn + offset + nFill,
              values.begin()); 
    for (int i = 0; i < nFill; ++i)
    {
        times[i] = startTime
                 + static_cast<double> (offset + i)*samplingPeriod;
    }
}

}

class Client::ClientImpl
{
public:
/*
    [[nodiscard]] int64_t getNextPacketNumber()
    {
        const std::string sequenceName{"packet_number_sequence"};
        auto session
            = reinterpret_cast<soci::session *> (mConnection.getSession());
        int64_t sequenceValue{-1};
        {
        soci::transaction tr(*session);
        *session <<  "SELECT nextval(:sequenceName)",
                     soci::use(sequenceName),
                     soci::into(sequenceValue);
        tr.commit();
        }
        if (sequenceValue < 0)
        {
            throw std::runtime_error("Failed to get next packet number");
        }
        return sequenceValue;
    }
*/
    [[nodiscard]] int getSensorIdentifier(const std::string &network,
                                          const std::string &station,
                                          const std::string &channel,
                                          const std::string &locationCode,
                                          const bool addIfNotExists) const
    {
        auto name = ::toName(network, station, channel, locationCode);
        // Maybe we already have this channel 
        {
        std::scoped_lock lock(mMutex);
        auto index = mSensorIdentifiers.find(name);
        if (index != mSensorIdentifiers.end())
        {
            return index->second;
        }
        }
        // Okay - let's look in the database for it
        int identifier{-1};
        auto session
            = reinterpret_cast<soci::session *> (mConnection.getSession());
        *session <<
            "SELECT COALESCE( (SELECT identifier FROM sensors WHERE network = :network AND station = :station AND channel = :channel AND location_code = :locationCode), -1)",
            soci::use(network),
            soci::use(station),
            soci::use(channel),
            soci::use(locationCode),
            soci::into(identifier);
        // This channel doesn't exist in the database so add it
        if (identifier ==-1)
        {
            if (!addIfNotExists)
            {
                spdlog::debug("Sensor " + name + " does not exist");
                return identifier;
            }
            {
            soci::transaction tr(*session);
            *session <<
                "INSERT INTO sensors (network, station, channel, location_code) VALUES (:network, :station, :channel, :locationCode) RETURNING identifier",
                soci::use(network),
                soci::use(station),
                soci::use(channel),
                soci::use(locationCode), 
                soci::into(identifier);
            tr.commit();
            }
            if (identifier ==-1)
            {
                throw std::runtime_error("Could not add " + name + " to sensors");
            }
            {
            std::scoped_lock lock(mMutex);
            mSensorIdentifiers.insert(std::pair {name, identifier}); 
            }
        }
        return identifier; 
    }
    [[nodiscard]] int getSensorIdentifier(const Packet &packet, const bool addIfNotExists) const
    {   
        auto network = packet.getNetwork();
        auto station = packet.getStation();
        auto channel = packet.getChannel();
        auto locationCode = packet.getLocationCode();
        return getSensorIdentifier(network, station, channel, locationCode, addIfNotExists);
    }
    void initializeSensors()
    {
        std::vector<std::string> names;
        std::vector<int> identifiers;
        auto session
            = reinterpret_cast<soci::session *> (mConnection.getSession());
        soci::rowset<soci::row> rows = (session->prepare << "SELECT identifier, network, station, channel, location_code FROM sensors");
        for (auto &row : rows)
        {
            auto identifier = row.get<int> (0);
            auto network = row.get<std::string> (1);
            auto station = row.get<std::string> (2);
            auto channel = row.get<std::string> (3);
            auto locationCode = row.get<std::string> (4);
            auto name = ::toName(network, station, channel, locationCode);
            names.push_back(name);
            identifiers.push_back(identifier);
        }
        // Set the sensors
        if (!names.empty())
        {
            {
            std::scoped_lock lock(mMutex);
            mSensorIdentifiers.clear();
            for (int i = 0; i < static_cast<int> (names.size()); ++i)
            { 
                try
                {
                    mSensorIdentifiers.insert(
                        std::pair{names.at(i), identifiers.at(i)} );
                }
                catch (const std::exception &e)
                {
                    spdlog::warn("Could not add "
                               + names[i]
                               + " to sensor map");
                }
            }
            }
            spdlog::debug(std::to_string(mSensorIdentifiers.size())
                        + " sensors in map");
        }
    }
    std::vector<Packet> query(const std::string &network,
                              const std::string &station,
                              const std::string &channel,
                              const std::string &locationCode,
                              const double startTime,
                              const double endTime) const
    {
        std::vector<Packet> result;
        // Ensure we're connected
        if (!mConnection.isConnected())
        {
            spdlog::info("In query; attempting to reconnect...");
            mConnection.reconnect();
            if (!mConnection.isConnected())
            {
                throw std::runtime_error("Could not connect to timescaledb database!");
            }
        }
        // Check the sensor is there
        constexpr bool addIfNotExists{false};
        auto sensorIdentifier
            = getSensorIdentifier(network, station, channel,
                                  locationCode, addIfNotExists); // Throws
        if (sensorIdentifier < 0)
        {
            throw std::invalid_argument("Could not obtain sensor identifier");
        }
        // Time to work
        auto session
            = reinterpret_cast<soci::session *> (mConnection.getSession());
#ifndef NDEBUG  
        auto queryTimeStart = std::chrono::high_resolution_clock::now();
#endif              
#ifdef BATCHED_QUERY
        constexpr int batchSize{64};
  #ifdef USE_BYTEA
        std::vector<double> packetStartTimes(batchSize);
        std::vector<double> packetSamplingRates(batchSize);
        std::vector<int> packetLengths(batchSize);
        std::vector<char> dataSignifiers(batchSize);
        std::vector<int8_t> endiannesses(batchSize);
        std::vector<std::string> packetByteArrays(batchSize);

        std::vector<double> allPacketStartTimes;
        allPacketStartTimes.reserve(512);
        std::vector<double> allPacketSamplingRates;
        allPacketSamplingRates.reserve(512);
        std::vector<int> allPacketLengths;
        allPacketLengths.reserve(512);
        std::vector<char> allDataSignifiers;
        allDataSignifiers.reserve(512);
        std::vector<int8_t> allEndiannesses;
        allEndiannesses.reserve(512);
        std::vector<std::string> allPacketByteArrays;
        allPacketByteArrays.reserve(512); 

        soci::statement statement = (session->prepare <<
            "SELECT EXTRACT(epoch FROM start_time), sampling_rate, n_samples, datatype, little_endian, data FROM packet WHERE sensor_identifier = :sensorIdentifier AND end_time > TO_TIMESTAMP(:startTime) AND start_time < TO_TIMESTAMP(:endTime)",
            soci::use(sensorIdentifier),
            soci::use(startTime),
            soci::use(endTime),
            soci::into(packetStartTimes),
            soci::into(packetSamplingRates),
            soci::into(packetLengths),
            soci::into(dataSignifiers),
            soci::into(endiannesses),
            soci::into(packetByteArrays));
        statement.execute();
        while (statement.fetch())
        {
            allPacketStartTimes.insert(allPacketStartTimes.end(),
                                       packetStartTimes.begin(),
                                       packetStartTimes.end());
            allPacketSamplingRates.insert(allPacketSamplingRates.end(),
                                          packetSamplingRates.begin(),
                                          packetSamplingRates.end());
            allPacketLengths.insert(allPacketLengths.end(),
                                    packetLengths.begin(),
                                    packetLengths.end());
            allDataSignifiers.insert(allDataSignifiers.end(),
                                     dataSignifiers.begin(),
                                     dataSignifiers.end());
            allEndiannesses.insert(allEndiannesses.end(),
                                   endiannesses.begin(),
                                   endiannesses.end());
            allPacketByteArrays.insert(allPacketByteArrays.end(),
                                       packetByteArrays.begin(),
                                       packetByteArrays.end());

            packetStartTimes.resize(batchSize);
            packetSamplingRates.resize(batchSize);
            packetLengths.resize(batchSize);
            dataSignifiers.resize(batchSize);
            endiannesses.resize(batchSize);
            packetByteArrays.resize(batchSize);
        }
  #else // BYTEA
        std::vector<double> packetStartTimes(batchSize);
        std::vector<double> samplingRates(batchSize);
        std::vector<std::string> stringDatas(batchSize);
        std::vector<double> allPacketStartTimes;
        allPacketStartTimes.reserve(512);
        std::vector<double> allSamplingRates;
        allSamplingRates.reserve(512);
        std::vector<std::string> allStringDatas; 
        allStringDatas.reserve(512);
        soci::statement statement = (session->prepare <<
            "SELECT EXTRACT(epoch FROM start_time), sampling_rate, data FROM packet WHERE sensor_identifier = :sensorIdentifier AND end_time > TO_TIMESTAMP(:startTime) AND start_time < TO_TIMESTAMP(:endTime)",
            soci::use(sensorIdentifier),
            soci::use(startTime),
            soci::use(endTime),
            soci::into(packetStartTimes),
            soci::into(samplingRates),
            soci::into(stringDatas));
        statement.execute();
        while (statement.fetch())
        {
            allStringDatas.insert(allStringDatas.end(),
                                  stringDatas.begin(), stringDatas.end());
            allSamplingRates.insert(allSamplingRates.end(),
                                    samplingRates.begin(), samplingRates.end()); 
            allPacketStartTimes.insert(allPacketStartTimes.end(),
                                       packetStartTimes.begin(), packetStartTimes.end());
            packetStartTimes.resize(batchSize);
            samplingRates.resize(batchSize);
            stringDatas.resize(batchSize);
        }
  #endif
  #ifndef NDEBUG
        auto queryTimeEnd = std::chrono::high_resolution_clock::now();
        double queryDuration
            = std::chrono::duration_cast<std::chrono::microseconds>
              (queryTimeEnd - queryTimeStart).count()*1.e-6;
        spdlog::info("Query time to recover "
                   + std::to_string(allPacketStartTimes.size())
                   + " packets was " + std::to_string(queryDuration) + " (s)");
        auto unpackTimeStart = std::chrono::high_resolution_clock::now();
  #endif
  #ifdef USE_BYTEA
        result = ::queryRowsToPackets(startTime,
                                      endTime,
                                      network,
                                      station,
                                      channel,
                                      locationCode,
                                      allPacketStartTimes,
                                      allPacketSamplingRates,
                                      allPacketLengths,
                                      allEndiannesses,
                                      allDataSignifiers,
                                      allPacketByteArrays,
                                      mSwapBytes);
  #else
        result = ::queryRowsToPackets(startTime,
                                      endTime,
                                      network,
                                      station,
                                      channel,
                                      locationCode,
                                      allPacketStartTimes,
                                      allSamplingRates,
                                      allStringDatas);
  #endif
  #ifndef NDEBUG
        auto unpackTimeEnd = std::chrono::high_resolution_clock::now();
        double unpackDuration
            = std::chrono::duration_cast<std::chrono::microseconds>
              (unpackTimeEnd - unpackTimeStart).count()*1.e-6;
        spdlog::info("Unpack time was  "
                    + std::to_string(unpackDuration) + " (s)");

  #endif
#else // not batched query
        double packetStartTime{0};
        double samplingRate{0};
        //int64_t packetNumber{0};
        std::string stringData;
        // TODO bad query -> use the endTime
        soci::statement statement = (session->prepare <<
            "SELECT EXTRACT(epoch FROM start_time), sampling_rate, data FROM packet WHERE sensor_identifier = :sensorIdentifier AND start_time BETWEEN TO_TIMESTAMP(:startTime) AND TO_TIMESTAMP(:startTime) + MAKE_INTERVAL(secs => (n_samples - 1)/sampling_rate)",
            soci::use(sensorIdentifier),
            soci::use(startTime),
            soci::use(endTime),
            soci::into(packetStartTime),
            soci::into(samplingRate),
            soci::into(stringData));
        statement.execute();
        int nPacketsRead = 0;
        while (statement.fetch())
        {
            nPacketsRead = nPacketsRead + 1;
            if (!stringData.empty())
            {
                UWaveServer::Packet packet;
                try
                {
                    packet.setNetwork(network);
                    packet.setStation(station);
                    packet.setChannel(channel);
                    packet.setLocationCode(locationCode);
                    packet.setSamplingRate(samplingRate);
                    packet.setStartTime(startTime);
                    auto jsonData = nlohmann::json::parse(stringData);
                    if (jsonData.contains("dataType") && jsonData.contains("samples"))
                    {
                        auto dataType = jsonData["dataType"].template get<std::string> ();
                        if (dataType == "integer")
                        {
                            auto data = jsonData["samples"].template get<std::vector<int>> ();
                            if (!data.empty()){packet.setData(std::move(data));}
                        }
                        else if (dataType == "double")
                        {
                            auto data = jsonData["samples"].template get<std::vector<double>> ();
                            if (!data.empty()){packet.setData(std::move(data));}
                        }    
                        else if (dataType == "integer64")
                        {
                            auto data = jsonData["samples"].template get<std::vector<int64_t>> ();
                            if (!data.empty()){packet.setData(std::move(data));}
                        }
                        else if (dataType == "float")
                        {
                            auto data = jsonData["samples"].template get<std::vector<float>> ();
                            if (!data.empty()){packet.setData(std::move(data));}
                        }
                        else
                        {
                            spdlog::warn("Unhandled data type: " + dataType);
                            continue;
                        } 
                    }
                    else
                    {
                        spdlog::warn("Need dataType and samples key");
                        continue;
                    }
                    result.push_back(std::move(packet));
                }
                catch (const std::exception &e)
                {
                    spdlog::warn(e.what());
                } 
           }
        }
        std::sort(result.begin(), result.end(),
                  [](const auto &lhs, const auto &rhs)
                  {
                      return lhs.getStartTime() < rhs.getStartTime();
                  });
#ifndef NDEBUG
        unpackTimeEnd = std::chrono::high_resolution_clock::now();
        unpackDuration
             = std::chrono::duration_cast<std::chrono::microseconds>
               (unpackTimeEnd - unpackTimeStart).count()*1.e-6;
        spdlog::info("Unpack time was  "
                    + std::to_string(unpackDuration) + " (s)");
#endif
#endif
        return result;
    }
 
    void insert(const Packet &packet)
    {
        if (packet.empty())
        {
            spdlog::warn("Packet has no data - returning");
            return;
        }
        // Ensure we're connected
        if (!mConnection.isConnected())
        {
            spdlog::info("In insert; attempting to reconnect...");
            mConnection.reconnect();
            if (!mConnection.isConnected())
            {
                throw std::runtime_error("Could not connect to timescaledb database!");
            }
        }
        // Get the sensor identifier
        constexpr bool addIfNotExists{true};
        auto sensorIdentifier = getSensorIdentifier(packet, addIfNotExists); // Throws
        if (sensorIdentifier < 0) 
        {
            throw std::runtime_error("Could not obtain sensor identifier");
        }
        // Okay, let's get to work
        auto session
            = reinterpret_cast<soci::session *> (mConnection.getSession()); 
        // TODO this needs tuning.  It appears multiple batches results
        // in some performance degradation so for now let's keep this big.
        //int64_t packetNumber = getNextPacketNumber(); // Throws
        auto nSamples = static_cast<int> (packet.size()); 
        auto startTime = packet.getStartTime().count()*1.e-6;
        auto endTime = packet.getEndTime().count()*1.e-6;
        auto samplingRate = packet.getSamplingRate();
        auto dataType = packet.getDataType();
#ifdef USE_BYTEA
        constexpr bool usePrefix{false}; // PG doesn't require us to prepend a 0x 
        std::string hexEncodedData;
        char dataTypeSignifier = 'i';
        if (dataType == UWaveServer::Packet::DataType::Integer32)
        {
            auto dataPtr = static_cast<const int *> (packet.data());
            hexEncodedData
                 = ::hexRepresentation(dataPtr, nSamples, usePrefix, mSwapBytes);
            dataTypeSignifier = 'i';
        }
        else if (dataType == UWaveServer::Packet::DataType::Integer64)
        {
            auto dataPtr = static_cast<const int64_t *> (packet.data());
            hexEncodedData
                 = ::hexRepresentation(dataPtr, nSamples, usePrefix, mSwapBytes);
            dataTypeSignifier = 'l';
        }
        else if (dataType == UWaveServer::Packet::DataType::Double)
        {
            auto dataPtr = static_cast<const double *> (packet.data());
            hexEncodedData
                 = ::hexRepresentation(dataPtr, nSamples, usePrefix, mSwapBytes);
            dataTypeSignifier = 'd';
        }
        else if (dataType == UWaveServer::Packet::DataType::Float)
        {
            auto dataPtr = static_cast<const float *> (packet.data());
            hexEncodedData
                = ::hexRepresentation(dataPtr, nSamples, usePrefix, mSwapBytes);
            dataTypeSignifier = 'f';
        }
#else
        nlohmann::json jsonData;
        if (dataType == UWaveServer::Packet::DataType::Integer32)
        {
            auto dataPtr = static_cast<const int *> (packet.data());
            std::vector<int> data{dataPtr, dataPtr + nSamples};
            jsonData["dataType"] = "integer";
            jsonData["samples"] = std::move(data);
        }
        else if (dataType == UWaveServer::Packet::DataType::Integer64)
        {
            auto dataPtr = static_cast<const int64_t *> (packet.data());
            std::vector<int64_t> data{dataPtr, dataPtr + nSamples};
            jsonData["dataType"] = "integer64";
            jsonData["samples"] = std::move(data);
        }
        else if (dataType == UWaveServer::Packet::DataType::Double)
        {
            auto dataPtr = static_cast<const double *> (packet.data());
            std::vector<double> data{dataPtr, dataPtr + nSamples};
            jsonData["dataType"] = "double";
            jsonData["samples"] = std::move(data);
        }
        else if (dataType == UWaveServer::Packet::DataType::Float)
        {
            auto dataPtr = static_cast<const float *> (packet.data());
            std::vector<float> data{dataPtr, dataPtr + nSamples};
            jsonData["dataType"] = "float";
            jsonData["samples"] = std::move(data);
        }
#endif // USE_BYTEA
        else
        {
#ifndef NDEBUG
            assert(false);
#else
            throw std::runtime_error("Unhandled data type");
#endif
        }
        {
        soci::transaction tr(*session);
#ifdef USE_BYTEA
        constexpr int8_t littleEndian{1}; // Always write as little endian
        soci::statement statement = (session->prepare <<
            "INSERT INTO packet(sensor_identifier, start_time, end_time, sampling_rate, n_samples, datatype, little_endian, data) VALUES (:sensorIdentifier, TO_TIMESTAMP(:startTime), TO_TIMESTAMP(:endTime), :samplingRate, :nSamples, :dataType, :littleEndian, DECODE(:data, 'hex')) ON CONFLICT DO NOTHING",
            soci::use(sensorIdentifier),
            soci::use(startTime),
            soci::use(endTime),
            //soci::use(packetNumber),
            soci::use(samplingRate),
            soci::use(nSamples),
            soci::use(dataTypeSignifier),
            soci::use(littleEndian),
            soci::use(hexEncodedData));
#else
        auto stringData = std::string {jsonData.dump(-1)};
        soci::statement statement = (session->prepare <<
            "INSERT INTO packet(sensor_identifier, start_time, end_time, sampling_rate, data) VALUES (:sensorIdentifier, TO_TIMESTAMP(:startTime), TO_TIMESTAMP(:endTime), :samplingRate, :data) ON CONFLICT DO NOTHING",
            soci::use(sensorIdentifier),
            soci::use(startTime),
            soci::use(endTime),
            //soci::use(packetNumber),
            soci::use(samplingRate),
            //soci::use(nSamples),
            //soci::use(dataTypeSignifier),
            //soci::use(mLittleEndian),
            soci::use(stringData));
#endif
        statement.execute(true);
        tr.commit();
        }
    }
    void getRetentionDuration()
    {
        auto session 
            = reinterpret_cast<soci::session *> (mConnection.getSession());
        std::string result;
        auto schema = mConnection.getSchema();
        if (!schema.empty())
        {
            *session <<
                "SELECT config::json->>'drop_after' AS retention FROM timescaledb_information.jobs WHERE hypertable_schema = :schema AND hypertable_name = 'sample' AND timescaledb_information.jobs.proc_name = 'policy_retention' LIMIT 1",
                soci::use(schema),
                soci::into(result);
        }
        else
        {
            *session << 
               "SELECT config::json->>'drop_after' AS retention FROM timescaledb_information.jobs WHERE hypertable_name = 'sample' AND timescaledb_information.jobs.proc_name = 'policy_retention' LIMIT 1",
            soci::into(result);
        }
        spdlog::debug("Retention policy is: " + result);
        std::chrono::seconds duration{-1};
        if (result.find("day") != std::string::npos)
        {
            std::vector<std::string> splitString;
            boost::split(splitString, result, boost::is_any_of("day"));  
            duration = std::chrono::seconds (std::stoi(splitString.at(0))*86400);
        }
        else if (result.find("hour") != std::string::npos)
        {
            std::vector<std::string> splitString;
            boost::split(splitString, result, boost::is_any_of("hour")); 
            duration = std::chrono::seconds (std::stoi(splitString.at(0))*3600);
        }
        else if (result.find("minute") != std::string::npos)
        {
            std::vector<std::string> splitString;
            boost::split(splitString, result, boost::is_any_of("minute")); 
            duration = std::chrono::seconds (std::stoi(splitString.at(0))*60);
        }
        else if (result.find("second") != std::string::npos)
        {
            std::vector<std::string> splitString;
            boost::split(splitString, result, boost::is_any_of("second")); 
            duration = std::chrono::seconds (std::stoi(splitString.at(0)));
        }
        else
        {
            spdlog::warn("Could not unpack retention policy: " + result
                       + " Using default retention policy");
        }
        if (duration.count() > 0)
        {
            mRetentionDuration = duration;
            spdlog::info("Using retention duration of "
                       + std::to_string(mRetentionDuration.count())
                       + " seconds");
        }
    }
    explicit ClientImpl(Connection::PostgreSQL &&connection)
    {
        mConnection = std::move(connection);
        if (!mConnection.isConnected())
        {
            spdlog::debug("Establishing postgres connection");
            mConnection.connect();
            if (!mConnection.isConnected())
            {
                throw std::runtime_error(
                    "Cannot establish database connection");
            }
        }
        try
        {
            getRetentionDuration();
        }
        catch (const std::exception &e)
        {
            spdlog::warn("Failed to get retention duration.  Failed with"
                       + std::string {e.what()});
        }
#ifdef USE_BYTEA
        if (mSwapBytes)
        {
            spdlog::info("Processor appears to be big endian; will swap bytes");
        }
        else
        {
            spdlog::info("Processor appears to be little endian; will not swap bytes");
        }
#endif
        initializeSensors();
    }
    mutable std::mutex mMutex;
    mutable Connection::PostgreSQL mConnection;
    mutable std::map<std::string, int> mSensorIdentifiers;
    std::chrono::seconds mRetentionDuration{365*86400}; // Make it something large like a year
    bool mSwapBytes{std::endian::native == std::endian::little ? false : true};
};

/// Constructor
Client::Client(Connection::PostgreSQL &&connection) :
    pImpl(std::make_unique<ClientImpl> (std::move(connection)))
{
}

/// Destructor
Client::~Client() = default;

/// Connected?
bool Client::isConnected() const noexcept
{
    return pImpl->mConnection.isConnected();
}

/// Connect
void Client::connect()
{
    pImpl->mConnection.connect();
}

/// Disconnect
void Client::disconnect()
{
    pImpl->mConnection.disconnect();
}

/// Write the data packet
void Client::write(const UWaveServer::Packet &packet)
{
    if (!packet.haveNetwork())
    {
        throw std::invalid_argument("Network not set on packet");
    }
    if (!packet.haveStation())
    {
        throw std::invalid_argument("Station not set on packet");
    }
    if (!packet.haveChannel())
    {
        throw std::invalid_argument("Channel not set on packet");
    }
    if (!packet.haveSamplingRate())
    {
        throw std::invalid_argument("Sampling rate not set on packet");
    }
    if (packet.empty())
    {
        spdlog::warn("Packet has not data - returning");
        return;
    }
    if (packet.getDataType() == UWaveServer::Packet::DataType::Unknown)
    {
        throw std::runtime_error("Packet's data type is unknown");
    }
    // Expired data?
    auto now = std::chrono::high_resolution_clock::now();
    auto endTime = packet.getEndTime();
    std::chrono::microseconds oldestAllowableTime
        = std::chrono::duration_cast<std::chrono::microseconds> 
          (now.time_since_epoch()) 
        - std::chrono::duration_cast<std::chrono::microseconds> 
          (pImpl->mRetentionDuration);
    if (endTime < oldestAllowableTime)
    {
        spdlog::warn(::toName(packet) + "'s data has expired; skipping");
        return;
    } 
    // Try to write it 
    pImpl->insert(packet);
}

std::vector<UWaveServer::Packet> Client::query(
    const std::string &networkIn,
    const std::string &stationIn,
    const std::string &channelIn,
    const std::string &locationCodeIn,
    const double startTime,
    const double endTime) const
{
    if (startTime >= endTime)
    {
        throw std::invalid_argument("Start time must be less han end time");
    }
    auto network = ::convertString(networkIn);
    if (network.empty())
    {
        throw std::invalid_argument("Network is empty");
    }
    auto station = ::convertString(stationIn);
    if (station.empty())
    {
        throw std::invalid_argument("Station is empty");
    } 
    auto channel = ::convertString(channelIn);
    if (channel.empty())
    {
        throw std::invalid_argument("Channel is empty");
    }
    auto locationCode = ::convertString(locationCodeIn);
    return pImpl->query(network, station, channel, locationCode, startTime, endTime); 
}
