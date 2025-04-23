#include <iostream>
#include <mutex>
#include <string>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <spdlog/spdlog.h>
#include <soci/soci.h>
#include "uWaveServer/database/client.hpp"
#include "uWaveServer/packet.hpp"
#include "uWaveServer/database/connection/postgresql.hpp"

using namespace UWaveServer::Database;

namespace
{

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

template<typename T>
void fill(const int nSamples,
          const int sensorIdentifier,
          const int64_t packetNumber,
          const double startTime,
          const double samplingRate,
          const T *valuesIn,
          std::vector<int> &sensorIdentifiers,
          std::vector<int64_t> &packetNumbers,
          std::vector<double> &samplingRates,
          std::vector<double> &times,
          std::vector<T> &values)
{
    sensorIdentifiers.resize(nSamples, sensorIdentifier);
    packetNumbers.resize(nSamples, packetNumber);
    samplingRates.resize(nSamples, samplingRate);
    values.resize(nSamples);
    std::copy(valuesIn, valuesIn + nSamples, values.begin()); 
    auto samplingPeriod = 1./samplingRate;
    times.resize(nSamples);
    for (int i = 0; i < nSamples; ++i)
    {
        times[i] = startTime + static_cast<double> (i)*samplingPeriod;
    }
}

}

class Client::ClientImpl
{
public:
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
    [[nodiscard]] int getSensorIdentifier(const Packet &packet)
    {
        auto network = packet.getNetwork();
        auto station = packet.getStation();
        auto channel = packet.getChannel();
        auto locationCode = packet.getLocationCode();
        auto name = ::toName(network, station, channel, locationCode);
        auto index = mSensorIdentifiers.find(name);
        if (index != mSensorIdentifiers.end())
        {
            return index->second;
        }
        int identifier{-1};
        auto session
            = reinterpret_cast<soci::session *> (mConnection.getSession());
        *session <<
            "COALESCE(SELECT identifier FROM sensors WHERE network = :network AND station = :station AND channel = :channel AND location_code = :locationCode), -1)",
            soci::use(network),
            soci::use(station),
            soci::use(channel),
            soci::use(locationCode),
            soci::into(identifier);
        // It doesn't exist so add it
        if (identifier ==-1)
        {
            //auto samplingRate = packet.getSamplingRate();
            {
            soci::transaction tr(*session);
            *session <<
                "COALESCE(INSERT INTO sensors (network, station, channel, location_code, high_sample_rate) VALUES (:network, :station, :channel, :locationCode) RETURNING identifier, -1)",
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
            mSensorIdentifiers.insert(std::pair {name, identifier}); 
        }
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
    void insert(const Packet &packet)
    {
        if (packet.empty())
        {
            spdlog::warn("Packet has no data - returning");
            return;
        } 
        // Ensure 
        auto session
            = reinterpret_cast<soci::session *> (mConnection.getSession()); 
        if (!mConnection.isConnected())
        {
            spdlog::info("Attempting to reconnect...");
            mConnection.connect();
            if (!mConnection.isConnected())
            {
                throw std::runtime_error("Could not connect to timescaledb database!");
            }
        }   
        auto packetNumber = getNextPacketNumber(); // Throws
        auto sensorIdentifier = getSensorIdentifier(packet); // Throws
        auto nSamples = packet.size(); 
        auto dataType = packet.getDataType();
        std::vector<int64_t> packetNumbers;
        std::vector<int> sensorIdentifiers;
        std::vector<double> times;
        std::vector<double> samplingRates;
        if (dataType == UWaveServer::Packet::DataType::Integer32)
        {
            // N.B. we want to do a bulk insert
            std::vector<int> values;
            ::fill(nSamples,
                   sensorIdentifier,
                   packetNumber,
                   packet.getStartTime().count()*1.e-6,
                   packet.getSamplingRate(),
                   static_cast<const int *> (packet.data()),
                   sensorIdentifiers,
                   packetNumbers, 
                   samplingRates,
                   times,
                   values);
            {
            soci::transaction tr(*session);
            soci::statement statement = (session->prepare <<
                "INSERT INTO internal.integer32_waveforms(sensor_identifier, time, sampling_rate, packet_number, value) VALUES(:sensorIdentifier, TO_TIMESTAMP(:time), :samplingRate, :packetNumber, :value)",
                soci::use(sensorIdentifiers),
                soci::use(times),
                soci::use(samplingRates),
                soci::use(packetNumbers),
                soci::use(values)); 
            for (int i = 0; i != nSamples; ++i)
            {
                statement.execute(true);
            }
            tr.commit();
            } 
        }
        else if (dataType == UWaveServer::Packet::DataType::Float)
        {
            std::vector<float> values;
            ::fill(nSamples,
                   sensorIdentifier,
                   packetNumber,
                   packet.getStartTime().count()*1.e-6,
                   packet.getSamplingRate(),
                   static_cast<const float *> (packet.data()),
                   sensorIdentifiers,
                   packetNumbers,
                   samplingRates,
                   times,
                   values);
        }
        else if (dataType == UWaveServer::Packet::DataType::Double)
        {
            std::vector<double> values;
            ::fill(nSamples,
                   sensorIdentifier,
                   packetNumber,
                   packet.getStartTime().count()*1.e-6,
                   packet.getSamplingRate(),
                   static_cast<const double *> (packet.data()),
                   sensorIdentifiers,
                   packetNumbers,
                   samplingRates,
                   times,
                   values);
        }
        else if (dataType == UWaveServer::Packet::DataType::Integer64)
        {
            std::vector<int64_t> values;
            ::fill(nSamples,
                   sensorIdentifier,
                   packetNumber,
                   packet.getStartTime().count()*1.e-6,
                   packet.getSamplingRate(),
                   static_cast<const int64_t *> (packet.data()),
                   sensorIdentifiers,
                   packetNumbers,
                   samplingRates,
                   times,
                   values);
        }
        else
        {
            if (dataType == UWaveServer::Packet::DataType::Unknown)
            {
                throw std::invalid_argument("Data type cannot be unknown");
            }
            throw std::runtime_error("Unhandled data type");
        }

    }
    void getRetentionPolicy()
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
            mRetentionPolicy = duration;
            spdlog::info("Using retention policy of " + std::to_string(mRetentionPolicy.count()) + " seconds");
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
            getRetentionPolicy();
        }
        catch (const std::exception &e)
        {
            spdlog::warn("Failed to get retention policy.  Failed with"
                       + std::string {e.what()});
        }
        initializeSensors();
    }
    mutable std::mutex mMutex;
    Connection::PostgreSQL mConnection;
    std::map<std::string, int> mSensorIdentifiers;
    std::chrono::seconds mRetentionPolicy{365*86400}; // Make it something large like a year
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
    pImpl->insert(packet);
}
