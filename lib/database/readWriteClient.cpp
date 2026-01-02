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
#include <pqxx/pqxx>
#include "uWaveServer/database/client.hpp"
#include "uWaveServer/database/credentials.hpp"
#include "uWaveServer/packet.hpp"
#include "private/pack.hpp"
#include "private/toName.hpp"
#ifdef WITH_ZLIB
#include "private/compression.hpp"
#endif

#define BATCHED_QUERY
#define PACKET_BASED_SCHEMA
#define USE_BYTEA

using namespace UWaveServer::Database;

namespace
{

std::string toTableName(const std::string &schema,
                        const std::string &network, const std::string &station)
{
    auto dataTableName = network + "_" + station + "_data";
    //std::replace(dataTableName.begin(), dataTableName.end(), '.', '_');
    if (!schema.empty()){dataTableName = schema + "." + dataTableName;}
    std::transform(dataTableName.begin(), dataTableName.end(),
                   dataTableName.begin(), ::tolower);
    return dataTableName;
}

std::string toTableName(const Credentials &credentials,
                        const std::string &network, const std::string &station)
{
    return ::toTableName(credentials.getSchema(), network, station);
}


UWaveServer::Packet unpackPacket(
    const std::string &network,
    const std::string &station,
    const std::string &channel,
    const std::string &locationCode,
    const double packetStartTime,
    const double packetSamplingRate,
    const char packetDataType,
    std::basic_string<std::byte> &packetByteArray,
    const bool packetIsLittleEndian,
    const bool amLittleEndian,
    const bool packetIsCompressed,
    const int packetSampleCount)
{
    UWaveServer::Packet packet;
    packet.setNetwork(network);
    packet.setStation(station);
    packet.setChannel(channel);
    packet.setLocationCode(locationCode);
    packet.setStartTime(packetStartTime);
    packet.setSamplingRate(packetSamplingRate);
    if (packetDataType == 'i')
    {
        auto timeSeries
            = ::decompressAndUnpack<int> (packetSampleCount,
                                          packetByteArray,
                                          packetIsLittleEndian,
                                          amLittleEndian,
                                          packetIsCompressed);
        packet.setData(std::move(timeSeries));
    }
    else if (packetDataType == 'l') 
    {
        auto timeSeries
            = ::decompressAndUnpack<int64_t> (packetSampleCount,
                                              packetByteArray, 
                                              packetIsLittleEndian,
                                              amLittleEndian,
                                              packetIsCompressed);
        packet.setData(std::move(timeSeries));
    }
    else if (packetDataType == 'f')
    {
        auto timeSeries
            = ::decompressAndUnpack<float> (packetSampleCount,
                                            packetByteArray,
                                            packetIsLittleEndian,
                                            amLittleEndian,
                                            packetIsCompressed);
        packet.setData(std::move(timeSeries));
    }
    else if (packetDataType == 'd')
    {
        auto timeSeries
             = ::decompressAndUnpack<double> (packetSampleCount,
                                              packetByteArray,
                                              packetIsLittleEndian,
                                              amLittleEndian,
                                              packetIsCompressed);
        packet.setData(std::move(timeSeries));
    }
    else if (packetDataType == 't')
    {
        auto timeSeries
            = ::decompressAndUnpack<char> (packetSampleCount,
                                           packetByteArray,
                                           packetIsLittleEndian,
                                           amLittleEndian,
                                           packetIsCompressed);
        packet.setData(std::move(timeSeries));
    }
    else
    {
        throw std::runtime_error("Cannot unpack data of type "
                               + std::string {packetDataType});
    }
    return packet;
}

std::vector<UWaveServer::Packet> unpackPackets(
    const std::string &network,
    const std::string &station,
    const std::string &channel,
    const std::string &locationCode,
    const bool amLittleEndian, 
    const std::vector<double> &packetStartTime,
    const std::vector<double> &packetSamplingRate,
    const std::vector<char> &packetDataType,
    std::vector<std::basic_string<std::byte>> &packetByteArray,
    const std::vector<bool> packetIsLittleEndian,
    const std::vector<bool> packetIsCompressed,
    const std::vector<int> packetSampleCount)
{
#ifndef NDEBUG
    assert(packetStartTime.size() == packetSamplingRate.size());
    assert(packetStartTime.size() == packetDataType.size());
    assert(packetStartTime.size() == packetByteArray.size());
    assert(packetStartTime.size() == packetIsLittleEndian.size());
    assert(packetStartTime.size() == packetIsCompressed.size());
    assert(packetStartTime.size() == packetSampleCount.size());
#endif
    std::vector<UWaveServer::Packet> result;
    if (packetStartTime.empty()){return result;}
    result.reserve(packetStartTime.size());
    for (int i = 0; i < static_cast<int> (packetStartTime.size()); ++i)
    {
        try
        {
            auto thisPacket = ::unpackPacket(network,
                                             station,
                                             channel,
                                             locationCode,
                                             packetStartTime[i],
                                             packetSamplingRate[i],
                                             packetDataType[i],
                                             packetByteArray[i],
                                             packetIsLittleEndian[i],
                                             packetIsCompressed[i],
                                             amLittleEndian,
                                             packetSampleCount[i]);
            result.push_back(std::move(thisPacket));
        }
        catch (const std::exception &e)
        {
            spdlog::warn("Failed to unpack packet for " 
                       + ::toName(network, station, channel, locationCode) 
                       + "; failed with " + std::string {e.what()});
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
    explicit ClientImpl(const Credentials &credentials) :
        mCredentials(credentials)
    {   
        mAmReadOnly = mCredentials.isReadOnly();
        connect();
        initializeSensors();
    }
    [[nodiscard]] bool isConnected() const noexcept
    {
        std::scoped_lock lock(mDatabaseMutex);
        if (mConnection)
        {
            return mConnection->dbname() != nullptr;
        }
        return false;
    }
    void connect()
    {
        disconnect();
        {
        std::scoped_lock lock(mDatabaseMutex);
        mConnection
           = std::make_unique<pqxx::connection>
             (mCredentials.getConnectionString());
        if (mConnection)
        {
            if (mConnection->dbname() == nullptr)
            {
                throw std::runtime_error("Failed to connect to "
                                       + mCredentials.getDatabaseName()
                                       + " at " + mCredentials.getHost());
            }
            // Schema
// TODO do i want to do this?
            auto schema = mCredentials.getSchema();
            if (!schema.empty())
            {
                spdlog::debug("Updating search path to " + schema);
                std::string query = "SET search_path TO " + schema + ", public";
                pqxx::work transaction(*mConnection);
                transaction.exec(query);
                transaction.commit();
            }
        }
        else
        {
            throw std::runtime_error("Failed to connect to "
                                   + mCredentials.getDatabaseName()
                                   + " at " + mCredentials.getHost());
        }
        }
        spdlog::info("Connected to " + mCredentials.getDatabaseName()
                   + " at " + mCredentials.getHost());
    }
    void disconnect()
    {
        std::scoped_lock lock(mDatabaseMutex);
        if (mConnection)
        {
            mConnection->close();
            mConnection = nullptr;
        }
    }
    void reconnect()
    {
        std::vector<std::chrono::seconds> reconnectSchedule
        {
            std::chrono::seconds {0},
            std::chrono::seconds {15},
            std::chrono::seconds {60}
        };
        for (const auto &timeOut : reconnectSchedule)
        {
            try
            {
                connect();
                if (isConnected()){return;}
            }
            catch (const std::exception &e)
            {
                spdlog::warn("Connection attempt failed with "
                           + std::string {e.what()});
            }
            spdlog::debug("Will attempt to reconnect in "
                        + std::to_string(timeOut.count()) + " seconds");
            std::this_thread::sleep_for(timeOut);
        }
        throw std::runtime_error("Failed to connect to database");
    }
    [[nodiscard]] std::map<std::string, std::pair<int, std::string>> getSensors()
    {
        // Ensure we're connected
        if (isConnected())
        {
            spdlog::info("Attempting to reconnect prior to getting sensors...");
            reconnect(); // Throws
        }
        //auto session
        //    = reinterpret_cast<soci::session *> (mConnection.getSession());
        std::vector<std::pair<std::string, std::pair<int, std::string>>>
            sensorTableMap;
        constexpr pqxx::zview query
{
"SELECT identifier, network, station, channel, location_code, data_table_name FROM sensors"
};
        // Streaming for this little data is unnecessary and dangerous
        {
        std::scoped_lock lock(mDatabaseMutex);
        pqxx::work transaction(*mConnection);
        pqxx::result queryResult = transaction.exec(query);
        for (const auto &row : queryResult) //int i = 0; i < queryResult.size(); ++i)
        {
            try
            {
                //const auto row = queryResult.at(i);
                auto identifier = row[0].as<int> ();
                auto network = row[1].as<std::string_view> ();
                auto station = row[2].as<std::string_view> ();
                auto channel = row[3].as<std::string_view> ();
                auto locationCode = row[4].as<std::string_view> ();
                auto tableName = row[5].as<std::string_view> ();
                auto name = ::toName(network, station, channel, locationCode);
                std::pair<int, std::string>
                    identifierTablePair{identifier, std::string{tableName}};
                std::pair<std::string, std::pair<int, std::string>>
                    sensorIdentifierTablePair{std::move(name),
                                              std::move(identifierTablePair)};
                sensorTableMap.push_back(std::move(sensorIdentifierTablePair)); 
            }
            catch (const std::exception &e)
            {
                spdlog::warn(e.what());
            }
        }
        transaction.commit();
        }
        std::map<std::string, std::pair<int, std::string>> result;
        for (auto &sensorTablePair : sensorTableMap)
        {
            if (!result.contains(sensorTablePair.first))
            {
                result.insert(std::move(sensorTablePair));
            }
        }
        return result;
    }
    // Search for a sensor and, potentially, if it doesn't exist add it to my
    // cache
    [[nodiscard]] std::map<std::string, std::vector<int>>
        getSensorIdentifiersAndTableName(const std::string &network,
                                         const std::string &station)
    {
        std::vector<std::pair<int, std::string>> sensorTablePairs;
        // Okay - let's look in the database for it
        constexpr pqxx::zview query
{
"SELECT identifier, data_table_name, channel, location_code FROM sensors WHERE network = $1 AND station = $2"
};
        pqxx::params queryParameters{network, station};
        int identifier{-1};
        std::string tableName;
        {
        std::scoped_lock databaseLock(mDatabaseMutex);
        pqxx::work transaction(*mConnection);
        pqxx::result queryResult = transaction.exec(query, queryParameters);
        for (const auto &row : queryResult) //int i = 0; i < queryResult.size(); ++i)
        {
            try
            {
                auto identifier = row[0].as<int> ();
                auto thisTableName = row[1].as<std::string> ();
                if (tableName.empty()){tableName = thisTableName;}
                if (tableName != thisTableName)
                {
                    spdlog::warn("Channel mapped to inconsistent table");
                }
                auto channel = row[2].as<std::string> ();
                auto locationCode = row[3].as<std::string> ();
                auto name = ::toName(network, station, channel, locationCode);
                auto newEntry
                    = std::pair{ identifier, std::move(thisTableName) };
                sensorTablePairs.push_back(std::move(newEntry));
            }
            catch (const std::exception &e)
            {
                spdlog::warn("Failed to unpack row");
            }
        }
        transaction.commit();
        }
        // Now we need to invert the control so that the keys are the
        // table names and then we have a list of identifiers
        std::map<std::string, std::vector<int>> result;
        for (auto &pair : sensorTablePairs)
        {
            if (result.empty())
            {
                std::vector<int> identifiers{pair.first}; 
                result.insert(std::pair {pair.second, std::move(identifiers)});
            }
            else
            {
                auto idx = result.find(pair.second);
                if (idx != result.end())
                {
                    idx->second.push_back(pair.first);
                }
                else
                {
                    std::vector<int> identifiers{pair.first};
                    result.insert(
                       std::pair {pair.second, std::move(identifiers)});
                }
            }
        }
        for (auto &kv : result)
        {
            std::sort(kv.second.begin(), kv.second.end());
        }
        return result;
    }
    [[nodiscard]] std::pair<int, std::string>
        getSensorIdentifierAndTableName(const std::string &network,
                                        const std::string &station,
                                        const std::string &channel,
                                        const std::string &locationCode,
                                        const bool addIfNotExists) const
    {
        auto name = ::toName(network, station, channel, locationCode);
        // Maybe we already have this channel 
        {
        std::scoped_lock lock(mMutex);
        auto index = mSensorToIdentifierAndTableName.find(name);
        if (index != mSensorToIdentifierAndTableName.end())
        {
            return index->second;
        }
        }
        // Okay - let's look in the database for it
        constexpr pqxx::zview query
{
"SELECT identifier, data_table_name FROM sensors WHERE network = $1 AND station = $2 AND channel = $3 AND location_code = $4"
};
        pqxx::params queryParameters{network, station, channel, locationCode};
        int identifier{-1};
        std::string tableName;
        {
        std::scoped_lock databaseLock(mDatabaseMutex);
        pqxx::work transaction(*mConnection);
        pqxx::result queryResult = transaction.exec(query, queryParameters);
        if (!queryResult.empty())
        {
            const auto row = queryResult[0];
            identifier = row[0].as<int> ();
            tableName = row[1].as<std::string> ();
            if (queryResult.size() > 1)
            {
                spdlog::warn("Multiple hit for " + name  
                           + " in sensors table - returning first");
            }
        }
        transaction.commit();
        // Deal with non-existent identifier.  Note, still have this locked
        // so another writer doesn't swing by and ruin my day. 
        if (identifier ==-1)
        {
            if (!addIfNotExists)
            {
                spdlog::debug("Sensor " + name + " does not exist");
                return std::pair {identifier, tableName}; 
            }
            // Add it the sensor
            std::string sensorTableName = "sensors";
            if (!mCredentials.getSchema().empty())
            {
                sensorTableName = mCredentials.getSchema() + ".sensors";
            }
            auto dataTableName = ::toTableName(mCredentials, network, station);
            const std::string insertSensorStatement =
            "INSERT INTO " + sensorTableName 
          + "(network, station, channel, location_code, data_table_name) "
          + "VALUES ($1, $2, $3, $4, $5) "
          //+ "ON CONFLICT DO NOTHING "
          + "RETURNING identifier, data_table_name";
            pqxx::params insertParameters{network, station,
                                          channel, locationCode,
                                          dataTableName};
            constexpr std::string_view createTableSuffix{
R"""(
(
    sensor_identifier INTEGER NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL CHECK(end_time >= start_time),
    load_time TIMESTAMPTZ DEFAULT NOW(),
    sampling_rate DOUBLE PRECISION NOT NULL CHECK(sampling_rate > 0),
    number_of_samples INT NOT NULL CHECK(number_of_samples >= 0),
    little_endian BOOLEAN NOT NULL,
    compressed BOOLEAN NOT NULL,
    data_type CHARACTER (1) NOT NULL CHECK(data_type IN ('i', 'f', 'd', 'l', 't')),
    data BYTEA NOT NULL,
    PRIMARY KEY (sensor_identifier, start_time),
    FOREIGN KEY (sensor_identifier) REFERENCES ynp.sensors (identifier)
)
)"""};
            const std::string createHyperTable = "SELECT create_hypertable('"
                                               + dataTableName
                                               + "', by_range('start_time', INTERVAL '"
                                               + std::to_string(2)
                                               + "hours'), if_not_exists => TRUE);";
            const std::string createCompression = "ALTER TABLE "
                                                + dataTableName
                                                + " SET(timescaledb.enable_columnstore, timescaledb.segmentby = 'sensor_identifier', timescaledb.orderby = 'start_time');";
            const std::string createChunkSkipping = "SELECT enable_chunk_skipping('"
                                                  + dataTableName
                                                  + "', 'sensor_identifier')";
            const std::string createRetention = "SELECT add_retention_policy('"
                                              + dataTableName
                                              + "', INTERVAL '"
                                              + std::to_string(5)
                                              + " days')";


 
            std::string createTable = "CREATE TABLE IF NOT EXISTS " + dataTableName
                                    + std::string {createTableSuffix}; 
            //std::cout << createTable << std::endl;
            //{
            //std::scoped_lock databaseLock(mDatabaseMutex);
            pqxx::work transaction(*mConnection);
            pqxx::result insertResult
                = transaction.exec(insertSensorStatement, insertParameters);
            if (!insertResult.empty())
            {
                const auto &insertRow = insertResult[0];
                identifier = insertRow[0].as<int> ();
                tableName = insertRow[1].as<std::string> ();
                if (insertResult.size() > 1)
                {
                    spdlog::warn("Multiple inserts for " + name  
                              + " in sensors table - returning first");
                }
            }
            else
            {
                spdlog::warn("Insert into sensors failed for " + name);
            }
            transaction.exec(createTable); 
            transaction.exec(createHyperTable);
            transaction.exec(createCompression);
            transaction.exec("SET timescaledb.enable_chunk_skipping TO ON;");
            transaction.exec(createChunkSkipping);
            transaction.exec(createRetention);
            transaction.commit();
            //}
        }
        } // End lock
        if (identifier >= 0)
        {
            std::pair<int, std::string> itemToInsert{identifier, tableName};
            {
            std::scoped_lock lock(mMutex);
            mSensorToIdentifierAndTableName.insert_or_assign(
               name, std::move(itemToInsert));
            }
        }
        return std::pair {identifier, tableName};
    }
    [[nodiscard]]
    std::pair<int, std::string>
        getSensorIdentifierAndTableName(const Packet &packet,
                                        const bool addIfNotExists) const
    {
        const auto network = packet.getNetworkReference();
        const auto station = packet.getStationReference();
        const auto channel = packet.getChannelReference();
        const auto locationCode = packet.getLocationCodeReference();
        return getSensorIdentifierAndTableName(network, station,
                                               channel, locationCode,
                                               addIfNotExists);
    }
    // Initialize my cache of sensors
    void initializeSensors()
    {
        auto sensors = getSensors();
        if (!sensors.empty())
        {
            std::scoped_lock lock(mMutex);
            mSensorToIdentifierAndTableName.clear();
            for (auto &sensor : sensors)
            {
                mSensorToIdentifierAndTableName.insert_or_assign(
                    sensor.first, std::move(sensor.second));
            }
            spdlog::debug(std::to_string(mSensorToIdentifierAndTableName.size())
                        + " sensors in map");
        }
    }
    bool contains(const std::string &network,
                  const std::string &station,
                  const std::string &channel,
                  const std::string &locationCode)
    {
        // Ensure we're connected
        if (!isConnected())
        {
            spdlog::debug(
                "Attempting to reconnect prior to checking if sensor exists..");
            reconnect(); // Throws
        }   
        // Check the sensor is there 
        constexpr bool addIfNotExists{false};
        auto sensorIdentifierAndTableName
		    = getSensorIdentifierAndTableName(network, station,
						      channel, locationCode,
						      addIfNotExists); // Throws
		if (sensorIdentifierAndTableName.first < 0)
		{
		    return false;
		}
		return true;
	    }
	    // Get the the packets for all channels for this station/network
	    [[nodiscard]]
	    std::map<std::string, std::vector<Packet>>
		query(const std::string &network,
		      const std::string &station,
		      const double startTime,
		      const double endTime)
	    {
		std::map<std::string, std::vector<Packet>> result;
		// Check the sensor is there
		constexpr bool addIfNotExists{false};
		auto tableNameAndSensorIdentifiers
		    = getSensorIdentifiersAndTableName(network, station);
		if (tableNameAndSensorIdentifiers.empty())
		{
		    throw std::invalid_argument(
			"Could not determine table holding sensor data for "
		       + network + "." + station);
		}
		// Time to work
	#ifndef NDEBUG  
		auto queryStartTime = std::chrono::high_resolution_clock::now();
	#endif     
		// Assemble query
		constexpr std::string_view queryPrefix{
	"SELECT sensor_identifier, EXTRACT(epoch FROM start_time), sampling_rate, number_of_samples, little_endian, compressed, data_type, data::bytea FROM "
		};
		constexpr std::string_view querySensorSpecificSuffix{
	" WHERE end_time > TO_TIMESTAMP($1) AND start_time < TO_TIMESTAMP($2) "
		};
		pqxx::params parameters{startTime,
					endTime};
		for (const auto &item : tableNameAndSensorIdentifiers)
		{
		    int nIdentifiers = static_cast<int> (item.second.size());
		    if (nIdentifiers < 1){continue;}
		    std::string querySuffix = " AND (sensor_identifier = ";
		    for (int i = 0; i < nIdentifiers; ++i)
		    {
			querySuffix = querySuffix + " sensor_identifier = "
				    + std::to_string(item.second.at(i));
			if (i < nIdentifiers - 1)
			{
			    querySuffix = querySuffix + " OR ";
			}
		    }
		    querySuffix = querySuffix + ")";
		}
	/*
		pqxx::params parameters{sensorIdentifier,
					startTime,
					endTime};
		std::string query = std::string {queryPrefix}
				  + tableName
				  + std::string {querySensorSpecificSuffix};
	*/

		return result;    
	    }
	    // Get the for this SCNL packets from the database
	    [[nodiscard]]
	    std::vector<Packet> query(const std::string &network,
				      const std::string &station,
				      const std::string &channel,
				      const std::string &locationCode,
				      const double startTime,
				      const double endTime)
	    {
		std::vector<Packet> result;
		// Ensure we're connected
		if (!isConnected())
		{
		    spdlog::info("Attempting to reconnect prior to query...");
		    reconnect(); // Throws
		}
		// Check the sensor is there
		constexpr bool addIfNotExists{false};
		auto [sensorIdentifier, tableName]
		    = getSensorIdentifierAndTableName(network, station,
						      channel, locationCode,
						      addIfNotExists); // Throws
		if (sensorIdentifier < 0)
		{
		    throw std::invalid_argument(
			"Could not obtain sensor identifier in query for "
		       + ::toName(network, station, channel, locationCode));
		}
		// Time to work
	#ifndef NDEBUG  
		auto queryStartTime = std::chrono::high_resolution_clock::now();
	#endif              
		// Assemble query
		constexpr std::string_view queryPrefix{
	"SELECT EXTRACT(epoch FROM start_time), sampling_rate, number_of_samples, little_endian, compressed, data_type, data::bytea FROM "
		};
		constexpr std::string_view querySensorSpecificSuffix{
	" WHERE sensor_identifier = $1 AND end_time > TO_TIMESTAMP($2) AND start_time < TO_TIMESTAMP($3)" 
		}; 
		pqxx::params parameters{sensorIdentifier,
					startTime,
					endTime};
		std::string query = std::string {queryPrefix}
				  + tableName
				  + std::string {querySensorSpecificSuffix};

		std::vector<double> packetStartTime;
		std::vector<double> packetSamplingRate;
		std::vector<int> packetSampleCount;
		std::vector<bool> packetIsLittleEndian;
		std::vector<bool> packetIsCompressed;
		std::vector<char> packetDataType;
		std::vector<std::basic_string<std::byte>> packetByteArray;
		// TODO: Should switch to a stream despite it being dangerous.  Just
		//       have to figure out how prepped statements work.
		{
		std::scoped_lock lock(mDatabaseMutex);
		pqxx::work transaction(*mConnection);
		pqxx::result queryResult = transaction.exec(query, parameters);
		auto queryResultSize = queryResult.size();
		packetStartTime.reserve(queryResultSize);
		packetSamplingRate.reserve(queryResultSize);
		packetSampleCount.reserve(queryResultSize);
		packetIsCompressed.reserve(queryResultSize);
		packetIsLittleEndian.reserve(queryResultSize);
		packetDataType.reserve(queryResultSize);
		packetByteArray.reserve(queryResultSize);
		for (int i = 0; i < static_cast<int> (queryResult.size()); ++i)
		{
		    const auto &row = queryResult[i];
		    packetStartTime.push_back(row[0].as<double> ());
		    packetSamplingRate.push_back(row[1].as<double> ());
		    packetSampleCount.push_back(row[2].as<int> ());
		    packetIsLittleEndian.push_back(row[3].as<bool> ());
		    packetIsCompressed.push_back(row[4].as<bool> ());
		    packetDataType.push_back(row[5].as<std::string_view> () [0]);
		    packetByteArray.push_back(
			row[6].as<std::basic_string<std::byte>> ()); 
		}
		transaction.commit();
		}
	#ifndef NDEBUG
		auto queryEndTime = std::chrono::high_resolution_clock::now();
		double queryDuration
		    = std::chrono::duration_cast<std::chrono::microseconds>
		      (queryEndTime - queryStartTime).count()*1.e-6;
		spdlog::info("Query duration was "
			   + std::to_string(queryDuration) + " (s)");
		auto unpackTimeTime = queryEndTime;
	#endif
		result = ::unpackPackets(network,
					 station,
					 channel,
					 locationCode,
					 mAmLittleEndian, 
					 packetStartTime,
					 packetSamplingRate,
					 packetDataType,
					 packetByteArray,
					 packetIsLittleEndian,
					 packetIsCompressed,
					 packetSampleCount);
	#ifndef NDEBUG
		auto unpackEndTime = std::chrono::high_resolution_clock::now();
		double unpackDuration
		    = std::chrono::duration_cast<std::chrono::microseconds>
		      (unpackEndTime - queryStartTime).count()*1.e-6;
		spdlog::info("Unpack duration was "
			    + std::to_string(unpackDuration) + " (s)");
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
		if (mAmReadOnly)
		{
		    spdlog::warn("Read-only session cannot insert data - returning");
		    return;
		}
		// Ensure we're connected
		if (!isConnected())
		{
		    spdlog::info("Attempting to reconnect prior to insert...");
		    reconnect(); // Will throw
		}
		// Get the sensor identifier
		constexpr bool addIfNotExists{true};
		auto [sensorIdentifier, tableName]
		    = getSensorIdentifierAndTableName(packet, addIfNotExists); // Throws
		if (sensorIdentifier < 0 || tableName.empty()) 
		{
		    throw std::runtime_error(
		       "Could not obtain sensor identifier in insert");
		}

		auto nSamples = static_cast<int> (packet.size()); 
		double startTime = packet.getStartTime().count()*1.e-6;
		double endTime = packet.getEndTime().count()*1.e-6;
		double samplingRate = packet.getSamplingRate();
		auto dataType = packet.getDataType();

		auto compressed = (mCompressionLevel != Z_NO_COMPRESSION) ? true : false;
		std::string binaryData;
		std::string dataTypeSignifier{'i'};
		if (dataType == UWaveServer::Packet::DataType::Integer32)
		{
		    auto dataPtr = static_cast<const int *> (packet.data());
		    binaryData 
			 = ::packAndCompress<int>
			   (nSamples, dataPtr, mCompressionLevel, mSwapBytes);
		    dataTypeSignifier = "i";
		}
		else if (dataType == UWaveServer::Packet::DataType::Integer64)
		{
		    auto dataPtr = static_cast<const int64_t *> (packet.data());
		    binaryData
			= ::packAndCompress<int64_t>
			  (nSamples, dataPtr, mCompressionLevel, mSwapBytes);
		    dataTypeSignifier = "l";
		}
		else if (dataType == UWaveServer::Packet::DataType::Double)
		{
		    auto dataPtr = static_cast<const double *> (packet.data());
		    binaryData
			= ::packAndCompress<double>
			  (nSamples, dataPtr, mCompressionLevel, mSwapBytes);
		    dataTypeSignifier = "d";
		}
		else if (dataType == UWaveServer::Packet::DataType::Float)
		{
		    auto dataPtr = static_cast<const float *> (packet.data());
		    binaryData
			= ::packAndCompress<float>
			  (nSamples, dataPtr, mCompressionLevel, mSwapBytes);
		    dataTypeSignifier = "f";
		}
		else if (dataType == UWaveServer::Packet::DataType::Text)
		{
		    auto dataPtr = static_cast<const char *> (packet.data());
		    binaryData
			= ::packAndCompress<char>
			  (nSamples, dataPtr, mCompressionLevel, mSwapBytes);
		    dataTypeSignifier = "t";
		}
		else
		{
	#ifndef NDEBUG
		    assert(false);
	#endif
		}
		constexpr bool littleEndian{true}; // Always write as little endian
		//auto castedBinaryData = pqxx::binary_cast(binaryData.data(), hexEncodedData.size());
		//std::cout << "send it again it" << castedBinaryData.size() << std::endl;
		constexpr std::string_view queryPrefix{"INSERT INTO "};
		constexpr std::string_view querySuffix{
	"(sensor_identifier, start_time, end_time, sampling_rate, number_of_samples, little_endian, compressed, data_type, data) VALUES($1, TO_TIMESTAMP($2), TO_TIMESTAMP($3), $4, $5, $6, $7, $8, $9) ON CONFLICT DO NOTHING"};
		std::string insertStatement
		    = std::string {queryPrefix}
		    + tableName
		    + std::string {querySuffix};

		pqxx::params parameters{
		    sensorIdentifier,
		    startTime,
		    endTime,
		    //load time - default is now
		    samplingRate,
		    nSamples,
		    littleEndian,
		    compressed,
		    std::string {dataTypeSignifier},
		    pqxx::binary_cast(binaryData.data(), binaryData.size())};
		{
		pqxx::work transaction(*mConnection);
		transaction.exec(insertStatement, parameters); 
		transaction.commit();
		}
	    }
	    void getRetentionDuration()
	    {
	/*
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
	*/
	    }
	    mutable std::mutex mDatabaseMutex;
	    mutable std::mutex mMutex;
	    mutable std::map<std::string, std::pair<int, std::string>>
		mSensorToIdentifierAndTableName;
	    mutable std::unique_ptr<pqxx::connection> mConnection{nullptr};
	    Credentials mCredentials;
	    std::chrono::seconds mRetentionDuration{365*86400}; // Make it something large like a year
	//    std::chrono::days mRetentionDuration{5};
	#ifdef WITH_ZLIB
	    int mCompressionLevel{Z_BEST_COMPRESSION};
	    bool mWriteCompressedData{true};
	#else
	    int mCompressionLevel{Z_NO_COMPRESSION};
	    bool mWriteCompressedData{false};
	#endif
	    bool mAmReadOnly{true};
	    bool mSwapBytes{std::endian::native == std::endian::little ? false : true};
	    bool mAmLittleEndian{std::endian::native == std::endian::little ? true : false};
	};

	/// Constructor
	Client::Client(const Credentials &credentials) :
	    pImpl(std::make_unique<ClientImpl> (credentials))
	{
	}

	/// Destructor
	Client::~Client() = default;

	/*
	/// Connected?
	bool Client::isConnected() const noexcept
	{
	    return pImpl->mConnection.isConnected();
	}
	*/

	/// Connect
	void Client::connect()
	{
	    //pImpl->mConnection.connect();
	}

	/// Disconnect
	void Client::disconnect()
	{
	    //pImpl->mConnection.disconnect();
	}

	/// Write the data packet
	void Client::write(const UWaveServer::Packet &packet)
	{
	    if (pImpl->mAmReadOnly)
	    {
		throw std::runtime_error(
		   "Cannot write to database with read-only credentials");
	    }
	    if (!packet.hasNetwork())
	    {
		throw std::invalid_argument("Network not set on packet");
	    }
	    if (!packet.hasStation())
	    {
		throw std::invalid_argument("Station not set on packet");
	    }
	    if (!packet.hasChannel())
	    {
		throw std::invalid_argument("Channel not set on packet");
	    }
	    if (!packet.hasSamplingRate())
	    {
		throw std::invalid_argument("Sampling rate not set on packet");
	    }
	    if (packet.empty())
	    {
		spdlog::warn("Packet has no data - returning");
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

	bool Client::contains(const std::string &networkIn,
			      const std::string &stationIn,
			      const std::string &channelIn,
			      const std::string &locationCodeIn) const
	{
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
    return pImpl->contains(network, station, channel, locationCode);
}

std::vector<UWaveServer::Packet> Client::query(
    const std::string &network,
    const std::string &station,
    const std::string &channel,
    const std::string &locationCode,
    const std::chrono::microseconds &t0MuS,
    const std::chrono::microseconds &t1MuS) const
{
    const double t0{t0MuS.count()*1.e-6};
    const double t1{t0MuS.count()*1.e-6};
    return query(network, station, channel, locationCode, t0, t1);
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
    return pImpl->query(network, station, channel, locationCode,
                        startTime, endTime); 
}

std::set<std::string> Client::getSensors() const
{
    auto sensorToTableMap = pImpl->getSensors();
    std::set<std::string> sensors;
    for (const auto &item : sensorToTableMap)
    {
        sensors.insert(item.first);
    }
    return sensors;
}
