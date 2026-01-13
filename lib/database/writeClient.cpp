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
#include <pqxx/pqxx>
#include "uWaveServer/database/writeClient.hpp"
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

class WriteClient::WriteClientImpl
{
public:
    explicit WriteClientImpl(const Credentials &credentials) :
        mCredentials(credentials)
    {   
        if (mCredentials.isReadOnly())
        {
            spdlog::warn("Database client will open in read-write mode");
        }
        connect();
        initializeStreams();
    }
    [[nodiscard]] bool isConnected() const noexcept
    {
        std::scoped_lock lock(mDatabaseMutex);
        if (mConnection)
        {
            return mConnection->is_open();
            //return mConnection->dbname() != nullptr;
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
                std::string query = "SET search_path TO " + schema;// + ", public";
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
    [[nodiscard]] std::map<std::string, std::pair<int, std::string>> getStreams()
    {
        // Ensure we're connected
        if (isConnected())
        {
            spdlog::info("Attempting to reconnect prior to getting streams...");
            reconnect(); // Throws
        }
        //auto session
        //    = reinterpret_cast<soci::session *> (mConnection.getSession());
        std::vector<std::pair<std::string, std::pair<int, std::string>>>
            streamTableMap;
        constexpr pqxx::zview query
{
"SELECT identifier, network, station, channel, location_code, data_table_name FROM streams"
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
                    streamIdentifierTablePair{std::move(name),
                                              std::move(identifierTablePair)};
                streamTableMap.push_back(std::move(streamIdentifierTablePair)); 
            }
            catch (const std::exception &e)
            {
                spdlog::warn(e.what());
            }
        }
        transaction.commit();
        }
        std::map<std::string, std::pair<int, std::string>> result;
        for (auto &streamTablePair : streamTableMap)
        {
            if (!result.contains(streamTablePair.first))
            {
                result.insert(std::move(streamTablePair));
            }
        }
        return result;
    }
    // Search for a stream and, potentially, if it doesn't exist add it to my
    // cache
    [[nodiscard]] std::map<std::string, std::vector<int>>
        getStreamIdentifiersAndTableName(const std::string &network,
                                         const std::string &station)
    {
        std::vector<std::pair<int, std::string>> streamTablePairs;
        // Okay - let's look in the database for it
        constexpr pqxx::zview query
{
"SELECT identifier, data_table_name, channel, location_code FROM streams WHERE network = $1 AND station = $2"
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
                streamTablePairs.push_back(std::move(newEntry));
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
        for (auto &pair : streamTablePairs)
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
        getStreamIdentifierAndTableName(const std::string &network,
                                        const std::string &station,
                                        const std::string &channel,
                                        const std::string &locationCode,
                                        const bool addIfNotExists) const
    {
        auto name = ::toName(network, station, channel, locationCode);
        // Maybe we already have this channel 
        {
        std::scoped_lock lock(mMutex);
        auto index = mStreamToIdentifierAndTableName.find(name);
        if (index != mStreamToIdentifierAndTableName.end())
        {
            return index->second;
        }
        }
        // Okay - let's look in the database for it
        constexpr pqxx::zview query
{
"SELECT identifier, data_table_name FROM streams WHERE network = $1 AND station = $2 AND channel = $3 AND location_code = $4"
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
                           + " in streams table - returning first");
            }
        }
        transaction.commit();
        // Deal with non-existent identifier.  Note, still have this locked
        // so another writer doesn't swing by and ruin my day. 
        if (identifier ==-1)
        {
            if (!addIfNotExists)
            {
                spdlog::debug("Stream " + name + " does not exist");
                return std::pair {identifier, tableName}; 
            }
            // Add it the stream
            auto schema = mCredentials.getSchema();
            if (!schema.empty())
            {
                std::string createCall
                    = "CALL public.create_stream_data_table_with_defaults_in_schema('"
                    + schema + "','"
                    + network + "','"
                    + station + "','"
                    + channel + "','"
                    + locationCode + "');";
                pqxx::work transaction(*mConnection);
                pqxx::result insertResult
                    = transaction.exec(createCall);
                transaction.commit();
            }
            else
            {
                std::string createCall
                    = "CALL public.create_stream_data_table_with_defaults('"
                    + network + "','"
                    + station + "','"
                    + channel + "','"
                    + locationCode + "')";
                pqxx::work transaction(*mConnection);
                pqxx::result insertResult
                    = transaction.exec(createCall);
                transaction.commit();
            }
        }
        } // End lock
        if (identifier < 0)
        {
            auto output = getStreamIdentifierAndTableName(
                network, station, channel, locationCode, false);
            if (output.first < 0)
            {
                throw std::runtime_error("Still can't get stream/table");
            }
            return output;
        }
        return std::pair {identifier, tableName};

/*
            std::string streamTableName = "streams";
            if (!mCredentials.getSchema().empty())
            {
                streamTableName = mCredentials.getSchema() + ".streams";
            }
            auto dataTableName = ::toTableName(mCredentials, network, station);
            const std::string insertStreamStatement =
            "INSERT INTO " + streamTableName 
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
    stream_identifier INTEGER NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL CHECK(end_time >= start_time),
    load_time TIMESTAMPTZ DEFAULT NOW(),
    sampling_rate DOUBLE PRECISION NOT NULL CHECK(sampling_rate > 0),
    number_of_samples INT NOT NULL CHECK(number_of_samples >= 0),
    little_endian BOOLEAN NOT NULL,
    compressed BOOLEAN NOT NULL,
    data_type CHARACTER (1) NOT NULL CHECK(data_type IN ('i', 'f', 'd', 'l', 't')),
    data BYTEA NOT NULL,
    PRIMARY KEY (stream_identifier, start_time),
    FOREIGN KEY (stream_identifier) REFERENCES ynp.streams (identifier)
)
)"""};
            const std::string createHyperTable = "SELECT create_hypertable('"
                                               + dataTableName
                                               + "', by_range('start_time', INTERVAL '"
                                               + std::to_string(2)
                                               + "hours'), if_not_exists => TRUE);";
            const std::string createCompression = "ALTER TABLE "
                                                + dataTableName
                                                + " SET(timescaledb.enable_columnstore, timescaledb.segmentby = 'stream_identifier', timescaledb.orderby = 'start_time');";
            const std::string createChunkSkipping = "SELECT enable_chunk_skipping('"
                                                  + dataTableName
                                                  + "', 'stream_identifier', if_not_exists => TRUE)";
            const std::string createRetention = "SELECT add_retention_policy('"
                                              + dataTableName
                                              + "', INTERVAL '"
                                              + std::to_string(5)
                                              + " days', if_not_exists => TRUE)";


 
            std::string createTable = "CREATE TABLE IF NOT EXISTS " + dataTableName
                                    + std::string {createTableSuffix}; 
            //std::cout << createTable << std::endl;
            //{
            //std::scoped_lock databaseLock(mDatabaseMutex);
            pqxx::work transaction(*mConnection);
            pqxx::result insertResult
                = transaction.exec(insertStreamStatement, insertParameters);
            if (!insertResult.empty())
            {
                const auto &insertRow = insertResult[0];
                identifier = insertRow[0].as<int> ();
                tableName = insertRow[1].as<std::string> ();
                if (insertResult.size() > 1)
                {
                    spdlog::warn("Multiple inserts for " + name  
                              + " in streams table - returning first");
                }
            }
            else
            {
                spdlog::warn("Insert into streams failed for " + name);
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
            mStreamToIdentifierAndTableName.insert_or_assign(
               name, std::move(itemToInsert));
            }
        }
        return std::pair {identifier, tableName};
*/
    }
    [[nodiscard]]
    std::pair<int, std::string>
        getStreamIdentifierAndTableName(const Packet &packet,
                                        const bool addIfNotExists) const
    {
        const auto network = packet.getNetworkReference();
        const auto station = packet.getStationReference();
        const auto channel = packet.getChannelReference();
        const auto locationCode = packet.getLocationCodeReference();
        return getStreamIdentifierAndTableName(network, station,
                                               channel, locationCode,
                                               addIfNotExists);
    }
    // Initialize my cache of streams
    void initializeStreams()
    {
        auto streams = getStreams();
        if (!streams.empty())
        {
            std::scoped_lock lock(mMutex);
            mStreamToIdentifierAndTableName.clear();
            for (auto &stream : streams)
            {
                mStreamToIdentifierAndTableName.insert_or_assign(
                    stream.first, std::move(stream.second));
            }
            spdlog::debug(std::to_string(mStreamToIdentifierAndTableName.size())
                        + " streams in map");
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
                "Attempting to reconnect prior to checking if stream exists..");
            reconnect(); // Throws
        }   
        // Check the stream is there 
        constexpr bool addIfNotExists{false};
        auto streamIdentifierAndTableName
            = getStreamIdentifierAndTableName(network, station,
                                              channel, locationCode,
                                              addIfNotExists); // Throws
        if (streamIdentifierAndTableName.first < 0)
        {
            return false;
        }
        return true;
    }
    // Write the packet 
    void insert(const Packet &packet)
    {
        if (packet.empty())
        {
            spdlog::warn("Packet has no data - returning");
            return;
        }
        // Ensure we're connected
        if (!isConnected())
        {
            spdlog::info("Attempting to reconnect prior to insert...");
            reconnect(); // Will throw
        }
        // Get the stream identifier
        constexpr bool addIfNotExists{true};
        auto [streamIdentifier, tableName]
             = getStreamIdentifierAndTableName(packet, addIfNotExists); // Throws
        if (streamIdentifier < 0 || tableName.empty()) 
        {
            throw std::runtime_error(
               "Could not obtain stream identifier in insert");
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
        "(stream_identifier, start_time, end_time, sampling_rate, number_of_samples, little_endian, compressed, data_type, data) VALUES($1, TO_TIMESTAMP($2), TO_TIMESTAMP($3), $4, $5, $6, $7, $8, $9) ON CONFLICT DO NOTHING"};
        std::string insertStatement = std::string {queryPrefix}
                                    + tableName
                                    + std::string {querySuffix};

        pqxx::params parameters{
            streamIdentifier,
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
    }
    mutable std::mutex mDatabaseMutex;
    mutable std::mutex mMutex;
    mutable std::map<std::string, std::pair<int, std::string>>
        mStreamToIdentifierAndTableName;
    mutable std::unique_ptr<pqxx::connection> mConnection{nullptr};
    Credentials mCredentials;
    std::chrono::seconds mRetentionDuration{365*86400}; // TODO look this up from settings
    //    std::chrono::days mRetentionDuration{5}; // TODO
#ifdef WITH_ZLIB
    int mCompressionLevel{Z_BEST_COMPRESSION};
    bool mWriteCompressedData{true};
#else
    int mCompressionLevel{Z_NO_COMPRESSION};
    bool mWriteCompressedData{false};
#endif
    bool mSwapBytes{std::endian::native == std::endian::little ? false : true};
    bool mAmLittleEndian{std::endian::native == std::endian::little ? true : false};
};

/// Constructor
WriteClient::WriteClient(const Credentials &credentials) :
    pImpl(std::make_unique<WriteClientImpl> (credentials))
{
}

/// Destructor
WriteClient::~WriteClient() = default;

/*
/// Connected?
bool WriteClient::isConnected() const noexcept
{
     return pImpl->mConnection.isConnected();
}
*/

/*
/// Connect
void WriteClient::connect()
{
     //pImpl->mConnection.connect();
}
*/

/*
/// Disconnect
void WriteClient::disconnect()
{
            //pImpl->mConnection.disconnect();
}
*/

/// Write the data packet
void WriteClient::write(const UWaveServer::Packet &packet)
{
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

bool WriteClient::contains(const std::string &networkIn,
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
/*
std::set<std::string> WriteClient::getStreams() const
{
    auto streamToTableMap = pImpl->getStreams();
    std::set<std::string> streams;
    for (const auto &item : streamToTableMap)
    {
        streams.insert(item.first);
    }
    return streams;
}
*/
