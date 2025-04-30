#include <iostream>
#include <iomanip>
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
#include <soci/soci.h>
#include "uWaveServer/database/client.hpp"
#include "uWaveServer/packet.hpp"
#include "uWaveServer/database/connection/postgresql.hpp"

using namespace UWaveServer::Database;

namespace
{

struct QueryRow
{   
    QueryRow() = default;
    QueryRow(const double samplingRateIn,
             const double timeIn,
             const int64_t packetNumberIn,
             const int valueIn) :
        samplingRate(samplingRateIn),
        time(timeIn),
        packetNumber(packetNumberIn),
        value64f(0),
        value64i(0),
        value32i(valueIn),
        value32f(0),
        haveValue64f(false),
        haveValue64i(false),
        haveValue32i(true),
        haveValue32f(false)
    {
    }
    QueryRow(const double samplingRateIn,
             const double timeIn,
             const int64_t packetNumberIn,
             const double valueIn) :
        samplingRate(samplingRateIn),
        time(timeIn),
        packetNumber(packetNumberIn),
        value64f(valueIn),
        value64i(0),
        value32i(0),
        value32f(0),
        haveValue64f(true),
        haveValue64i(false),
        haveValue32i(false),
        haveValue32f(false)
    {
    }
    QueryRow(const double samplingRateIn,
             const double timeIn,
             const int64_t packetNumberIn,
             const int64_t valueIn) :
        samplingRate(samplingRateIn),
        time(timeIn),
        packetNumber(packetNumberIn),
        value64f(0),
        value64i(valueIn),
        value32i(0),
        value32f(0),
        haveValue64f(false),
        haveValue64i(true),
        haveValue32i(false),
        haveValue32f(false)
    {
    }
    QueryRow(const double samplingRateIn,
             const double timeIn,
             const float packetNumberIn,
             const int valueIn) :
        samplingRate(samplingRateIn),
        time(timeIn),
        packetNumber(packetNumberIn),
        value64f(0),
        value64i(0),
        value32i(0),
        value32f(valueIn),
        haveValue64f(false),
        haveValue64i(false),
        haveValue32i(false),
        haveValue32f(true)
    {
    }
    double samplingRate{0};
    double time{0};
    int64_t packetNumber{-1};
    double value64f{0};
    int64_t value64i{0};
    int value32i{0};
    float value32f{0};
    bool haveValue64f{false};
    bool haveValue64i{false};
    bool haveValue32i{false};
    bool haveValue32f{false};
};

UWaveServer::Packet queryRowsToPacket(
    const std::string &network,
    const std::string &station,
    const std::string &channel,
    const std::string &locationCode,
    const std::vector<::QueryRow> &rows,
    const int64_t packetNumber)
{
    // Make a list of the indices where this matches
    std::vector<int> indices;
    indices.reserve(rows.size());
    for (int i = 0; i < static_cast<int> (rows.size()); ++i)
    {
        if (rows[i].packetNumber == packetNumber)
        {
            indices.push_back(i);
        } 
    }
    if (indices.empty())
    {
        throw std::invalid_argument(
            "Could not find rows matching packet number "
           + std::to_string(packetNumber));
    }
    // Figure out the data type.  Note, we inserted a packet into the database
    // and tagged that packet with a unique identifyer.  Since the packet has
    // a fixed data type determining the first data type is sufficient to know
    // all the data types of subsequent samples.
    auto &firstRow = rows.at(indices.at(0));
    UWaveServer::Packet packet;
    packet.setNetwork(network);
    packet.setStation(station);
    packet.setChannel(channel);
    packet.setLocationCode(locationCode);
    packet.setSamplingRate(firstRow.samplingRate);
    packet.setStartTime(firstRow.time);
    if (firstRow.haveValue32i)
    {
        std::vector<int> values;
        values.reserve(indices.size());
        for (int i = 0; i < static_cast<int> (indices.size()); ++i)
        {
            values.push_back(rows[i].value32i);
        }
        packet.setData(std::move(values));
    }
    else if (firstRow.haveValue64f)
    {
        std::vector<double> values;
        values.reserve(indices.size());
        for (int i = 0; i < static_cast<int> (indices.size()); ++i)
        {
            values.push_back(rows[i].value64f);
        }
        packet.setData(std::move(values));
    }
    else if (firstRow.haveValue64i)
    {
        std::vector<int64_t> values;
        values.reserve(indices.size());
        for (int i = 0; i < static_cast<int> (indices.size()); ++i)
        {
            values.push_back(rows[i].value64i);
        }
        packet.setData(std::move(values));
    }
    else if (firstRow.haveValue32f)
    {
        std::vector<float> values;
        values.reserve(indices.size());
        for (int i = 0; i < static_cast<int> (indices.size()); ++i)
        {
            values.push_back(rows[i].value32f);
        }
        packet.setData(std::move(values));
    }
    else
    {
        throw std::runtime_error("Unhandled data type in packing reconstruction");
    }
    return packet;
}

std::vector<int64_t> uniquePacketNumbers(
    const std::vector<::QueryRow> &rows)
{
    std::vector<int64_t> packetNumbers;
    if (rows.empty()){return packetNumbers;}
    if (rows.size() == 1)
    {
        packetNumbers.push_back(rows.at(0).packetNumber); 
        return packetNumbers;
    }
    packetNumbers.resize(rows.size());
    for (int i = 0; i < static_cast<int> (rows.size()); ++i)
    {
        packetNumbers[i] = rows[i].packetNumber;
    }
    std::sort(packetNumbers.begin(), packetNumbers.end());
    auto last = std::unique(packetNumbers.begin(), packetNumbers.end());
    packetNumbers.erase(last, packetNumbers.end());
    return packetNumbers;
}

std::vector<UWaveServer::Packet> 
    queryRowsToPackets(const std::string &network,
                       const std::string &station,
                       const std::string &channel,
                       const std::string &locationCode,
                       const std::vector<::QueryRow> &rows)
{
    std::vector<UWaveServer::Packet> result;
    if (rows.empty()){return result;}
    auto packetNumbers = ::uniquePacketNumbers(rows);
    for (const auto &packetNumber : packetNumbers)
    {
        try
        {
            auto packet
                = ::queryRowsToPacket(network, station, channel, locationCode, rows, packetNumber);
            result.push_back(std::move(packet));
        }
        catch (...)
        {
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
    [[nodiscard]] int getSensorIdentifier(const std::string &network,
                                          const std::string &station,
                                          const std::string &channel,
                                          const std::string &locationCode) const
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
    [[nodiscard]] int getSensorIdentifier(const Packet &packet) const
    {   
        auto network = packet.getNetwork();
        auto station = packet.getStation();
        auto channel = packet.getChannel();
        auto locationCode = packet.getLocationCode();
        return getSensorIdentifier(network, station, channel, locationCode);
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
        auto session
            = reinterpret_cast<soci::session *> (mConnection.getSession()); 
        auto sensorIdentifier
            = getSensorIdentifier(network, station, channel, locationCode); // Throws
        if (sensorIdentifier < 0)
        {
            throw std::runtime_error("Could not obtain sensor identifier");
        }
#ifndef NDEBUG
        auto queryTimeStart = std::chrono::high_resolution_clock::now(); 
#endif
        std::vector<QueryRow> queryRows;
        queryRows.reserve(8192);
        constexpr int batchSize{2048};
        double samplingRate{0};
        double time{0};
        int64_t packetNumber{0};
        std::optional<int> value32i;
        std::optional<double> value32f;
        std::optional<double> value64f;
        std::optional<int64_t> value64i;
        int8_t firstDataType{-1};
        bool mixedPrecision{false};
        soci::statement statement = (session->prepare <<
            "SELECT EXTRACT(epoch FROM time), sampling_rate, packet_number, value_i32, value_f32, value_f64, value_i64 FROM sample WHERE sensor_identifier = :sensorIdentifier AND time BETWEEN TO_TIMESTAMP(:startTime) AND TO_TIMESTAMP(:endTime) ORDER BY time ASC",
        soci::use(sensorIdentifier),
        soci::use(startTime),
        soci::use(endTime),
        soci::into(time),
        soci::into(samplingRate),
        soci::into(packetNumber),
        soci::into(value32i), //, indicator32i),
        soci::into(value32f), //, indicator32f),
        soci::into(value64f), //, indicator64f),
        soci::into(value64i)); //, indicator64i));
        statement.execute();
        int nSamplesRead = 0;
        while (statement.fetch())
        {
            nSamplesRead = nSamplesRead + 1;
            //std::cout << std::setprecision(16) << time << " " << value32i << " " << packetNumber << std::endl;
            if (value32i) //indicator32i == soci::indicator::i_ok)
            {
                if (queryRows.empty()){firstDataType = 1;}
                if (firstDataType != 1){mixedPrecision = true;}
                QueryRow row
                {
                    samplingRate,
                    time,
                    packetNumber, *value32i
                };
                queryRows.push_back(std::move(row));
            }
            else if (value32f) //indicator32f == soci::indicator::i_ok)
            {
                if (queryRows.empty()){firstDataType = 2;}
                if (firstDataType != 2){mixedPrecision = true;}
                auto value = static_cast<float> (*value32f);
                QueryRow row
                {
                    samplingRate,
                    time,
                    packetNumber, value
                };
                queryRows.push_back(std::move(row));
            }
            else if (value64f) //indicator64f == soci::indicator::i_ok)
            {
                if (queryRows.empty()){firstDataType = 3;}
                if (firstDataType != 3){mixedPrecision = true;}
                QueryRow row
                {
                    samplingRate,
                    time,
                    packetNumber, *value64f
                };
                queryRows.push_back(std::move(row));
            }
            else if (value64i) //indicator64i == soci::indicator::i_ok)
            {
                if (queryRows.empty()){firstDataType = 4;}
                if (firstDataType != 4){mixedPrecision = true;}
                QueryRow row
                {
                    samplingRate,
                    time,
                    packetNumber, *value64i
                };
                queryRows.push_back(std::move(row));
            }
            else
            {
                spdlog::warn("Unhandled data type");
            }
        }
        if (mixedPrecision)
        {
            throw std::runtime_error("Cannot handle mixed precision");
        }
        if (nSamplesRead != static_cast<int> (queryRows.size()))
        {
            spdlog::warn("Only unpacked " + std::to_string(queryRows.size()) 
                       + " rows out of " + std::to_string(nSamplesRead));
        }
#ifndef NDEBUG
        auto queryTimeEnd = std::chrono::high_resolution_clock::now();
        double queryDuration
             = std::chrono::duration_cast<std::chrono::microseconds>
               (queryTimeEnd - queryTimeStart).count()*1.e-6;
        spdlog::debug("Query time to recover "
                    + std::to_string(nSamplesRead)
                    + " samples was " + std::to_string(queryDuration) + " (s)");
        auto unpackTimeStart = std::chrono::high_resolution_clock::now();
#endif
        result = ::queryRowsToPackets(network, station, channel,
                                      locationCode, queryRows);
#ifndef NDEBUG
        auto unpackTimeEnd = std::chrono::high_resolution_clock::now();
        double unpackDuration
             = std::chrono::duration_cast<std::chrono::microseconds>
               (unpackTimeEnd - unpackTimeStart).count()*1.e-6;
        spdlog::debug("Unpack time was  "
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
        auto session
            = reinterpret_cast<soci::session *> (mConnection.getSession()); 
        // TODO this needs tuning.  It appears multiple batches results
        // in some performance degradation so for now let's keep this big.
        constexpr int batchSize = 2048;
#ifndef NDEBUG
        assert(batchSize > 0);
#endif
        auto sensorIdentifier = getSensorIdentifier(packet); // Throws
        if (sensorIdentifier < 0)
        {
            throw std::runtime_error("Could not obtain sensor identifier");
        }
        int64_t packetNumber = getNextPacketNumber(); // Throws
        auto nSamples = packet.size(); 
        auto dataType = packet.getDataType();
        auto startTime = packet.getStartTime().count()*1.e-6;
        auto samplingRate = packet.getSamplingRate();
        auto samplingPeriod = 1./samplingRate;
        int nBatches = nSamples/batchSize + 1;
        if (dataType == UWaveServer::Packet::DataType::Integer32)
        {
            //spdlog::info("Writing "
            //            + std::to_string(nSamples)
            //            + " samples for packet " + ::toName(packet));
            // N.B. we want to do a bulk insert.  The ON CONFLICT DO NOTHING
            // means that the old data perserveres in case the
            // (identifier, time) already exists.  This is an optimization
            // that also prevents the entire batch from failing in the case
            // of one bad sample.
            auto valuesPtr = static_cast<const int *> (packet.data());
            {
            soci::transaction tr(*session);
            for (int batch = 0; batch < nBatches; ++batch)
            {
                int i1 = batch*batchSize;
                int i2 = std::min(i1 + batchSize, nSamples);
#ifndef NDEBUG
                assert(i2 <= nSamples);
#endif
                auto nFill = i2 - i1; 
                if (nFill < 1){break;}

                std::vector<int> sensorIdentifiers(nFill, sensorIdentifier);
                std::vector<double> samplingRates(nFill, samplingRate);
                std::vector<int64_t> packetNumbers(nFill, packetNumber);
                std::vector<double> times(nFill);
                std::vector<int> values(nFill);
                ::fill(nFill,
                       i1,
                       startTime,
                       samplingPeriod,
                       valuesPtr,
                       times, values);
                // Create the insert statement
                soci::statement statement = (session->prepare <<
                    "INSERT INTO sample(sensor_identifier, time, sampling_rate, packet_number, value_i32) VALUES (:sensorIdentifier, TO_TIMESTAMP(:time), :samplingRate, :packetNumber, :value) ON CONFLICT DO NOTHING",
                soci::use(sensorIdentifiers),
                soci::use(times),
                soci::use(samplingRates),
                soci::use(packetNumbers),
                soci::use(values)); 
                statement.execute(true);
            }
            tr.commit();
            } 
            //spdlog::info("Finished writing integer packet");
        }
        else if (dataType == UWaveServer::Packet::DataType::Float)
        {
            auto valuesPtr = static_cast<const float *> (packet.data());
            {
            soci::transaction tr(*session);
            for (int batch = 0; batch < nBatches; ++batch)
            {
                int i1 = batch*batchSize;
                int i2 = std::min(i1 + batchSize, nSamples);
#ifndef NDEBUG
                assert(i2 <= nSamples);
#endif
                auto nFill = i2 - i1; 
                if (nFill < 1){break;}

                std::vector<int> sensorIdentifiers(nFill, sensorIdentifier);
                std::vector<double> samplingRates(nFill, samplingRate);
                std::vector<int64_t> packetNumbers(nFill, packetNumber);
                std::vector<double> times(nFill);
                std::vector<double> values(nFill);
                ::fill(nFill,
                       i1,
                       startTime,
                       samplingPeriod,
                       valuesPtr,
                       times, values);
                // Create the insert statement
                soci::statement statement = (session->prepare <<
                    "INSERT INTO sample(sensor_identifier, time, sampling_rate, packet_number, value_f32) VALUES (:sensorIdentifier, TO_TIMESTAMP(:time), :samplingRate, :packetNumber, :value) ON CONFLICT DO NOTHING",
                soci::use(sensorIdentifiers),
                soci::use(times),
                soci::use(samplingRates),
                soci::use(packetNumbers),
                soci::use(values)); 
                statement.execute(true);
            }
            tr.commit();
            }
        }
        else if (dataType == UWaveServer::Packet::DataType::Double)
        {
            auto valuesPtr = static_cast<const double *> (packet.data());
            {
            soci::transaction tr(*session);
            for (int batch = 0; batch < nBatches; ++batch)
            {
                int i1 = batch*batchSize;
                int i2 = std::min(i1 + batchSize, nSamples);
#ifndef NDEBUG
                assert(i2 <= nSamples);
#endif
                auto nFill = i2 - i1; 
                if (nFill < 1){break;}

                std::vector<int> sensorIdentifiers(nFill, sensorIdentifier);
                std::vector<double> samplingRates(nFill, samplingRate);
                std::vector<int64_t> packetNumbers(nFill, packetNumber);
                std::vector<double> times(nFill);
                std::vector<double> values(nFill);
                ::fill(nFill,
                       i1,
                       startTime,
                       samplingPeriod,
                       valuesPtr,
                       times, values);
                // Create the insert statement
                soci::statement statement = (session->prepare <<
                    "INSERT INTO sample(sensor_identifier, time, sampling_rate, packet_number, value_f64) VALUES (:sensorIdentifier, TO_TIMESTAMP(:time), :samplingRate, :packetNumber, :value) ON CONFLICT DO NOTHING",
                soci::use(sensorIdentifiers),
                soci::use(times),
                soci::use(samplingRates),
                soci::use(packetNumbers),
                soci::use(values)); 
                statement.execute(true);
            }
            tr.commit();
            }
        }
        else if (dataType == UWaveServer::Packet::DataType::Integer64)
        {
            auto valuesPtr = static_cast<const int64_t *> (packet.data());
            {
            soci::transaction tr(*session);
            for (int batch = 0; batch < nBatches; ++batch)
            {
                int i1 = batch*batchSize;
                int i2 = std::min(i1 + batchSize, nSamples);
#ifndef NDEBUG
                assert(i2 <= nSamples);
#endif
                auto nFill = i2 - i1; 
                if (nFill < 1){break;}

                std::vector<int> sensorIdentifiers(nFill, sensorIdentifier);
                std::vector<double> samplingRates(nFill, samplingRate);
                std::vector<int64_t> packetNumbers(nFill, packetNumber);
                std::vector<double> times(nFill);
                std::vector<int64_t> values(nFill);
                ::fill(nFill,
                       i1,
                       startTime,
                       samplingPeriod,
                       valuesPtr,
                       times, values);
                // Create the insert statement
                soci::statement statement = (session->prepare <<
                    "INSERT INTO sample(sensor_identifier, time, sampling_rate, packet_number, value_i64) VALUES (:sensorIdentifier, TO_TIMESTAMP(:time), :samplingRate, :packetNumber, :value) ON CONFLICT DO NOTHING",
                soci::use(sensorIdentifiers),
                soci::use(times),
                soci::use(samplingRates),
                soci::use(packetNumbers),
                soci::use(values)); 
                statement.execute(true);
            }
            tr.commit();
            }
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
        initializeSensors();
    }
    mutable std::mutex mMutex;
    mutable Connection::PostgreSQL mConnection;
    mutable std::map<std::string, int> mSensorIdentifiers;
    std::chrono::seconds mRetentionDuration{365*86400}; // Make it something large like a year
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
