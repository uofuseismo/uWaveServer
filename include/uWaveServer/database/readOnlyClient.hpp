#ifndef UWAVE_SERVER_DATABASE_READ_ONLY_CLIENT_HPP
#define UWAVE_SERVER_DATABASE_READ_ONLY_CLIENT_HPP
#include <memory>
#include <vector>
#include <string>
#include <chrono>
#include <map>
#include <set>
namespace UWaveServer
{
 class Packet;
}
namespace UWaveServer::Database
{
 class Credentials;
}
namespace UWaveServer::Database
{
/// @brief Defines a database Timescale + Postgres database client.
/// @copyright Ben Baker (University of Utah) distributed under the MIT license.
class ReadOnlyClient
{
public:
    /// @brief Constructs the client from the given postgres connection.
    explicit ReadOnlyClient(const Credentials &credentials);

    /// @result True indicates the network, station, channel, and locationCode
    ///         packets are in the database.
    [[nodiscard]] bool contains(const std::string &network,
                                const std::string &station,
                                const std::string &channel,
                                const std::string &locationCode,
                                const bool checkCacheOnly = false) const;
    [[nodiscard]] std::vector<UWaveServer::Packet>
        query(const std::string &network,
              const std::string &station,
              const std::string &channel,
              const std::string &locationCode,
              const std::chrono::microseconds &startTime,
              const std::chrono::microseconds &endTime) const;
    [[nodiscard]] std::vector<UWaveServer::Packet>
        query(const std::string &network,
              const std::string &station,
              const std::string &channel,
              const std::string &locationCode,
              const double startTime,
              const double endTime) const;
    [[nodiscard]] std::map<std::string, std::vector<UWaveServer::Packet>>
        queryAllChannelsForStation(const std::string &network,
                                   const std::string &station,
                                   const std::chrono::microseconds &t0MuS,
                                   const std::chrono::microseconds &t1MuS) const;
    [[nodiscard]] std::map<std::string, std::vector<UWaveServer::Packet>>
        queryAllChannelsForStation(const std::string &network,
                                   const std::string &station,
                                   const double starTime,
                                   const double endTime) const;

    /// @result Get most up-to-date list of streams in the database.
    [[nodiscard]] std::set<std::string> getStreams() const;
    
    /// @brief (Re)Establishes a connection.
    void connect();
    /// @result True indicates the client is connected.
    [[nodiscard]] bool isConnected() const noexcept;
    /// @brief Closes the connection.
    void disconnect();

    /// @brief Destrutcor.
    ~ReadOnlyClient();

    ReadOnlyClient() = delete;
private:
    class ReadOnlyClientImpl;
    std::unique_ptr<ReadOnlyClientImpl> pImpl;
};
}
#endif 
