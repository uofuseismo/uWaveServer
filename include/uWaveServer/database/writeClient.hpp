#ifndef UWAVE_SERVER_DATABASE_WRITE_CLIENT_DB_HPP
#define UWAVE_SERVER_DATABASE_WRITE_CLIENT_DB_HPP
#include <memory>
#include <vector>
#include <string>
#include <chrono>
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
///        This is explicitly designed for writing threads.
/// @copyright Ben Baker (University of Utah) distributed under the MIT license.
class WriteClient
{
public:
    /// @brief Constructs the client from the given postgres connection.
    explicit WriteClient(const Credentials &credentials);

    /// @brief Writes a packet to the database.
    /// @param[in] packet  A data packet with a network, station, channel,
    ///                    and location code as well as a start time, end time,
    ///                    sampling rate, and time series.
    /// @throws std::invalid_argument if the packet is malformed.
    /// @throws std::runtime_error if there is another error while writing
    ///         the data.
    void write(const UWaveServer::Packet &packet);

    /// @result True indicates the network, station, channel, and locationCode
    ///         packets are in the database.
    [[nodiscard]] bool contains(const std::string &network,
                                const std::string &station,
                                const std::string &channel,
                                const std::string &locationCode) const;
    /// @result Get most up-to-date list of sensors in the database.
    //[[nodiscard]] std::set<std::string> getSensors() const;
    
    /// @brief (Re)Establishes a connection.
    void connect();
    /// @result True indicates the client is connected.
    [[nodiscard]] bool isConnected() const noexcept;
    /// @brief Closes the connection.
    void disconnect();

    /// @brief Destrutcor.
    ~WriteClient();

    WriteClient() = delete;
private:
    class WriteClientImpl;
    std::unique_ptr<WriteClientImpl> pImpl;
};
}
#endif 
