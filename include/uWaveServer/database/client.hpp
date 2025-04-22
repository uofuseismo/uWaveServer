#ifndef UWAVE_SERVER_DATABASE_CLIENT_DB_HPP
#define UWAVE_SERVER_DATABASE_CLIENT_DB_HPP
#include <memory>
namespace UWaveServer
{
 class Packet;
}
namespace UWaveServer::Database::Connection
{
 class PostgreSQL;
}
namespace UWaveServer::Database
{
/// @brief Defines a database Timescale + Postgres database client.
/// @copyright Ben Baker (University of Utah) distributed under the MIT license.
class Client
{
public:
    /// @brief Constructs the client from the given postgres connection.
    explicit Client(Connection::PostgreSQL &&connection);
    /// @brief Writes a packet to the database.
    /// @param[in] packet  A data packet with a network, station, channel,
    ///                    and location code as well as a start time, end time,
    ///                    sampling rate, and time series.
    /// @throws std::invalid_argument if the packet is malformed.
    /// @throws std::runtime_error if there is another error while writing
    ///         the data.
    void write(const UWaveServer::Packet &packet);
    /// @brief Destrutcor.
    ~Client();

    Client() = delete;
private:
    class ClientImpl;
    std::unique_ptr<ClientImpl> pImpl;
};
}
#endif 
