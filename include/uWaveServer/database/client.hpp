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
class Client
{
public:
    explicit Client(Connection::PostgreSQL &&connection);
    Client() = delete;
    void write(const UWaveServer::Packet &packet);
    ~Client();
private:
    class ClientImpl;
    std::unique_ptr<ClientImpl> pImpl;
};
}
#endif 
