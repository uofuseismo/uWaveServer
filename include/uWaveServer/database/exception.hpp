#ifndef UWAVE_SERVER_DATABASE_EXCEPTION_HPP
#define UWAVE_SERVER_DATABASE_EXCEPTION_HPP
#include <string>
#include <exception>
namespace UWaveServer::Database
{
class ReconnectError final : public std::exception
{
public:
    explicit ReconnectError(char *msg) :
        mMessage(msg)
    {
    }
    explicit ReconnectError(std::string &&msg) :
        mMessage(std::move(msg))
    {
    }
    const char *what() const noexcept
    {
        return mMessage.c_str();
    } 
private:
    std::string mMessage{"Unable to reconnect to database"};
};
}
#endif
