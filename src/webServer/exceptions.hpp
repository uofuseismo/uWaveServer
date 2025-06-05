#ifndef UWAVE_SERVER_WEB_SERVER_SERVICE_EXCEPTIONS_HPP
#define UWAVE_SERVER_WEB_SERVER_SERVICE_EXCEPTIONS_HPP
#include <exception>
namespace UWaveServer::WebServer
{
/// @brief This should result in a 403 FORBIDDEN error.
/// @copyright Ben Baker (UUSS) distributed under the MIT license.
class InvalidPermissionException final : public std::exception 
{
public:
    InvalidPermissionException(const std::string &message) :
        mMessage(message)
    {
    }
    InvalidPermissionException(const char *message) :
        mMessage(message)
    {
    }
    ~InvalidPermissionException() final = default;
    virtual const char *what () const noexcept final
    {
        return mMessage.c_str();
    }
private:
    std::string mMessage;
};
/// @brief This should result in a 400 Bad Request error.
/// @copyright Ben Baker (UUSS) distributed under the MIT license.
class BadRequestException final : public std::exception
{
public:
    BadRequestException(const std::string &message) :
        mMessage(message)
    {   
    }   
    BadRequestException(const char *message) :
        mMessage(message)
    {   
    }   
    ~BadRequestException() final = default;
    virtual const char *what () const noexcept final
    {   
        return mMessage.c_str();
    }   
private:
    std::string mMessage;
};
/// @brief This should result in a 501 not-implemented error.
/// @copyright Ben Baker (UUSS) distributed under the MIT license.
class UnimplementedException final : public std::exception 
{
public:
    UnimplementedException(const std::string &message) :
        mMessage(message)
    {   
    }   
    UnimplementedException(const char *message) :
        mMessage(message)
    {   
    }   
    ~UnimplementedException() final = default;
    virtual const char *what () const noexcept final
    {   
        return mMessage.c_str();
    }   
private:
    std::string mMessage;
};
/// @brief This should result in a 204 no-content error.
/// @copyright Ben Baker (UUSS) distributed under the MIT license.
class NoContentException final : public std::exception 
{
public:
    NoContentException(const std::string &message) :
        mMessage(message)
    {   
    }   
    NoContentException(const char *message) :
        mMessage(message)
    {   
    }   
    ~NoContentException() final = default;
    virtual const char *what () const noexcept final
    {   
        return mMessage.c_str();
    }   
private:
    std::string mMessage;
};
/// @brief This should result in a 404 not found error.
/// @copyright Ben Baker (UUSS) distributed under the MIT license.
class NotFoundException final : public std::exception 
{
public:
    NotFoundException(const std::string &message) :
        mMessage(message)
    {   
    }   
    NotFoundException(const char *message) :
        mMessage(message)
    {   
    }   
    ~NotFoundException() final = default;
    virtual const char *what () const noexcept final
    {   
        return mMessage.c_str();
    }   
private:
    std::string mMessage;
};

}
#endif
