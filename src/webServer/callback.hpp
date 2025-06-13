#ifndef UWAVE_SERVER_WEB_SERVER_CALLBACK_HPP
#define UWAVE_SERVER_WEB_SERVER_CALLBACK_HPP
#include <vector>
#include <string>
#include <memory>
#include <map>
#include <functional>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/verb.hpp>
namespace UWaveServer::Database
{
 class Client;
}
namespace UWaveServer::WebServer
{
class IAuthenticator;
}
namespace UWaveServer::WebServer
{
/// @class Callback "callback.hpp" 
/// @brief This is the callback that the Beast server will call to process
///        responses.
/// @detail This exists simply to make the Beast server file smaller (it's large
///         to begin with) and speed up compilation.
/// @copyright Ben Baker (University of Utah) distributed under the MIT license.
class Callback
{
public:
    Callback() = delete;
    /// @brief Constructor.
    explicit Callback(std::vector<std::unique_ptr<UWaveServer::Database::Client>> &&postgresClient);
/*
             std::shared_ptr<
                std::map<std::string, std::unique_ptr<CCTService::AQMSPostgresClient>>
             > &client,
             std::shared_ptr<CCTService::IAuthenticator> &authenticator);
*/
    /// @brief Destructor.
    ~Callback();
    /// @brief Processes an HTTP GET/POST/PUT request, e.g., 
    ///        jsonPayLoad = callback(httpHeader, httpPayload, httpVerb);
    /// @param[in] header   The HTTP header.  Most critically, this will contain
    ///                     the Authorization field which can be Basic
    ///                     or Bearer.
    /// @param[in] message  The JSON request message to process.
    /// @param[in] method   The HTTP verb - e.g., GET/POST/PUT.
    [[nodiscard]]
    std::pair<std::string, std::string> 
        operator()(const boost::beast::http::header<true, boost::beast::http::basic_fields<std::allocator<char> > > &requestHeader,
                   const std::string &message,
                   boost::beast::http::verb method) const;
    /// @result A function pointer to the callback function.
    [[nodiscard]]
    std::function<
        std::pair<std::string, std::string>
        (
            const boost::beast::http::header<true, boost::beast::http::basic_fields<std::allocator<char> > > &,
            const std::string &,
            const boost::beast::http::verb
        )
    > getCallbackFunction() const noexcept;

    Callback& operator=(const Callback &) = delete;
    Callback(const Callback &) = delete;
private:
    class CallbackImpl;
    std::unique_ptr<CallbackImpl> pImpl;
};
}
#endif
