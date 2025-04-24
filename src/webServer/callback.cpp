#include <iostream>
#include <boost/url.hpp>
#include <spdlog/spdlog.h>
#include "uWaveServer/database/client.hpp"
#include "callback.hpp"

using namespace UWaveServer::WebServer;

class Callback::CallbackImpl
{
public:
    /// @brief Authenticates the user with the given user name and
    ///        password stored in a base64 representation.  
    /// @result The response to propagate back to the client and
    ///         the user's credentials.
    /// @throws InvalidPermissionException if the user cannot be authenticated.
/*
    [[nodiscard]] std::pair<nlohmann::json, IAuthenticator::Credentials>
        authenticate(const std::string &userNameAndPasswordHex) const
    {
        nlohmann::json jsonResponse; 
        IAuthenticator::Credentials credentials;
        auto authorizationValue
            = base64::from_base64(userNameAndPasswordHex);
        auto splitIndex = authorizationValue.find(":");
        if (splitIndex != authorizationValue.npos)
        {
            std::string userName
                = authorizationValue.substr(0, splitIndex);
            std::string password;
            if (splitIndex < authorizationValue.length() - 1)
            {
                 password
                     = authorizationValue.substr(splitIndex + 1,
                                                 authorizationValue.length());
            }
            if (!mAuthenticator->authenticate(userName, password))
            {
                throw InvalidPermissionException("Could not authenticate "
                                               + userName);
            }

            spdlog::debug("Sending authentication response...");
            auto tempCredentials = mAuthenticator->getCredentials(userName); 
            if (tempCredentials)
            {
                credentials = std::move(*tempCredentials);
                jsonResponse["status"] = "success";
                jsonResponse["jsonWebToken"] = credentials.token;
                jsonResponse["permissions"]
                    = CCTService::permissionsToString(credentials.permissions);
            }
            else
            {
                spdlog::warn("Somehow failed to return credentials");
                throw std::runtime_error("Internal error");
            }
        }
        else
        {
            throw BadRequestException(
               "Basic authorization field could not be parsed");
        }
        return std::pair {jsonResponse, credentials};
    }
    /// @brief Authorizes a user given with the provided JWT.
    /// @param[in] jsonWebToken  The JSON web token.
    /// @result The user's credentials.
    /// @throws InvalidPermissionException if the user is not authorized.
    [[nodiscard]] IAuthenticator::Credentials 
        authorize(const std::string jsonWebToken) const
    {
        IAuthenticator::Credentials credentials;
        spdlog::debug("Created: " + jsonWebToken);
        try
        {
             credentials = mAuthenticator->authorize(jsonWebToken);
             spdlog::debug("Callback authorized " + credentials.user);
        }
        catch (const std::exception &e)
        {
             spdlog::debug("Could not authenticated bearer token: "
                         + std::string {e.what()});
             throw InvalidPermissionException(
                 "Could not authorize bearer token");
        }
        return credentials;
    }
*/
///private:
    std::function<std::string (const boost::beast::http::header
                               <
                                   true,
                                   boost::beast::http::basic_fields<std::allocator<char>>
                               > &,
                               const std::string &,
                               const boost::beast::http::verb)> mCallbackFunction;
    std::shared_ptr<UWaveServer::Database::Client> mPostgresClient{nullptr};
/*
    std::shared_ptr<
       std::map<std::string, std::unique_ptr<CCTService::AQMSPostgresClient>>
    > mAQMSClients{nullptr};
    std::shared_ptr<CCTService::IAuthenticator> mAuthenticator{nullptr};
*/
};


/// @brief Constructor.
Callback::Callback(
    std::shared_ptr<UWaveServer::Database::Client> &postgresClient
/*
    std::shared_ptr<
       std::map<std::string, std::unique_ptr<CCTService::AQMSPostgresClient>>
    > &aqmsClients,
    std::shared_ptr<CCTService::IAuthenticator> &authenticator
*/
    ) :
    pImpl(std::make_unique<CallbackImpl> ())
{
    if (postgresClient == nullptr)
    {
        throw std::invalid_argument("Postgres client is NULL");
    }
/*
    if (!cctEventsService->isRunning())
    {
        throw std::runtime_error("CCT event service is not running");
    }
    if (authenticator == nullptr)
    {
        throw std::invalid_argument("Authenticator is NULL");
    }
*/
    pImpl->mCallbackFunction
        = std::bind(&Callback::operator(),
                    this,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3);
    pImpl->mPostgresClient = postgresClient;
//    pImpl->mAQMSClients = aqmsClients;
//    pImpl->mAuthenticator = authenticator;
}

/// @brief Destructor.
Callback::~Callback() = default;


/// @brief Actually processes the requests.
std::string Callback::operator()(
    const boost::beast::http::header
    <
        true,
        boost::beast::http::basic_fields<std::allocator<char>>
    > &requestHeader,
    const std::string &message,
    const boost::beast::http::verb httpRequestType) const
{
std::cout << "---------------------------" << std::endl;
std::cout << requestHeader.target() << std::endl;
std::cout << message << std::endl;
std::cout << "---------------------------" << std::endl;
    // I only know how to handle these types of requests
    if (httpRequestType != boost::beast::http::verb::get)
    {
        throw std::runtime_error("Unhandled http request verb");
    }
    // Try to parse the URI from the header
    std::string network;
    std::string station;
    std::string channel;
    std::string locationCode;
    try
    {
for(auto &&it : requestHeader)
{
std::cout << "header: " << it.name() << " " << it.value() << std::endl;
}
        auto uri = "http://" + std::string{requestHeader["Host"]};
        auto parsedURI = boost::urls::parse_uri(uri);//requestHeader["Host"]);
        for (auto param : parsedURI->params())
        {
            std::cout << param.key << " " << param.value << std::endl;
        }
    }
    catch (...)
    {

    }
    // Maybe it came from curl
    return "{\"win\" : \"yes\"}";

}

/// @result A function pointer to the callback.
std::function<
    std::string (
        const boost::beast::http::header<
            true,
            boost::beast::http::basic_fields<std::allocator<char>> 
        > &,
        const std::string &,
        const boost::beast::http::verb
    )>
Callback::getCallbackFunction() const noexcept
{
    return pImpl->mCallbackFunction;
}
