#include <iostream>
#include <ctime>
#include <chrono>
#include <boost/url.hpp>
#include <boost/algorithm/string.hpp>
#include <spdlog/spdlog.h>
#include "uWaveServer/database/client.hpp"
#include "uWaveServer/packet.hpp"
#include "callback.hpp"

using namespace UWaveServer::WebServer;

namespace
{
/// Converts YYYY-MM-DDTHH:MM:SS or YYYY-MM-DDTHH:MM:SS.XXXXXX
/// to seconds since the epoch.
double toTimeStamp(const std::string &timeStringIn)
{
    auto timeString = timeStringIn;
    if (timeString.back() == 'Z'){timeString.pop_back();}
    // In case we have something like 2025-04-22T12:13:22.32
/*
    if (timeString.size() > 19)
    {
        while (timeString.size() < 26)
        {
            timeString.push_back('0');
        }
    }
*/
    int year{1900};
    unsigned int month{1};
    unsigned int dom{1};
    int hour{0};
    int minute{0};
    int second{0};
    int microSecond{0};
    if (timeString.size() == 26)
    {
        sscanf(timeString.c_str(),
               "%04d-%02d-%02dT%02d:%02d:%02d.%06d",
               &year, &month, &dom, &hour, &minute, &second, &microSecond);
    }
    else if (timeString.size() == 19)
    {
        sscanf(timeString.c_str(),
               "%04d-%02d-%02dT%02d:%02d:%02d",
               &year, &month, &dom, &hour, &minute, &second);
    }
    else
    {
        throw std::invalid_argument(timeString + " is an invalid time string");
    } 
    auto yearMonthDay = std::chrono::year_month_day(std::chrono::year {year},
                                                    std::chrono::month {month},
                                                    std::chrono::day {dom} );
    std::chrono::sys_days elapsedDays{yearMonthDay};
    auto hourMinuteSecond
        = std::chrono::hh_mm_ss<std::chrono::seconds> {
             std::chrono::hours {hour}
           + std::chrono::minutes (minute)
           + std::chrono::seconds (second) };
    auto timeStamp = elapsedDays.time_since_epoch() + hourMinuteSecond.to_duration();
    auto time = timeStamp.count() + microSecond*1.e-6;
    return time;
}
}

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
/*
std::cout << "---------------------------" << std::endl;
std::cout << requestHeader.target() << std::endl;
std::cout << requestHeader["host"] << std::endl;
std::cout << message << std::endl;
std::cout << "---------------------------" << std::endl;
*/
    // Try to build a URL to parse
    std::string host;
    try
    {
        host = std::string {requestHeader["host"]};
    }
    catch (...)
    {
        host = "127.0.0.1";
    }
    auto requestString = std::string {requestHeader.target()};
    if (requestString.empty())
    {
        requestString = "/";
    }
    if (requestString.at(0) != '/')
    {
        requestString = "/" + requestString;
    }
    if (host.back() == '/'){host.pop_back();} 
    if (host.find("http://") != 0 || host.find("https://" != 0))
    {
        host = "http://" + host;
    }
    auto uri = host + requestString;
    // Try to lift parameters
    spdlog::info("Parsing: " + uri);
    boost::system::result<boost::url_view> parsedURL;
    try
    {
        parsedURL = boost::urls::parse_uri(host + requestString); 
    }
    catch (const std::exception &e) 
    {   
        spdlog::warn("Invalid URI: " + uri);
        throw std::runtime_error("Failed to build/parse URI");
    }   
    // Now we unpack the parameters
    std::string network;
    std::string station;
    std::string channel;
    std::string locationCode;
    double startTime{0};
    double endTime{0};
    for (auto param : parsedURL->params())
    {
        //std::cout << param.key << " " << param.value << std::endl;
        auto key = std::string {param.key};
        boost::algorithm::to_lower(param.key);
        if (key.find("net") == 0 || key.find("network") == 0)
        {
            network = std::string {param.value};
            boost::algorithm::to_upper(network);
        }
        else if (key.find("sta") == 0 || key.find("station") == 0)
        {
            if (key.find("start") == 0 || key.find("starttime") == 0)
            {
                startTime = ::toTimeStamp(std::string {param.value});
            }
            else
            {
                station = std::string {param.value};
                boost::algorithm::to_upper(station);
            }
        }
        else if (key.find("cha") == 0 || key.find("channel") == 0)
        {
            channel = std::string {param.value};
            boost::algorithm::to_upper(channel);
        }
        else if (key.find("loc") == 0 || key.find("location") == 0)
        {
            locationCode = std::string {param.value};
            boost::algorithm::to_upper(locationCode);
        }
        else if (key.find("end") == 0 || key.find("endtime") == 0)
        {
            endTime = ::toTimeStamp(std::string {param.value});
        }
    }
    if (network.empty()){throw std::invalid_argument("net[work] not specified");}
    if (station.empty()){throw std::invalid_argument("sta[tion] not specified");} 
    if (channel.empty()){throw std::invalid_argument("cha[nnel] not specified");}
    if (startTime >= endTime)
    {
        throw std::invalid_argument("start[time] = "
                                  + std::to_string(startTime)
                                  + " must be less than end[time] = "
                                  + std::to_string(endTime));
    }
    spdlog::info("Querying: " + network + "." + station + "." + channel + "." + locationCode + " from time " + std::to_string(startTime) + " to " + std::to_string(endTime));
    if (pImpl->mPostgresClient)
    {
        try
        {
            auto data = pImpl->mPostgresClient->query(network, station, channel, locationCode, startTime, endTime);
        }
        catch (const std::exception &e)
        {
            spdlog::error(e.what());
        }
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
