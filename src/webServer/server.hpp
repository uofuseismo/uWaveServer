#ifndef SERVER_HPP
#define SERVER_HPP
// This is a customized version of Vinnie Falco's HTTP plain/SSL server
// that performs asynchronous communication.  That file was distributed
// under the Boost Software License, Version 1.0.
// (https://www.boost.org/LICENSE_1_0.txt)
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/strand.hpp>
#include <boost/config.hpp>
#include <boost/algorithm/string.hpp>
#include <functional>
#include <map>
#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include "listener.hpp"
#include "exceptions.hpp"

/*
namespace beast = boost::beast;         // from <boost/beast.hpp>
namespace http = beast::http;           // from <boost/beast/http.hpp>
namespace net = boost::asio;            // from <boost/asio.hpp>
namespace ssl = boost::asio::ssl;       // from <boost/asio/ssl.hpp>
using tcp = boost::asio::ip::tcp;       // from <boost/asio/ip/tcp.hpp>
*/

namespace
{

// Return a reasonable mime type based on the extension of a file.
boost::beast::string_view
getMIMEType(const boost::beast::string_view path)
{
    const std::map<std::string, std::string> fileMap
    {
        std::pair {".htm",     "text/html"},
        std::pair {".html",    "text/html"},
        std::pair {".php",     "text/html"},
        std::pair {".css",     "text/css"},
        std::pair {".txt",     "text/plain"},
        std::pair {".js",      "application/javascript"},
        std::pair {".json",    "application/json"},
        std::pair {".geojson", "application/json"},
        std::pair {".xml",     "application/xml"},
        std::pair {".swf",     "application/x-shockwave-flash"},
        std::pair {".flv",     "video/x-flv"},
        std::pair {".png",     "image/png"},
        std::pair {".jpe",     "image/jpeg"},
        std::pair {".jpeg",    "image/jpeg"},
        std::pair {".jpg",     "image/jpeg"},
        std::pair {".gif",     "image/gif"},
        std::pair {".bmp",     "image/bmp"},
        std::pair {".ico",     "image/vnd.microsoft.icon"},
        std::pair {".tiff",    "image/tiff"},
        std::pair {".tif",     "image/tiff"},
        std::pair {".svg",     "image/svg+xml"},
        std::pair {".svgz",    "image/svg+xml"}
    }; 
    auto extension = std::filesystem::path {path}.extension().string();
    auto index = fileMap.find(extension);
    if (index == fileMap.end()){return "application/text";}
    return index->second; 
}

// Append an HTTP relative-path to a local filesystem path.
// The returned path is normalized for the platform.
std::string createPath(boost::beast::string_view base,
                       boost::beast::string_view path)
{
    std::string result(base);
#ifdef BOOST_MSVC
    constexpr char pathSeparator = '\\';
    if (result.back() == pathSeparator)
    {
        result.resize(result.size() - 1);
    }
    result.append(path.data(), path.size());
    for (auto &c : result)
    {
        if (c == '/'){c = pathSeparator;}
    }
#else
    constexpr char pathSeparator = '/';
    if (result.back() == pathSeparator)
    {
        result.resize(result.size() - 1);
    }
    result.append(path.data(), path.size());
#endif
    return result;
}

// Return a response for the given request.
//
// The concrete type of the response message (which depends on the
// request), is type-erased in message_generator.
template <class Body, class Allocator>
boost::beast::http::message_generator
handleRequest(
    boost::beast::string_view documentRoot,
    boost::beast::http::request
    <
       Body, boost::beast::http::basic_fields<Allocator>
    > &&request,
    std::function<std::string (const boost::beast::http::header
                               <
                                   true,
                                   boost::beast::http::basic_fields<std::allocator<char>>
                               > &,
                               const std::string &,
                               const boost::beast::http::verb)> &callback)
{
    //std::cout << request.base() << std::endl;
    //std::cout << request.body() << std::endl;
    // Returns a bad request response
    const auto badRequest = [&request](boost::beast::string_view why)
    {
        spdlog::info("Bad request");
        boost::beast::http::response<boost::beast::http::string_body> result
        {
            boost::beast::http::status::bad_request,
            request.version()
        };
#ifdef ENABLE_CORS
        result.set(boost::beast::http::field::access_control_allow_origin, "*");
#endif
        result.set(boost::beast::http::field::server,
                   BOOST_BEAST_VERSION_STRING);
        result.set(boost::beast::http::field::content_type,
                   "application/json");
        result.keep_alive(request.keep_alive());
        result.body() = "{\"status\":\"error\", \"reason\":\""
                      + std::string(why)
                      + "\"}";
        result.prepare_payload();
        return result;
    };

    // Returns a invalid permission (403) response
    const auto forbidden = [&request](boost::beast::string_view why)
    {
        spdlog::info("Forbidden");
        boost::beast::http::response<boost::beast::http::string_body> result
        {
            boost::beast::http::status::forbidden,
            request.version()
        };
#ifdef ENABLE_CORS
        result.set(boost::beast::http::field::access_control_allow_origin, "*");
#endif
        result.set(boost::beast::http::field::server,
                   BOOST_BEAST_VERSION_STRING);
        result.set(boost::beast::http::field::content_type,
                   "application/json");
        result.keep_alive(request.keep_alive());
        result.body() = "{\"status\":\"error\",\"reason\":\""
                      + std::string(why)
                      + "\"}";
        result.prepare_payload();
        return result;
    }; 

    // Returns an unimplemented (501) response
    const auto unimplemented = [&request](boost::beast::string_view why)
    {
        spdlog::info("Unimplemented");
        boost::beast::http::response<boost::beast::http::string_body> result
        {
            boost::beast::http::status::not_implemented,
            request.version()
        };
#ifdef ENABLE_CORS
        result.set(boost::beast::http::field::access_control_allow_origin, "*");
#endif
        result.set(boost::beast::http::field::server,
                   BOOST_BEAST_VERSION_STRING);
        result.set(boost::beast::http::field::content_type,
                   "text/html");
        result.keep_alive(request.keep_alive());
        result.body() = std::string(why);
        result.prepare_payload();
        return result;
    };

    // Returns a not found response
    const auto notFound = [&request](boost::beast::string_view target)
    {
        spdlog::info("Not found");
        boost::beast::http::response<boost::beast::http::string_body> result
        {
            boost::beast::http::status::not_found,
            request.version()
        };
#ifdef ENABLE_CORS
        result.set(boost::beast::http::field::access_control_allow_origin, "*");
#endif
        result.set(boost::beast::http::field::server,
                   BOOST_BEAST_VERSION_STRING);
        result.set(boost::beast::http::field::content_type,
                   "text/html");
        result.keep_alive(request.keep_alive());
        result.body() = "The resource '" + std::string(target)
                      + "' was not found.";
        result.prepare_payload();
        return result;
    };

    // Returns an indication that user is not authorized
    const auto unauthorized = [&request](boost::beast::string_view target)
    {
        spdlog::info("Unauthorized");
        boost::beast::http::response<boost::beast::http::string_body> result
        {
            boost::beast::http::status::unauthorized,
            request.version()
        };
#ifdef ENABLE_CORS
        result.set(boost::beast::http::field::access_control_allow_origin, "*");
#endif
        result.set(boost::beast::http::field::server,
                   BOOST_BEAST_VERSION_STRING);
        result.set(boost::beast::http::field::content_type,
                   "application/json");
        result.keep_alive(request.keep_alive());
        result.body() = "{\"status\":\"error\",\"reason\":\""
                      + std::string(target)
                      + "\"}";
        result.prepare_payload();
        return result;
    };

    /// Options request for CORS
    auto optionsHandler = [&request]()
    {
        spdlog::debug("CORS");
        boost::beast::http::response<boost::beast::http::string_body> result
        {
            boost::beast::http::status::no_content,
            request.version()
        };
#ifdef ENABLE_CORS
        result.set(boost::beast::http::field::access_control_allow_origin, "*");
#endif
        result.set("Access-Control-Allow-Credentials",
                   "true");
        result.set(boost::beast::http::field::access_control_allow_methods,
                   "GET,HEAD,OPTIONS,POST,PUT");
        result.set(boost::beast::http::field::access_control_allow_headers,
                   "Access-Control-Allow-Origin, Access-Control-Allow-Headers, Access-Control-Allow-Methods, Connection, Origin, Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers, Authorization");
        result.set(boost::beast::http::field::access_control_max_age,
                   "3600");
        result.set(boost::beast::http::field::connection,
                   "close");
        result.set(boost::beast::http::field::server,
                   BOOST_BEAST_VERSION_STRING);
        result.set(boost::beast::http::field::content_type,
                   "text/html");
        result.keep_alive(request.keep_alive());
        result.body() = "";
        result.prepare_payload();
        return result;
    };

    // Returns a server error response
    auto const serverError = [&request](boost::beast::string_view what)
    {
        spdlog::info("Server error");
        boost::beast::http::response<boost::beast::http::string_body> result
        {
            boost::beast::http::status::internal_server_error,
            request.version()
        };
#ifdef ENABLE_CORS
        result.set(boost::beast::http::field::access_control_allow_origin, "*");
#endif
        result.set(boost::beast::http::field::server,
                   BOOST_BEAST_VERSION_STRING);
        result.set(boost::beast::http::field::content_type,
                   "application/json");
        result.keep_alive(request.keep_alive());
        result.body() = "{\"status\":\"error\",\"reason\":\""
                      + std::string(what)
                      + "\"}";
        result.prepare_payload();
        return result;
    };

    // This comes up during CORS weirdness.  Basically, we need to tell
    // the browser all the headers the backend will accept.
    if (request.method() == boost::beast::http::verb::options)
    { 
        return optionsHandler();
    }

    // Make sure we can handle the method
    if (request.method() != boost::beast::http::verb::get)
    {
        return badRequest("Unknown HTTP-method");
    }

    // Process the put/post/get request
    if (request.method() == boost::beast::http::verb::get)
    {
/*
        try
        {
            auto payload = callback(request.base(),
                                    request.body(),
                                    request.method());
            boost::beast::http::response<boost::beast::http::string_body> result
            {
                boost::beast::http::status::ok,
                request.version()
            };
#ifdef ENABLE_CORS
            result.set(boost::beast::http::field::access_control_allow_origin, "*");
#endif
            result.set(boost::beast::http::field::server,
                       BOOST_BEAST_VERSION_STRING);
            result.set(boost::beast::http::field::content_type,
                       "application/json");
            result.keep_alive(false); //request.keep_alive());
            result.body() = payload;
            result.prepare_payload();
            return result;
        }
        catch (const UWaveServer::WebServer::InvalidPermissionException &e)
        {
            return forbidden(e.what());
        }
        catch (const UWaveServer::WebServer::UnimplementedException &e)
        {
            return unimplemented(e.what());
        }
        catch (const std::invalid_argument &e)
        {
            return badRequest(e.what());
        }
        catch (const std::exception &e)
        {
            return serverError(e.what());
        }
*/
    }
    return serverError("Unhandled method");
}

//------------------------------------------------------------------------------

// Handles an HTTP server connection.
// This uses the Curiously Recurring Template Pattern so that
// the same code works with both SSL streams and regular sockets.
template<class Derived>
class Session
{
public:
    // Take ownership of the buffer
    Session(
        boost::beast::flat_buffer buffer,
        const std::shared_ptr<const std::string> &documentRoot,
        const std::function
        <
            std::string (const boost::beast::http::header
                         <
                             true,
                             boost::beast::http::basic_fields<std::allocator<char>>
                         > &,
                         const std::string &,
                         const boost::beast::http::verb )
        > &callback) :
        mDocumentRoot(documentRoot),
        mBuffer(std::move(buffer)),
        mCallback(callback)
    {
    }

    void doRead()
    {
        // Construct a new parser for each message
        mRequestParser.emplace();
        mRequestParser->body_limit(2048);

        // Set the timeout.
        boost::beast::get_lowest_layer(
            derived().stream()).expires_after(std::chrono::seconds(30));

        // Read a request
        boost::beast::http::async_read(
            derived().stream(),
            mBuffer,
            //mRequest,
            *mRequestParser,
            boost::beast::bind_front_handler(
                &Session::onRead,
                derived().shared_from_this()));
    }
private:
    void onRead(
        boost::beast::error_code errorCode,
        const size_t bytesTransferred)
    {
        boost::ignore_unused(bytesTransferred);

        // This means they closed the connection
        if (errorCode == boost::beast::http::error::end_of_stream)
        {
            return derived().closeConnection();
        }

        if (errorCode)
        {
            if (errorCode != boost::asio::ssl::error::stream_truncated)
            {
                spdlog::critical("Session::onRead read failed with "
                               + std::string {errorCode.what()});
            }
            return;
        }

        // Send the response
        sendResponse(::handleRequest(*mDocumentRoot,
                                     //std::move(mRequest),
                                     mRequestParser->release(),
                                     mCallback));
    }

    void sendResponse(boost::beast::http::message_generator &&message)
    {
        bool keepAlive = message.keep_alive();

        // Write the response
        boost::beast::async_write(
            derived().stream(),
            std::move(message),
            boost::beast::bind_front_handler(
                &Session::onWrite,
                derived().shared_from_this(),
                keepAlive));
    }

    void onWrite(const bool keepAlive,
                 boost::beast::error_code errorCode,
                 const size_t bytesTransferred)
    {
        boost::ignore_unused(bytesTransferred);

        if (errorCode)
        {
            if (errorCode != boost::asio::ssl::error::stream_truncated)
            {
                spdlog::critical("Session::onWrite write failed with "
                               + std::string {errorCode.what()});
            }
            return;
        }

        if (!keepAlive)
        {
            // This means we should close the connection, usually because
            // the response indicated the "Connection: close" semantic.
            return derived().closeConnection();
        }

        // Read another request
        doRead();
    }

protected:
    boost::beast::flat_buffer mBuffer;

private:
    // Access the derived class, this is part of
    // the Curiously Recurring Template Pattern idiom.
    Derived& derived()
    {
        return static_cast<Derived &> (*this);
    }

    std::shared_ptr<const std::string> mDocumentRoot;
    //boost::beast::http::request<boost::beast::http::string_body> mRequest;
    // The parser is stored in an optional container so we can
    // construct it from scratch it at the beginning of each new message.
    boost::optional
    <    
     boost::beast::http::request_parser<boost::beast::http::string_body>
    > mRequestParser;
    std::function<std::string (const boost::beast::http::header
                               <
                                  true,
                                  boost::beast::http::basic_fields<std::allocator<char>>
                               > &,
                               const std::string &,
                               const boost::beast::http::verb)> mCallback;
};

// Handles a plain HTTP connection
class PlainSession : public Session<::PlainSession>,
                     public std::enable_shared_from_this<::PlainSession>
{
public:
    // Create the session
    PlainSession(
        boost::asio::ip::tcp::socket&& socket,
        boost::beast::flat_buffer buffer,
        const std::shared_ptr<const std::string> &documentRoot,
        const std::function
        <
            std::string (const boost::beast::http::header
                         <
                             true,
                             boost::beast::http::basic_fields<std::allocator<char>>
                         > &,
                         const std::string &,
                         const boost::beast::http::verb)
        > &callback) :
        ::Session<::PlainSession>(
            std::move(buffer),
            documentRoot,
            callback),
        mStream(std::move(socket))
    {
    }

    // Called by the base class
    boost::beast::tcp_stream& stream()
    {
        return mStream;
    }

    // Start the asynchronous operation
    void run()
    {
        // We need to be executing within a strand to perform async operations
        // on the I/O objects in this session. Although not strictly necessary
        // for single-threaded contexts, this example code is written to be
        // thread-safe by default.
        boost::asio::dispatch(mStream.get_executor(),
                              boost::beast::bind_front_handler(
                                 &Session::doRead,
                                 shared_from_this()));
    }

    void closeConnection()
    {
        // Send a TCP shutdown
        boost::beast::error_code errorCode;
        mStream.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_send,
                                  errorCode);

        // At this point the connection is closed gracefully
    }
 
private:
    boost::beast::tcp_stream mStream;
};

// Handles an SSL HTTP connection
class SSLSession : public Session<::SSLSession>,
                   public std::enable_shared_from_this<::SSLSession>
{
public:
    // Create the session
    SSLSession(
        boost::asio::ip::tcp::socket &&socket,
        boost::asio::ssl::context &sslContext,
        boost::beast::flat_buffer buffer,
        const std::shared_ptr<const std::string> &documentRoot,
        const std::function
        <
            std::string (const boost::beast::http::header
                         <
                             true,
                             boost::beast::http::basic_fields<std::allocator<char>>
                         > &,
                         const std::string &,
                         const boost::beast::http::verb)
        > &callback) :
        ::Session<::SSLSession>(std::move(buffer),
                                documentRoot,
                                callback),
        mStream(std::move(socket), sslContext)
    {
    }

    // Called by the base class
    boost::beast::ssl_stream<boost::beast::tcp_stream> &stream()
    {
        return mStream;
    }

    // Start the asynchronous operation
    void run()
    {
        auto self = shared_from_this();
        // We need to be executing within a strand to perform async operations
        // on the I/O objects in this session.
        boost::asio::dispatch(mStream.get_executor(), [self]() {
            // Set the timeout.
            boost::beast::get_lowest_layer(self->mStream).expires_after(
                std::chrono::seconds(30));

            // Perform the SSL handshake
            // Note, this is the buffered version of the handshake.
            self->mStream.async_handshake(
                boost::asio::ssl::stream_base::server,
                self->mBuffer.data(),
                boost::beast::bind_front_handler(
                    &::SSLSession::onHandshake,
                    self));
        });
    }

    void onHandshake(boost::beast::error_code errorCode,
                     const size_t bytesUsed)
    {
        if (errorCode)
        {
            if (errorCode != boost::asio::ssl::error::stream_truncated)
            {
                spdlog::critical(
                    "SSLSession::onHandhsake handshake failed with: "
                   + std::string {errorCode.what()});
            }
            return;
        }

        // Consume the portion of the buffer used by the handshake
        mBuffer.consume(bytesUsed);

        doRead();
    }

    void closeConnection()
    {
        // Set the timeout.
        boost::beast::get_lowest_layer(mStream).
            expires_after(std::chrono::seconds(30));

        // Perform the SSL shutdown
        mStream.async_shutdown(
            boost::beast::bind_front_handler(
                &::SSLSession::onShutdown,
                shared_from_this()));
    }

    void onShutdown(boost::beast::error_code errorCode)
    {
        if (errorCode)
        {
            if (errorCode != boost::asio::ssl::error::stream_truncated)
            {
                spdlog::critical("SSLSession::onShutdown failed with: "
                               + std::string {errorCode.what()});
            }
            return;
        }

        // At this point the connection is closed gracefully
    }
private:
    boost::beast::ssl_stream<boost::beast::tcp_stream> mStream;
};

//------------------------------------------------------------------------------

// Detects SSL handshakes
class DetectSession : public std::enable_shared_from_this<::DetectSession>
{
public:
    DetectSession(
        boost::asio::ip::tcp::socket &&socket,
        boost::asio::ssl::context& sslContext,
        const std::shared_ptr<const std::string> &documentRoot,
        const std::function
        <
            std::string (const boost::beast::http::header
                         <
                            true,
                            boost::beast::http::basic_fields<std::allocator<char>>
                         > &,
                         const std::string &,
                         const boost::beast::http::verb request)
        > &callback) :
        mStream(std::move(socket)),
        mSSLContext(sslContext),
        mDocumentRoot(documentRoot),
        mCallback(callback)
    {
    }

    // Launch the detector
    void run()
    {
        // Set the timeout.
        boost::beast::get_lowest_layer(mStream)
           .expires_after(std::chrono::seconds(30));

        // Detect a TLS handshake
        boost::beast::async_detect_ssl(
            mStream,
            mBuffer,
            boost::beast::bind_front_handler(
                &::DetectSession::onDetect,
                shared_from_this()));
    }

    void onDetect(boost::beast::error_code errorCode, const bool result)
    {
        if (errorCode)
        {
            if (errorCode != boost::asio::ssl::error::stream_truncated)
            {
                spdlog::critical(
                   "DetectSession::onDetect: Failed to detect; failed with: "
                  + std::string {errorCode.what()});
            }
            return;
        }

        if (result)
        {
            // Launch SSL session
            std::make_shared<::SSLSession>(
                mStream.release_socket(),
                mSSLContext,
                std::move(mBuffer),
                mDocumentRoot,
                mCallback)->run();
            return;
        }

        // Launch plain session
        std::make_shared<::PlainSession>(
            mStream.release_socket(),
            std::move(mBuffer),
            mDocumentRoot,
            mCallback)->run();
    }

private:
    boost::beast::tcp_stream mStream;
    boost::asio::ssl::context& mSSLContext;
    std::shared_ptr<const std::string> mDocumentRoot;
    boost::beast::flat_buffer mBuffer;
    std::function<std::string (const boost::beast::http::header
                               <
                                   true,
                                   boost::beast::http::basic_fields<std::allocator<char>>
                               > &,
                               const std::string &,
                               const boost::beast::http::verb)> mCallback;
};

}
#endif
