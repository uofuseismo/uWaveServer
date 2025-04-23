#include <spdlog/spdlog.h>
#include <boost/beast/http/verb.hpp>
#include "listener.hpp"
#include "server.hpp"

using namespace UWaveServer::WebServer;

// Accepts incoming connections and launches the sessions
Listener::Listener(
        boost::asio::io_context& ioContext,
        boost::asio::ssl::context &sslContext,
        boost::asio::ip::tcp::endpoint endpoint, 
        const std::shared_ptr<const std::string> &documentRoot,
        const std::function
        <
            std::string (const boost::beast::http::header<true, boost::beast::http::basic_fields<std::allocator<char> > > &,
                         const std::string &,
                         const boost::beast::http::verb)
        > &callback) :
          mIOContext(ioContext),
          mSSLContext(sslContext),
          mAcceptor(boost::asio::make_strand(ioContext)),
          mDocumentRoot(documentRoot),
          mCallback(callback)
{   
    boost::beast::error_code errorCode;

    // Open the acceptor
    mAcceptor.open(endpoint.protocol(), errorCode);
    if (errorCode)
    {
        if (errorCode != boost::asio::ssl::error::stream_truncated)
        {
            spdlog::critical("Listener failed to open; failed with: "
                           + std::string {errorCode.what()});
        }
        return;
    }

    // Allow address reuse
    mAcceptor.set_option(boost::asio::socket_base::reuse_address(true),
                         errorCode);
    if (errorCode)
    {
        if (errorCode != boost::asio::ssl::error::stream_truncated)
        {
            spdlog::critical(
                "Listener failed to set reuse_address option; failed with: "
               + std::string {errorCode.what()});
        }
        return;
    }

    // Bind to the server address
    mAcceptor.bind(endpoint, errorCode);
    if (errorCode)
    {
        if (errorCode != boost::asio::ssl::error::stream_truncated)
        {
            spdlog::critical("Listener failed to bind; failed with: "
                           + std::string {errorCode.what()});
        }
        return;
    }

    // Start listening for connections
    mAcceptor.listen(
        boost::asio::socket_base::max_listen_connections, errorCode);
    if (errorCode)
    {
        if (errorCode != boost::asio::ssl::error::stream_truncated)
        {
            spdlog::critical("Listener failed to listen; failed with: "
                           + std::string {errorCode.what()});
        }
        return;
    }
}

void Listener::run()
{
    doAccept();
}

void Listener::doAccept()
{
    // The new connection gets its own strand
    mAcceptor.async_accept(
        boost::asio::make_strand(mIOContext),
        boost::beast::bind_front_handler(
           &UWaveServer::WebServer::Listener::onAccept,
            shared_from_this()));
}

void Listener::onAccept(boost::beast::error_code errorCode,
                        boost::asio::ip::tcp::socket socket)
{
    if (errorCode)
    {
        if (errorCode != boost::asio::ssl::error::stream_truncated)
        {
            spdlog::critical("Listener::onAccept: accept failed with: "
                           + std::string {errorCode.what()});
        }
    }
    else
    {
        // Create the detector session and run it
        std::make_shared<::DetectSession>(
            std::move(socket),
            mSSLContext,
            mDocumentRoot,
            mCallback)->run();
    }

    // Accept another connection
    doAccept();
}
