cmake_minimum_required(VERSION 3.28)
project(uWaveServer VERSION 0.1.0 LANGUAGES CXX)
enable_testing()

option(ENABLE_SSL "Enable SSL connections for the web server backend" ON) 
option(WITH_CORS "Compile with CORS * for the web server backend" OFF)

include(FetchContent)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

# Boost
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()

find_package(spdlog REQUIRED)
find_package(SOCI COMPONENTS Core PostgreSQL REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(Boost COMPONENTS program_options date_time REQUIRED)
find_package(MiniSEED REQUIRED)
find_package(SEEDLink REQUIRED)
find_package(Earthworm)
if (${ENABLE_SSL})
   find_package(OpenSSL COMPONENTS SSL Crypto REQUIRED)
else()
   find_package(OpenSSL COMPONENTS SSL Crypto)
endif()
if (${OPENSSL_FOUND})
   message("Found SSL version ${OPENSSL_VERSION}")
endif()


configure_file(${CMAKE_SOURCE_DIR}/lib/private/version.hpp.in
               ${CMAKE_SOURCE_DIR}/include/uWaveServer/version.hpp)

set(LIBRARY_SRC
    lib/dataClient/dataClient.cpp
    lib/dataClient/streamSelector.cpp
    lib/packet.cpp
    lib/packetSanitizer.cpp
    lib/packetSanitizerOptions.cpp
    lib/version.cpp
    lib/database/client.cpp
    lib/database/connection/postgresql.cpp)
if (${SEEDLINK_FOUND})
   message("Using SEEDLink")
   set(LIBRARY_SRC ${LIBRARY_SRC}
       lib/dataClient/seedLink.cpp
       lib/dataClient/seedLinkOptions.cpp)
endif()
#if (${Earthworm_FOUND})
#   message("Using Earthworm")
#   target_compile_definitions(uWaveServer WITH_EARTHWORM)
#endif()

set(BUILD_SHARED_LIBS YES)
add_library(uWaveServer ${LIBRARY_SRC})
set_target_properties(uWaveServer PROPERTIES
                      CXX_STANDARD 20
                      CXX_STANDARD_REQUIRED YES
                      CXX_EXTENSIONS NO)
target_link_libraries(uWaveServer
                      PRIVATE spdlog::spdlog SOCI::soci_core SOCI::soci_postgresql 
                              ${MINISEED_LIBRARY})
target_include_directories(uWaveServer
                           PRIVATE ${MINISEED_INCLUDE_DIR} ${PostgreSQL_INCLUDE_DIRS}
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
                           PUBLIC $<INSTALL_INTERFACE:${CMAKE_SOURCE_DIR}/include/uWaveServer>)
if (${SEEDLINK_FOUND})
   target_link_libraries(uWaveServer PRIVATE ${SEEDLINK_LIBRARY})
   target_include_directories(uWaveServer PRIVATE ${SEEDLINK_INCLUDE_DIR})
   target_compile_definitions(uWaveServer PRIVATE WITH_SEEDLINK)
endif()
if (${Earthworm_FOUND})
#   target_link_libraries(uWaveServer PRIVATE ${EARTHWORM_UTILITY_LIBRARY} ${EARTHWORM_MT_LIBRARY})
#   target_include_directories(uWaveServer PRIVATE ${EARTHWORM_INCLUDE_DIR})
    target_compile_definitions(uWaveServer PRIVATE WITH_EARTHWORM)
endif()

add_executable(uwsDataLoader
               src/main.cpp)
set_target_properties(uwsDataLoader PROPERTIES
                      CXX_STANDARD 20
                      CXX_STANDARD_REQUIRED YES 
                      CXX_EXTENSIONS NO) 
target_include_directories(uwsDataLoader
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(uwsDataLoader
                      PRIVATE uWaveServer spdlog::spdlog Boost::program_options)# SOCI::Core SOCI::PostgreSQL)

add_executable(uwsWebServer
               src/webServer/main.cpp
               src/webServer/callback.cpp
               src/webServer/listener.cpp)
if (${ENABLE_SSL})
   target_compile_definitions(uwsWebServer PUBLIC WITH_OPENSSL)
   target_link_libraries(uwsWebServer PRIVATE OpenSSL::SSL OpenSSL::Crypto)
endif()
set_target_properties(uwsWebServer PROPERTIES
                      CXX_STANDARD 20
                      CXX_STANDARD_REQUIRED YES 
                      CXX_EXTENSIONS NO) 
target_include_directories(uwsWebServer
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(uwsWebServer
                      PRIVATE uWaveServer spdlog::spdlog Boost::program_options)# SOCI::Core SOCI::PostgreSQL)

##########################################################################################
#                                     CPACK Packaging                                    #
##########################################################################################
set(CPACK_PACKAGE_NAME ${PROJECT_NAME})
set(CPACK_PACKAGE_VENDOR "UUSS")
set(CPACK_PACKAGE_CONTACT "ben.baker@utah.edu")
set(CPACK_PACKAGE_LICENSE "MIT")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "A set of services for interacting with a TimeScaleDB-enabled Postgres database for storing seismic data at UUSS.")
set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CPACK_PACKAGE_NAME})
set(CPACK_VERBATIM_VARIABLES YES)
set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE")
set(CPACK_COMPONENTS_ALL executables)
if (WIN32)
    set(CPACK_GENERATOR ZIP WIX)
elseif (APPLE)
    set(CPACK_GENERATOR TGZ productbuild)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(CPACK_GENERATOR TGZ RPM)
else()
    set(CPACK_GENERATOR TGZ)
endif()
set(CPACK_SOURCE_IGNORE_FILES
  /\\.git/
  \\.swp
  \\.orig
  /CMakeLists\\.txt\\.user
  /private/
)
include(CPack) # Put this last!

