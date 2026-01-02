cmake_minimum_required(VERSION 3.28)
project(uWaveServer VERSION 0.2.0 LANGUAGES CXX)
enable_testing()

option(ENABLE_SSL "Enable SSL connections for the web server backend" ON) 
option(ENABLE_COMPRESSION "Enable copmression for the web server backend" ON)
#option(WITH_CORS "Compile with CORS * for the web server backend" OFF)
option(BUILD_SHARED_LIBS "Build using shared libraries" OFF)

include(FetchContent)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

# Boost
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()
if (NOT BUILD_SHARED_LIBS)
   message("Will attempt to use Boost static libraries")
   set(Boost_USE_STATIC_LIBS ON)
endif()
if (${ENABLE_COMPRESSION})
   set(CROW_ENABLE_COMPRESSION ON)
else()
   set(CROW_ENABLE_COMPRESSION OFF)
endif()
find_package(spdlog REQUIRED)
#find_package(SOCI COMPONENTS Core PostgreSQL REQUIRED)
#find_package(PostgreSQL REQUIRED)
#find_package(nlohmann_json 3.8.0 REQUIRED)

find_package(Boost COMPONENTS program_options url headers REQUIRED)
find_package(libmseed REQUIRED)
find_package(SEEDLink REQUIRED)
find_package(libpqxx REQUIRED)
find_package(ZLIB REQUIRED)
find_package(opentelemetry-cpp REQUIRED)
find_package(Catch2 3)
find_package(Crow)
if (${ENABLE_SSL})
   if (NOT BUILD_SHARED_LIBS)
      message("Will use static SSL libraries")
      set(OPENSSL_USE_STATIC_LIBS TRUE)
   endif()
   find_package(OpenSSL COMPONENTS SSL Crypto REQUIRED)
else()
   if (NOT BUILD_SHARED_LIBS)
      message("Will use static SSL libraries")
      set(OPENSSL_USE_STATIC_LIBS TRUE)
   endif()
   find_package(OpenSSL COMPONENTS SSL Crypto)
endif()
if (${OPENSSL_FOUND})
   message("Found SSL version ${OPENSSL_VERSION}")
   if (${Crow_FOUND})
      set(CROW_ENABLE_SSL ON)
   endif()
endif()


configure_file(${CMAKE_SOURCE_DIR}/lib/private/version.hpp.in
               ${CMAKE_SOURCE_DIR}/include/uWaveServer/version.hpp)

set(LIBRARY_SRC
    lib/dataClient/dataClient.cpp
    lib/dataClient/streamSelector.cpp
    lib/database/writeClient.cpp
    lib/database/readOnlyClient.cpp
    lib/database/credentials.cpp
    lib/testDuplicatePacket.cpp
    lib/testExpiredPacket.cpp
    lib/testFuturePacket.cpp
    lib/packet.cpp
    lib/packetSanitizer.cpp
    lib/packetSanitizerOptions.cpp
    lib/version.cpp
    )
if (${SEEDLink_FOUND})
   message("Using SEEDLink")
   set(LIBRARY_SRC ${LIBRARY_SRC}
       lib/dataClient/seedLink.cpp
       lib/dataClient/seedLinkOptions.cpp)
endif()

if (BUILD_SHARED_LIBS)
   message("Will build shared library")
   add_library(uWaveServer SHARED ${LIBRARY_SRC})
   set_target_properties(uWaveServer PROPERTIES
                         CXX_STANDARD 20
                         CXX_STANDARD_REQUIRED YES 
                         CXX_EXTENSIONS NO) 
else()
   message("Will build static library")
   add_library(uWaveServer STATIC ${LIBRARY_SRC})
   set_target_properties(uWaveServer PROPERTIES
                         CXX_STANDARD 20
                         CXX_STANDARD_REQUIRED YES
                         CXX_EXTENSIONS NO)
   #                      OUTPUT_NAME uWaveServer-static)
endif()
target_sources(uWaveServer
               PUBLIC
               FILE_SET HEADERS
               BASE_DIRS
                  ${CMAKE_CURRENT_SOURCE_DIR}
               FILES 
                  include/uWaveServer/dataClient/dataClient.hpp
                  include/uWaveServer/dataClient/streamSelector.hpp
                  include/uWaveServer/database/writeClient.hpp
                  include/uWaveServer/database/readOnlyClient.hpp
                  include/uWaveServer/database/credentials.hpp
                  include/uWaveServer/testDuplicatePacket.hpp
                  include/uWaveServer/testExpiredPacket.hpp
                  include/uWaveServer/testFuturePacket.hpp
                  include/uWaveServer/packet.hpp
                  include/uWaveServer/packetSanitizer.hpp
                  include/uWaveServer/packetSanitizerOptions.hpp
                  include/uWaveServer/version.hpp)
target_link_libraries(uWaveServer
                      PUBLIC libpqxx::pqxx #nlohmann_json::nlohmann_json libpqxx::pqxx
                      PRIVATE spdlog::spdlog_header_only mseed::mseed_static ZLIB::ZLIB)
#if (${ZLIB_FOUND})
#   target_link_libraries(uWaveServer PRIVATE ZLIB::ZLIB)
   target_compile_definitions(uWaveServer PRIVATE WITH_ZLIB)
#endif()
target_include_directories(uWaveServer
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
                           #PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include/uWaveServer>
                           PUBLIC $<INSTALL_INTERFACE:include>)
if (${SEEDLink_FOUND})
   target_link_libraries(uWaveServer PRIVATE SEEDLink::SEEDLink)
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
                      PRIVATE uWaveServer spdlog::spdlog_header_only Boost::program_options)

if (${Crow_FOUND})
   message("Will build web server")
   add_executable(uHTTPWaveServer
                  src/httpWaveServer.cpp)
   set_target_properties(uHTTPWaveServer PROPERTIES
                         CXX_STANDARD 20
                         CXX_STANDARD_REQUIRED YES
                         CXX_EXTENSIONS NO)
   target_link_libraries(uHTTPWaveServer
                         PRIVATE uWaveServer
                                 Boost::program_options
                                 Crow::Crow
                                 spdlog::spdlog_header_only
                                 opentelemetry-cpp::prometheus_exporter)
   if (${ENABLE_SSL})
      target_link_libraries(uHTTPWaveServer PRIVATE OpenSSL::SSL OpenSSL::Crypto)
   endif()
   if (${ZLIB_FOUND})
      target_link_libraries(uHTTPWaveServer PRIVATE ZLIB::ZLIB)
   endif()
   target_include_directories(uHTTPWaveServer
                              PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
   #                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>)
endif()
#add_executable(uwsWebServer
#               src/webServer/main.cpp
#               src/webServer/callback.cpp
#               src/webServer/listener.cpp)
#if (${ENABLE_SSL})
#   target_compile_definitions(uwsWebServer PUBLIC WITH_OPENSSL)
#   target_link_libraries(uwsWebServer PRIVATE OpenSSL::SSL OpenSSL::Crypto)
#endif()
#set_target_properties(uwsWebServer PROPERTIES
#                      CXX_STANDARD 20
#                      CXX_STANDARD_REQUIRED YES 
#                      CXX_EXTENSIONS NO) 
#target_include_directories(uwsWebServer
#                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>
#                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
#target_link_libraries(uwsWebServer
#                      PRIVATE
#                      uWaveServer spdlog::spdlog_header_only Boost::program_options Boost::url
#                      nlohmann_json::nlohmann_json libmseed::libmseed)# SOCI::Core SOCI::PostgreSQL)

if (${Catch2_FOUND})
   message("Found catch")
   add_executable(unitTests
                  testing/pack.cpp
                  testing/packet.cpp
                  testing/testPacket.cpp
                  #testing/database.cpp
                  )
   set_target_properties(unitTests PROPERTIES
                         CXX_STANDARD 20
                         CXX_STANDARD_REQUIRED YES
                         CXX_EXTENSIONS NO)
   target_link_libraries(unitTests PRIVATE
                         Catch2::Catch2 Catch2::Catch2WithMain
                         uWaveServer spdlog::spdlog_header_only mseed::mseed_static
                         ZLIB::ZLIB)
   target_include_directories(unitTests
                              PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
                              PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>
                              PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>)
   add_test(NAME unitTests 
            COMMAND unitTests)
   #if (${ZLIB_FOUND})
   #   message("Will test libz")
      #target_link_libraries(unitTests PRIVATE ZLIB::ZLIB)
      target_compile_definitions(unitTests PRIVATE WITH_ZLIB)
   #endif()
endif()
#add_executable(binaryTest test/binary.cpp)
#set_target_properties(binaryTest PROPERTIES
#                      CXX_STANDARD 20
#                      CXX_STANDARD_REQUIRED YES
#                      CXX_EXTENSIONS NO)
#target_include_directories(binaryTest
#                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>
#                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
#target_link_libraries(binaryTest PRIVATE uWaveServer spdlog::spdlog_header_only Boost::program_options Boost::url SOCI::Core SOCI::PostgreSQL)

##########################################################################################
#                                      Installation                                      #
##########################################################################################
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
configure_package_config_file(
    cmake/${PROJECT_NAME}Config.cmake.in "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
    VERSION "${version}"
    COMPATIBILITY AnyNewerVersion
)
#install(TARGETS uWaveServer uwsWebServer uwsDataLoader
#        EXPORT ${PROJECT_NAME}-targets
#        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
#        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
#        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
#        COMPONENT Runtime)
install(TARGETS uWaveServer
        EXPORT ${PROJECT_NAME}-targets
        FILE_SET HEADERS
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        COMPONENT libraries)
install(TARGETS uHTTPWaveServer uwsDataLoader
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        COMPONENT applications)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/uWaveServer
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        COMPONENT headers)
export(EXPORT ${PROJECT_NAME}-targets
       FILE "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Targets.cmake")
install(EXPORT ${PROJECT_NAME}-targets
        FILE ${PROJECT_NAME}Targets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME})
install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME})


##########################################################################################
#                                     CPACK Packaging                                    #
##########################################################################################
include(CPackComponent)
set(CPACK_PACKAGE_NAME ${PROJECT_NAME})
set(CPACK_PACKAGE_VENDOR "UUSS")
set(CPACK_PACKAGE_CONTACT "ben.baker@utah.edu")
set(CPACK_PACKAGE_LICENSE "MIT")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "A set of services for interacting with a TimeScaleDB-enabled Postgres database for storing seismic data at UUSS.")
set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CPACK_PACKAGE_NAME})
set(CPACK_VERBATIM_VARIABLES YES)
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE")
cpack_add_component(libraries
                    DISPLAY_NAME "Application common library"
                    DEPENDS ${SEEDLink_LIBRARY} ${ZLIB_LIBRARIES})# ${libmseed_LIBRARY})
cpack_add_component(headers
                    DISPLAY_NAME "libuWaveServer header files"
                    DEPENDS libraries)
cpack_add_component(applications
                    DISPLAY_NAME "uWaveServer Applications"
                    DEPENDS libraries)
set(CPACK_GENERATOR ZIP TGZ)
if (WIN32)
   list(APPEND CPACK_GENERATOR WIX)
elseif (APPLE)
   list(APPEND CPACK_GENERATOR productbuild)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
   find_program(RPMBUILD_PROGRAM rpmbuild)
   if (RPMBUILD_PROGRAM)
      list(APPEND CPACK_GENERATOR RPM)
      #set(CPACK_RPM_PACKAGE_REQUIRES "${CPACK_RPM_PACKAGE_REQUIRES abc >= 123")
   endif()
   if (${BUILD_FOR_DEBIAN})
      find_program(DPKG_PROGRAM dpkg REQUIRED)
   else()
      find_program(DPKG_PROGRAM dpkg)
   endif()
   if (DPKG_PROGRAM)
      list(APPEND CPACK_GENERATOR DEB)
      set(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS}")# libsodium (>= 1.0.20) libzmq (>=4.3.0)")
      set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON) 
   endif()
else()
   message("Unknown operating system")
endif()
message("CPack generators " ${CPACK_GENERATOR})
set(CPACK_SOURCE_IGNORE_FILES
  /\\.git/
  \\.swp
  \\.orig
  /CMakeLists\\.txt\\.user
  /private/
)
include(CPack) # Put this last!

