cmake_minimum_required(VERSION 3.28)
project(uWaveServer VERSION 0.1.0 LANGUAGES CXX)
enable_testing()

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

configure_file(${CMAKE_SOURCE_DIR}/src/private/version.hpp.in
               ${CMAKE_SOURCE_DIR}/include/uWaveServer/version.hpp)

set(LIBRARY_SRC
    src/dataClient/dataClient.cpp
    src/dataClient/streamSelector.cpp
    src/deduplicator.cpp
    src/packet.cpp
    src/version.cpp
    src/database/client.cpp
    src/database/connection/postgresql.cpp)
if (${SEEDLINK_FOUND})
   message("Found SEEDLink")
   set(LIBRARY_SRC ${LIBRARY_SRC}
       src/dataClient/seedLink.cpp
       src/dataClient/seedLinkOptions.cpp)
endif()

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
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
                           PUBLIC $<INSTALL_INTERFACE:${CMAKE_SOURCE_DIR}/include/uWaveServer>)
if (${SEEDLINK_FOUND})
   target_link_libraries(uWaveServer PRIVATE ${SEEDLINK_LIBRARY})
   target_include_directories(uWaveServer PRIVATE ${SEEDLINK_INCLUDE_DIR})
endif()


add_executable(uwsDataLoader
               src/main.cpp)
set_target_properties(uwsDataLoader PROPERTIES
                      CXX_STANDARD 20
                      CXX_STANDARD_REQUIRED YES 
                      CXX_EXTENSIONS NO) 
target_include_directories(uwsDataLoader
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(uwsDataLoader
                      PRIVATE uWaveServer spdlog::spdlog Boost::program_options)# SOCI::Core SOCI::PostgreSQL)

add_executable(uwsWebServer
               src/webServer.cpp)
set_target_properties(uwsWebServer PROPERTIES
                      CXX_STANDARD 20
                      CXX_STANDARD_REQUIRED YES 
                      CXX_EXTENSIONS NO) 
target_include_directories(uwsWebServer
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

