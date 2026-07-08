cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(TARGET_NAME tierdb)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include(FetchContent)

FetchContent_Declare(
    Corrosion
    GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG v0.5
)
FetchContent_MakeAvailable(Corrosion)

corrosion_import_crate(MANIFEST_PATH tierdb-core-ffi/Cargo.toml)

include_directories(src/include)
include_directories(duckdb/third_party/yyjson/include)

set(EXTENSION_SOURCES
    src/tierdb_extension.cpp
    src/storage/tierdb_client.cpp
    src/storage/tierdb_catalog.cpp
    src/storage/tierdb_insert.cpp
    src/storage/tierdb_delete.cpp
    src/storage/tierdb_update.cpp
    src/storage/tierdb_schema.cpp
    src/storage/tierdb_json.cpp
    src/storage/tierdb_table.cpp
    src/storage/tierdb_storage.cpp
    src/storage/tierdb_transaction.cpp
    src/storage/tierdb_transaction_manager.cpp
    duckdb/third_party/yyjson/yyjson.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} tierdb_core_ffi-static)
target_link_libraries(${LOADABLE_EXTENSION_NAME} tierdb_core_ffi)

if(APPLE)
    set(TIERDB_APPLE_FRAMEWORKS "-framework CoreFoundation" "-framework SystemConfiguration")
    target_link_libraries(${EXTENSION_NAME} ${TIERDB_APPLE_FRAMEWORKS})
    target_link_libraries(${LOADABLE_EXTENSION_NAME} ${TIERDB_APPLE_FRAMEWORKS})
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
