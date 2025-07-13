cmake_minimum_required(VERSION 3.10)
project(IrisMerge)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Protobuf
find_package(Protobuf REQUIRED)
set(PROTOBUF_GENERATE_CPP_APPEND_PATH TRUE)
set(SUBSTRAIT_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/external/substrait/proto)
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)

file(MAKE_DIRECTORY ${GENERATED_DIR})
file(MAKE_DIRECTORY ${GENERATED_DIR}/substrait)
file(MAKE_DIRECTORY ${GENERATED_DIR}/substrait/extensions)

set(proto_files
    "${SUBSTRAIT_PROTO_DIR}/substrait/algebra.proto"
    "${SUBSTRAIT_PROTO_DIR}/substrait/capabilities.proto"
    "${SUBSTRAIT_PROTO_DIR}/substrait/extended_expression.proto"
    "${SUBSTRAIT_PROTO_DIR}/substrait/function.proto"
    "${SUBSTRAIT_PROTO_DIR}/substrait/parameterized_types.proto"
    "${SUBSTRAIT_PROTO_DIR}/substrait/plan.proto"
    "${SUBSTRAIT_PROTO_DIR}/substrait/type.proto"
    "${SUBSTRAIT_PROTO_DIR}/substrait/type_expressions.proto"
    "${SUBSTRAIT_PROTO_DIR}/substrait/extensions/extensions.proto"
)

execute_process(
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
    --cpp_out=${GENERATED_DIR}
    --proto_path=${SUBSTRAIT_PROTO_DIR}
    ${proto_files}
)

file(GLOB PROTO_SRCS "${GENERATED_DIR}/substrait/*.cc" "${GENERATED_DIR}/substrait/extensions/*.cc")
file(GLOB PROTO_HDRS "${GENERATED_DIR}/substrait/*.h" "${GENERATED_DIR}/substrait/extensions/*.h")


add_executable(iris_merge src/main.cc src/iris_merge.cc ${PROTO_SRCS})

# Include directories
target_include_directories(iris_merge PUBLIC include ${GENERATED_DIR})

# Add Abseil
add_subdirectory(external/abseil-cpp)

# Link against Protobuf
target_link_libraries(iris_merge PRIVATE protobuf::libprotobuf protobuf::libprotobuf-lite absl::statusor)

# Add tests
enable_testing()
add_subdirectory(external/googletest)
add_executable(iris_merge_test test/main_test.cc src/iris_merge.cc ${PROTO_SRCS})
target_include_directories(iris_merge_test PUBLIC include ${GENERATED_DIR})
target_link_libraries(iris_merge_test gtest gtest_main protobuf::libprotobuf protobuf::libprotobuf-lite absl::statusor)

add_test(NAME iris_merge_test COMMAND iris_merge_test)
