cmake_minimum_required(VERSION 3.18)

project(sbd_proj_2 C)

include_directories(include)

set(SOURCES main.c record.c idx_seq_file.c)

add_executable(${PROJECT_NAME} ${SOURCES})

target_link_libraries(${PROJECT_NAME} PRIVATE m)
