function(cat IN_FILE OUT_FILE)
    file(READ ${IN_FILE} CONTENTS)
    file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction()

if(MSVC)
    set(hardfork_hpp_file "${CMAKE_CURRENT_SOURCE_DIR}/include/steemit/${CURRENT_TARGET}/hardfork.hpp")
else(MSVC)
    set(hardfork_hpp_file "${CMAKE_CURRENT_BINARY_DIR}/include/steemit/${CURRENT_TARGET}/hardfork.hpp")
endif(MSVC)

file(WRITE ${hardfork_hpp_file} "")
file(GLOB HARDFORK_FILES "${CURRENT_DIRECTORY}/hardfork.d/*.hf")
foreach(ITERATOR ${HARDFORK_FILES})
    cat(${ITERATOR} ${hardfork_hpp_file})
endforeach()