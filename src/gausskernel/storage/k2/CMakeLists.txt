#This is the main CMAKE for build all k2.
# k2
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_k2_SRC)
set(TGT_k2_INC ${PROJECT_SRC_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR})
set(k2_DEF_OPTIONS ${MACRO_OPTIONS})
set(k2_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(k2_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(k2 TGT_k2_SRC TGT_k2_INC "${k2_DEF_OPTIONS}" "${k2_COMPILE_OPTIONS}" "${k2_LINK_OPTIONS}")
set_target_properties(k2 PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/k2.control
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/k2--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS k2 DESTINATION lib/postgresql)


install(FILES ${PROJECT_SRC_DIR}/gausskernel/storage/k2/k2_conf.json
    DESTINATION share/postgresql
    RENAME k2_conf.json.sample
)
