subdir = src/gausskernel/storage/k2/pggate/catalog
top_builddir = ../../../../../..
include $(top_builddir)/src/Makefile.global

OBJ_DIR = ./obj

OBJS = $(OBJ_DIR)/cluster_info_handler.o $(OBJ_DIR)/database_info_handler.o $(OBJ_DIR)/sql_catalog_client.o $(OBJ_DIR)/sql_catalog_defaults.o $(OBJ_DIR)/sql_catalog_manager.o $(OBJ_DIR)/table_info_handler.o

DEPS = $(OBJ_DIR)/cluster_info_handler.d $(OBJ_DIR)/database_info_handler.d $(OBJ_DIR)/sql_catalog_client.d $(OBJ_DIR)/sql_catalog_defaults.d $(OBJ_DIR)/sql_catalog_manager.d $(OBJ_DIR)/table_info_handler.d

include $(top_srcdir)/src/gausskernel/common.mk

$(OBJ_DIR)/%.o: %.cc
	$(COMPILE.cpp) -MMD -MP -MF"$(patsubst %.o,%.d,$@)" -MT"$@" -o $@ $<

$(OBJS): | buildrepo

clean:
	rm -rf $(OBJ_DIR)

buildrepo:
	@$(call make-repo)

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

define make-repo
   mkdir -p $(OBJ_DIR);
endef
