subdir = src/gausskernel/storage/access/k2/catalog
top_builddir = ../../../../../..
include $(top_builddir)/src/Makefile.global

OBJ_DIR = ./obj
OBJS = $(OBJ_DIR)/k2_catalog_defaults.o $(OBJ_DIR)/database_info_handler.o $(OBJ_DIR)/table_info_handler.o  $(OBJ_DIR)/cluster_info_handler.o $(OBJ_DIR)/sql_catalog_manager.o $(OBJ_DIR)/sql_catalog_client.o

DEPS =  $(OBJS:.o=.d)

include $(top_srcdir)/src/gausskernel/common.mk


$(OBJ_DIR)/%.o: %.cpp
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
