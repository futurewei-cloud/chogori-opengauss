subdir = src/gausskernel/storage/k2/entities
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

OBJ_DIR = ./obj
OBJS = $(OBJ_DIR)/entity_ids.o $(OBJ_DIR)/schema.o $(OBJ_DIR)/types.o $(OBJ_DIR)/index.o $(OBJ_DIR)/type.o $(OBJ_DIR)/expr.o $(OBJ_DIR)/table.o $(OBJ_DIR)/value.o

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
