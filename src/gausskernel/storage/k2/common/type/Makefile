subdir = src/gausskernel/storage/k2/common/type
top_builddir = ../../../../../..
include $(top_builddir)/src/Makefile.global

OBJ_DIR = ./obj
OBJS = $(OBJ_DIR)/varint.o $(OBJ_DIR)/decimal.o $(OBJ_DIR)/int128.o $(OBJ_DIR)/slice.o

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
