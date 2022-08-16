subdir = src/gausskernel/storage/k2/common
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS     = type

OBJ_DIR = ./obj
OBJS = $(OBJ_DIR)/malloc.o $(OBJ_DIR)/common-utils.o $(OBJ_DIR)/errno.o $(OBJ_DIR)/flag_tags.o $(OBJ_DIR)/k2pg-internal.o $(OBJ_DIR)/pgsql_error.o $(OBJ_DIR)/status.o $(OBJ_DIR)/stol_utils.o $(OBJ_DIR)/transaction_error.o

DEPS =  $(OBJS:.o=.d)

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
