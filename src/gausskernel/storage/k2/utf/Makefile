subdir = src/gausskernel/storage/k2/utf
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

OBJ_DIR = ./obj
OBJS = $(OBJ_DIR)/rune.o

include $(top_srcdir)/src/gausskernel/common.mk

$(OBJ_DIR)/%.o: %.c
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
