# storage/k2/Makefile

MODULE_big = k2_fdw
EXTENSION = k2_fdw
REGRESS = k2_fdw
DATA = k2_fdw--1.0.sql k2_fdw.control
# SHLIB_LINK = -lfmt
subdir = src/gausskernel/storage/k2
top_builddir = ../../../..

include $(top_builddir)/src/Makefile.global

SUBDIRS     = common entities utf pggate

OBJ_DIR = ./obj
OBJS = $(OBJ_DIR)/k2_fdw.o $(OBJ_DIR)/config.o $(OBJ_DIR)/session.o $(OBJ_DIR)/error_reporting.o $(OBJ_DIR)/fdw_handlers.o $(OBJ_DIR)/parse.o
DEPS := $(OBJ_DIR)/k2_fdw.d $(OBJ_DIR)/config.d $(OBJ_DIR)/session.d $(OBJ_DIR)/error_reporting.d $(OBJ_DIR)/fdw_handlers.d $(OBJ_DIR)/parse.d

include $(top_srcdir)/src/gausskernel/common.mk

# exclude_option=-fPIE
override CPPFLAGS := $(filter-out $(exclude_option),$(CPPFLAGS))
override CXXFLAGS += -faligned-new

$(OBJS): | buildrepo

install: install-data

.PHONY: install-data
install-data: installdirs
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(DATA)) '$(DESTDIR)$(datadir)/extension/'
	$(INSTALL_DATA) k2_conf.json '$(DESTDIR)$(datadir)/k2_conf.json.sample'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension'

uninstall: uninstall-data

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)/extension'/, $(notdir $(DATA)))
	rm -f $(DESTDIR)$(datadir)/k2_conf.json.sample

clean:
	rm -rf $(OBJ_DIR)

buildrepo:
	@$(call make-repo)

$(OBJ_DIR)/%.o: %.cpp
	$(COMPILE.cpp) -MMD -MP -MF"$(patsubst %.o,%.d,$@)" -MT"$@" -o $@ $<

show:
	@echo "CC_VERSION=${CC_VERSION}"
	@echo "CC=${CC}"
	@echo
	@echo "DEBUG=${DEBUG}"
	@echo
	@echo "BUILD=${BUILD}"
	@echo
	@echo "SRC_DIRS=${SRC_DIRS}"
	@echo
	@echo "CPPFLAGS=${CPPFLAGS}"
	@echo
	@echo "CFLAGS=${CFLAGS}"
	@echo
	@echo "LDFLAGS=${LDFLAGS}"
	@echo
	@echo "CXXFLAGS=${CXXFLAGS}"
	@echo
	@echo "CPPS=${CPPS}"
	@echo
	@echo "OBJS=${OBJS}"

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

define make-repo
   mkdir -p $(OBJ_DIR);
endef
