subdir = src/gausskernel/storage/access/pggate
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif

OBJS = k2pg-internal.o k2pg_util.o slice.o status.o pg_gate_api.o

include $(top_srcdir)/src/gausskernel/common.mk
