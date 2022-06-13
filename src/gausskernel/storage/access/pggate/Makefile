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

OBJS = k2pg-internal.o k2pg_util.o k2pg_aux.o slice.o status.o pg_gate_thread_local.o pg_gate_api.o k2_expr.o k2_type.o k2catam.o k2cat_cmds.o k2_plan.o k2_table_ops.o k2_index_ops.o

include $(top_srcdir)/src/gausskernel/common.mk
