subdir = src/gausskernel/storage/access/k2
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif

SUBDIRS     = catalog

OBJS = k2pg-internal.o k2pg_util.o k2pg_aux.o pg_gate_thread_local.o pg_gate_api.o k2catam.o k2cat_cmds.o \
   k2_plan.o k2_table_ops.o k2_index_ops.o k2_bootstrap.o status.o session.o config.o pg_ids.o pg_memctx.o \
   pg_schema.o pg_session.o pg_statement.o pg_tabledesc.o storage.o k2_util.o

include $(top_srcdir)/src/gausskernel/common.mk
