subdir = src/gausskernel/storage/k2/pggate
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS     = catalog

OBJ_DIR = ./obj
OBJS = $(OBJ_DIR)/k2_adapter.o $(OBJ_DIR)/k2_config.o $(OBJ_DIR)/k2_gate.o $(OBJ_DIR)/k2_txn.o $(OBJ_DIR)/pg_column.o $(OBJ_DIR)/pg_ddl.o $(OBJ_DIR)/pg_dml.o $(OBJ_DIR)/pg_dml_read.o $(OBJ_DIR)/pg_dml_write.o $(OBJ_DIR)/pg_gate_api.o $(OBJ_DIR)/pg_gate_impl.o $(OBJ_DIR)/pg_gate_thread_local_vars.o $(OBJ_DIR)/pg_memctx.o $(OBJ_DIR)/pg_op.o $(OBJ_DIR)/pg_op_api.o $(OBJ_DIR)/pg_select.o $(OBJ_DIR)/pg_session.o $(OBJ_DIR)/pg_statement.o $(OBJ_DIR)/pg_tabledesc.o $(OBJ_DIR)/pg_tuple.o $(OBJ_DIR)/pg_txn_handler.o

DEPS = $(OBJ_DIR)/k2_adapter.d $(OBJ_DIR)/k2_config.d $(OBJ_DIR)/k2_gate.d $(OBJ_DIR)/k2_seastar_app.d $(OBJ_DIR)/k2_txn.d $(OBJ_DIR)/pg_column.d $(OBJ_DIR)/pg_ddl.d $(OBJ_DIR)/pg_dml.d $(OBJ_DIR)/pg_dml_read.d $(OBJ_DIR)/pg_dml_write.d $(OBJ_DIR)/pg_gate_api.d $(OBJ_DIR)/pg_gate_impl.d $(OBJ_DIR)/pg_gate_thread_local_vars.d $(OBJ_DIR)/pg_memctx.d $(OBJ_DIR)/pg_op.d $(OBJ_DIR)/pg_op_api.d $(OBJ_DIR)/pg_select.d $(OBJ_DIR)/pg_session.d $(OBJ_DIR)/pg_statement.d $(OBJ_DIR)/pg_tabledesc.d $(OBJ_DIR)/pg_tuple.d $(OBJ_DIR)/pg_txn_handler.d


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
