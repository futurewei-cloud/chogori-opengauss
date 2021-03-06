import re


class IndexInfo:
    def __init__(self, schema, table, indexname, columns, indexdef):
        self.schema = schema
        self.table = table
        self.indexname = indexname
        self.columns = columns
        self.indexdef = indexdef
        self.primary_key = False
        self.redundant_obj = []


class ExecuteFactory:
    def __init__(self, dbname, user, password, host, port, schema, multi_node, max_index_storage):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.schema = schema
        self.max_index_storage = max_index_storage
        self.multi_node = multi_node

    @staticmethod
    def record_redundant_indexes(cur_table_indexes, redundant_indexes):
        cur_table_indexes = sorted(cur_table_indexes,
                                   key=lambda index_obj: len(index_obj.columns.split(',')))
        # record redundant indexes
        for pos, index in enumerate(cur_table_indexes[:-1]):
            is_redundant = False
            for candidate_index in cur_table_indexes[pos + 1:]:
                if re.match(r'%s' % index.columns, candidate_index.columns):
                    is_redundant = True
                    index.redundant_obj.append(candidate_index)
            if is_redundant:
                redundant_indexes.append(index)

    @staticmethod
    def get_valid_indexes(record, hypoid_table_column, valid_indexes):
        tokens = record.split(' ')
        for token in tokens:
            if 'btree' in token:
                hypo_index_id = re.search(r'\d+', token.split('_', 1)[0]).group()
                table_columns = hypoid_table_column.get(hypo_index_id)
                if not table_columns:
                    continue
                table_name, columns = table_columns.split(':')
                if table_name not in valid_indexes.keys():
                    valid_indexes[table_name] = []
                if columns not in valid_indexes[table_name]:
                    valid_indexes[table_name].append(columns)

    @staticmethod
    def record_ineffective_negative_sql(candidate_index, obj, ind):
        cur_table = candidate_index.table
        if cur_table not in obj.statement and \
                re.search(r'(\.%s\s)' % cur_table.split('.')[-1], obj.statement.lower()):
            return
        if any(re.match(r'(insert\sinto\s%s\s)' % table, obj.statement.lower())
               for table in [cur_table, cur_table.split('.')[-1]]):
            candidate_index.insert_sql_num += obj.frequency
            candidate_index.negative_pos.append(ind)
            candidate_index.total_sql_num += obj.frequency
        elif any(re.match(r'(delete\sfrom\s%s\s)' % table, obj.statement.lower())
                 for table in [cur_table, cur_table.split('.')[-1]]):
            candidate_index.delete_sql_num += obj.frequency
            candidate_index.negative_pos.append(ind)
            candidate_index.total_sql_num += obj.frequency
        elif any(re.match(r'(update\s%s\s)' % table, obj.statement.lower())
                 for table in [cur_table, cur_table.split('.')[-1]]):
            candidate_index.update_sql_num += obj.frequency
            # the index column appears in the UPDATE set condition, the statement is negative
            if any(column in obj.statement.lower().split('where ', 1)[0] for column in
                   candidate_index.columns.split(',')):
                candidate_index.negative_pos.append(ind)
            else:
                candidate_index.ineffective_pos.append(ind)
            candidate_index.total_sql_num += obj.frequency
        elif cur_table in obj.statement or \
                re.search(r'(\s%s\s)' % cur_table.split('.')[-1], obj.statement.lower()):
            candidate_index.select_sql_num += obj.frequency
            # SELECT scenes to filter out positive
            if ind not in candidate_index.positive_pos and \
                    any(column in obj.statement.lower() for column in candidate_index.columns):
                candidate_index.ineffective_pos.append(ind)
            candidate_index.total_sql_num += obj.frequency

    @staticmethod
    def match_last_result(table_name, index_column, history_indexes, history_invalid_indexes):
        for column in history_indexes.get(table_name, dict()):
            if re.match(r'%s' % column, index_column):
                history_indexes[table_name].remove(column)
                history_invalid_indexes[table_name] = history_invalid_indexes.get(table_name, list())
                history_invalid_indexes[table_name].append(column)
                if not history_indexes[table_name]:
                    del history_indexes[table_name]

