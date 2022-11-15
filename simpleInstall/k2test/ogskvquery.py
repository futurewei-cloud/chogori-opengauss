# pip install msgpack, requests
import argparse, unittest, sys
import types
from skvclient import (CollectionMetadata, CollectionCapacity, SKVClient,
                       HashScheme, StorageDriver, Schema, SchemaField, FieldType, TimeDelta,
                       Operation, Value, Expression, TxnOptions, Txn)
import logging
from ast import literal_eval

def query(coll=None, schema=None, table_oid=None, table_name=None, database=None, txnarg = None):
    if txnarg:
        txn = Txn(cl, literal_eval(txnarg))
    else:
        status, txn = cl.begin_txn()
        if not status.is2xxOK():
            raise Exception(status.message)
    if database:
        coll = get_db_coll(database)
        
    if database and not schema:
        tables = get_tables(db_name=database)
        if table_oid:
            table = [t for t in tables if int(t.data['TableOid']) == int(table_oid)]
        elif table_name:
            table = [t for t in tables if t.data['TableName'].decode() == table_name]
        if not table:
            raise Exception(f"Table with oid {table_oid} not found")
        schema = next(t for t in table).data['TableId'].decode()
        
    status, query = txn.create_query(str.encode(coll), str.encode(schema))
    
    if not status.is2xxOK():
        raise Exception(status.message)
    status, records = txn.queryAll(query)
    if not status.is2xxOK():
        raise Exception(status.message)
    return records

def get_schema(coll, schema_name,  table_oid=None, table_name=None, database=None):
    if database:
        coll = get_db_coll(database)

    if database and not schema_name:
        tables = get_tables(db_name=database)
        if table_oid:
            table = [t for t in tables if int(t.data['TableOid']) == int(table_oid)]
        elif table_name:
            table = [t for t in tables if t.data['TableName'].decode() == table_name]
        if not table:
            raise Exception(f"Table with oid {table_oid} not found")
        schema_name = next(t for t in table).data['TableId'].decode()
    status, schema = cl.get_schema(str.encode(coll), str.encode(schema_name))
    if not status.is2xxOK():
        raise Exception(status.message)
    return schema

def print_schema(schema):
    for (k, v) in schema.__dict__.items():
        if k == 'fields':
            print(f'{k}:')
            for item in v:
                print(f' {item.__dict__}')
        else:
            print(f'{k}:{v}')
                
def get_clusters():
    return query(coll="K2RESVD_COLLECTION_SQL_PRIMARY_CLUSTER", schema="K2RESVD_SCHEMA_SQL_CLUSTER_META")

def get_databases():
    return query(coll="K2RESVD_COLLECTION_SQL_PRIMARY_CLUSTER", schema="K2RESVD_SCHEMA_SQL_DATABASE_META")

def get_db_coll(db_name):
    dbs = get_databases()
    if dbs:
        db = next(d for d in dbs if d.data['DatabaseName'] == str.encode(db_name))
    else:
        db = None
    if db is None:
        raise Exception(f"Database {db_name} not found")
    return db.data['DatabaseId'].decode()
    
def get_tables(coll=None, db_name=None):
    if not coll and db_name:
        coll = get_db_coll(db_name)
    return query(coll=coll, schema="K2RESVD_SCHEMA_SQL_TABLE_META")

def print_records(records):
    for r in records:
        print(r.data)
            
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("command", help="Command")
    parser.add_argument("--http", default="http://172.17.0.1:30000", help="HTTP API URL")
    parser.add_argument("--coll", default="", help="Collection name")
    parser.add_argument("--schema", default="", help="Schema name")
    parser.add_argument("--db", default="template1", help="Database name")
    parser.add_argument("--toid", help="Table oid")
    parser.add_argument("--table", help="Table name")
    parser.add_argument("--txn")
    
    args = parser.parse_args()
    cl = SKVClient(args.http)
    if args.command == "query":
        records = query(coll=args.coll, schema=args.schema,
                        txnarg=args.txn, table_oid=args.toid, table_name=args.table,
                        database=args.db)
        print_records(records)
    elif args.command == "get-schema":
        schema = get_schema(coll=args.coll, schema_name=args.schema,
                            table_oid=args.toid, table_name=args.table,
                            database=args.db)
        print_schema(schema)
    elif args.command == "get-clusters":
        clusters = get_clusters()
        print_records(clusters)
    elif args.command == "get-databases":
        databases = get_databases()
        print_records(databases)
    elif args.command == "get-tables":
        tables = get_tables(coll=args.coll, db_name=args.db)
        print_records(tables)
    else:
        raise Exception(f"Invalid command {args.command}")

