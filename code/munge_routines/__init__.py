#!/usr/bin/python3
# munge_routines/__init__.py
# under construction

import psycopg2
from psycopg2 import sql
from datetime import datetime


def get_upsert_id(schema,table,req_var_ds,other_var_ds,con,cur):
    ''' each rvd in req_var_ds and each ovd in other_var_ds should have keys "fieldname","datatype" and "value".
    Returns a triple (id_number,req_var,status), where status is "inserted" or "selected" to indicate whether record existed already in the db or not.'''
    fnames = [d['fieldname'] for d in req_var_ds] + [d['fieldname'] for d in other_var_ds]
    r_id_slots = ['{'+str(i)+'}' for i in range(2, len (req_var_ds) + 2)]
    o_id_slots = ['{'+str(i)+'}' for i in range (2 + len(req_var_ds), 2 + len(req_var_ds)+ len(other_var_ds))]
    # id_slots = ['{'+str(i)+'}' for i in range(2, len(req_var_ds) + len(req_var_ds) + 3)]
    
    
    ## *** kludge: need to omit datatype for INTEGER, not sure why. ***
    for rvd in req_var_ds:
        if rvd['datatype'] == 'INTEGER': rvd['datatype'] = ''
    for ovd in other_var_ds:
        if ovd['datatype'] == 'INTEGER': ovd['datatype'] = ''
    ## end of kludge ***
    
    vars = [rvd['datatype'] + ' %s' for rvd in req_var_ds]+[ovd['datatype'] + ' %s'   for ovd in other_var_ds]
    var_slots = ','.join(vars)
    q = 'WITH input_rows('+','.join(r_id_slots+o_id_slots)+') AS (VALUES ('+var_slots+') ), ins AS (INSERT INTO {0}.{1} ('+','.join(r_id_slots+o_id_slots)+') SELECT * FROM input_rows ON CONFLICT ('+','.join(r_id_slots)+') DO NOTHING RETURNING "Id", '+','.join(r_id_slots+o_id_slots)+') SELECT "Id", '+','.join(r_id_slots+o_id_slots)+', \'inserted\' AS source FROM ins UNION  ALL SELECT c."Id", '+  ','.join(['c.'+i for i in r_id_slots+o_id_slots])  +',\'selected\' AS source FROM input_rows JOIN {0}.{1} AS c USING ('+ ','.join(r_id_slots+o_id_slots)+');'
    sql_ids = [schema,table] + fnames
    format_args = [sql.Identifier(x) for x in sql_ids]
    strs = [rvd['value'] for rvd in req_var_ds] + [  d['value'] for d in other_var_ds]

    cur.execute(sql.SQL(q).format( *format_args ),strs)
    a =  list( cur.fetchall()[0])       # returns a single id
    
    con.commit()
    return(a)

def format_type_for_insert(schema,table,txt,con,cur):
    ''' schema.table must have a "txt" field. This function creates a (type_id, othertype_text) pair; for types in the enumeration, returns (type_id for the given txt, ""), while for other types returns (type_id for "other",txt)  '''
    q = 'SELECT "Id" FROM {}.{} WHERE txt = %s'
    sql_ids = [schema,table]
    cur.execute(   sql.SQL(q).format( sql.Identifier(schema),sql.Identifier(table)),[txt])
    a = cur.fetchall()
    if a:
        return([a[0][0],''])
    else:
        cur.execute(   sql.SQL(q).format( sql.Identifier(schema),sql.Identifier(table)),['other'])
        a = cur.fetchall()
        return([a[0][0],txt])



