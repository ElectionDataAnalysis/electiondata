#!/usr/bin/python3
# under construction

import psycopg2
from psycopg2 import sql
from datetime import datetime


def get_upsert_id(schema,table,req_var_d,other_var_ds,con,cur):
    other_ct = len(other_var_ds)
    fnames = [req_var_d['fieldname']] + [d['fieldname'] for d in other_var_ds]
    id_slots = ['{'+str(i)+'}' for i in range(2, other_ct + 3)]
    vars = [req_var_d['datatype'] + ' %s']+[d['datatype'] + ' %s'   for d in other_var_ds]
    var_slots = ','.join(vars)
    q = 'WITH input_rows('+','.join(id_slots)+') AS (VALUES ('+var_slots+') ), ins AS (INSERT INTO {0}.{1} ('+','.join(id_slots)+') SELECT * FROM input_rows ON CONFLICT ({2}) DO NOTHING RETURNING id, '+','.join(id_slots)+') SELECT \'inserted\' AS source, id, '+','.join(id_slots)+' FROM ins UNION  ALL SELECT \'selected\' AS source, c.id, '+  ','.join(['c.'+i for i in id_slots])  +' FROM input_rows JOIN {0}.{1} AS c USING ('+ req_var_d['fieldname']+');'
    sql_ids = [schema,table] + fnames
    format_args = [sql.Identifier(x) for x in sql_ids]
    strs = [req_var_d['value']] + [  d['value'] for d in other_var_ds]

    cur.execute(sql.SQL(q).format( *format_args ),strs)
    a = cur.fetchall()
    con.commit()
    return(a)

def format_type_for_insert(schema,table,txt,con,cur):
    ''' schema.table must have a "txt" field. This function creates a (type_id, othertype_text) pair; for types in the enumeration, returns (type_id for the given txt, ""), while for other types returns (type_id for "other",txt)  '''
    q = 'SELECT "id" FROM {}.{} WHERE "txt" = %s'
    sql_ids = [schema,table]
    cur.execute(   sql.SQL(q).format( sql.Identifier(schema),sql.Identifier(table)),[txt])
    a = cur.fetchall()
    if a:
        return([a[0][0],''])
    else:
        cur.execute(   sql.SQL(q).format( sql.Identifier(schema),sql.Identifier(table)),['other'])
        a = cur.fetchall()
        return([a[0][0],txt])



