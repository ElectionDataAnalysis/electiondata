#!/usr/bin/python3
# under construction

import psycopg2
from psycopg2 import sql

def get_upsert_id(schema,table,req_var_d,other_var_ds):
    rs = []             # strings for display on web page
    other_ct = len(other_var_ds)
    fnames = [req_var_d['fieldname'] + [d['fieldname'] for d in other_var_ds]
    id_slots = ['{'+str(i)+'}' for i in range(2, other_ct + 2]
    var_slots = ','.join(['%s'] * other_ct)     # this doesn't check types; ***
    q = 'WITH input_rows('+','.join(id_slots)+') AS (VALUES ('+var_slots+') ), ins AS (INSERT INTO {0}.{1}} ('+','.join(id_slots)+') SELECT * FROM input_rows ON CONFLICT ({2}) DO NOTHING RETURNING id, '+id_slots+') SELECT \'i\' AS source, id, '+','.join(id_slots)+' FROM ins UNION  ALL SELECT \'s\' AS source, c.id, '+  ','.join(['c.'+i for i in id_slots])  +' FROM input_rows JOIN {0}.{1} AS c USING ('+ req_var_d['fieldname']+');'
    sql_ids = [schema,table] + fnames
    format_args = [sql.Identifier(x) for x in sql_ids]
    strs = [req_var_d['value']] + [  d['value'] for d in other_var_ds]

    cur.execute(sql.SQL(q).format( *format_args ),strs)

    return('</p><p>'.rs)


req_var_d= {'fieldname':'name','datatype':'text','value':'North Carolina General Election 2018-11-06'}

other_var_ds = [{'fieldname':'enddate','datatype':'DATE','value':'2018-11-06'}, {'fieldname':'electiontype_id','datatype':'INTEGER','value':'1'}, {'fieldname':'other_type','datatype':'TEXT','value':''}]

