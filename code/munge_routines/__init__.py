#!/usr/bin/python3
# under construction


def get_upsert_id(schema,table,req_var_d,other_var_ds):
    rs = []             # strings for display on web page
    slots = ','.join(['{'+str(i)+'}' for i in range(2,len(other_var_ds)+2)])
    fnames = ','.join( req_var_d['fieldname'] + [ d['fieldname'] for d in other_var_ds])
    q = 'WITH input_rows('+slots+') AS (VALUES (text \'NC\') ), ins AS (INSERT INTO {0}.{1}} (name) SELECT * FROM input_rows ON CONFLICT (name) DO NOTHING RETURNING id, name) SELECT \'i\' AS source, id, name FROM ins UNION  ALL SELECT \'s\' AS source, c.id, c.name FROM input_rows JOIN {0}.{1} AS c USING (name);'
        
    cur.execute(sql.SQL(q).format(sql.Identifier(schema),sql.Identifier(table)))

        return('</p><p>'.rs)


req_var_d= {'fieldname':'name','datatype':'text','value':'North Carolina General Election 2018-11-06'}

other_var_ds = [{'fieldname':'enddate','datatype':'DATE','value':'2018-11-06'},
                {'fieldname':'electiontype_id','datatype':'INTEGER','value':'1'},
                {'fieldname':'other_type','datatype':'TEXT','value':''}
    ]
