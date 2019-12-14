#!/usr/bin/python3
# under construction
#  *** may need to add "NOT NULL" requirements per CDF

import psycopg2
from psycopg2 import sql


def create_common_data_format_schema (con,cur,schema_name,CDF_specs_path):
    ''' schema_name example: 'cdf'; Creates schema with that name on the given db connection and cursor'''
    rs = ['create_common_data_format_schema (con,cur,'+ schema_name+')']
    # create the blank schema
    cur.execute(sql.SQL('DROP SCHEMA IF EXISTS {0} CASCADE; CREATE SCHEMA {0};').format(sql.Identifier(schema_name)))
    con.commit()
    
    # create a sequence for unique id values across all tables
    cur.execute(sql.SQL('CREATE SEQUENCE {}.id_seq;').format(sql.Identifier(schema_name)))
    con.commit()

    # create enumeration tables
    # *** this worked: enumeration_path = '/container_root_dir/CDF_schema_def_info/enumerations/'
    enumeration_path = CDF_specs_path+'enumerations/'
    for t in ['IdentifierType','CountItemStatus','ReportingUnitType','ElectionType','CountItemType']:
        q = 'DROP TABLE IF EXISTS {0}.{1}; CREATE TABLE {0}.{1} (Id BIGINT DEFAULT nextval(\'{0}.id_seq\') PRIMARY KEY,Txt TEXT UNIQUE NOT NULL); COPY {0}.{1} (txt) FROM %s'
        cur.execute(sql.SQL(q).format(sql.Identifier(schema_name), sql.Identifier(t)),(enumeration_path + t + '.txt',))

    # create all other tables, in set order because of foreign keys
    with open(table_file_path,'r') as f:
        table_ds = eval(f.read())
    for d in table_ds:
        field_defs = ['Id BIGINT DEFAULT nextval(\'{0}.id_seq\') PRIMARY KEY']
        format_args = [schema_name,d['tablename']]
        ctr = 2     # counter to track sql.Identifiers
        for f in d['fields']:
            field_defs.append( '{'+str(ctr)+'} ' + f['datatype'] )
            format_args.append( f['fieldname'])
            ctr += 1
        for e in d['enumerations']:
            field_defs.append( '{'+str(ctr)+'} INTEGER NOT NULL REFERENCES {0}.{'+str(ctr+1)+'}(Id), {'+str(ctr+2)+'} TEXT' )
            format_args.append(e+'_Id')
            format_args.append(e)
            format_args.append('Other'+e)
            ctr += 3
        for other in d['other_element_refs']:
            field_defs.append('{'+str(ctr)+'} INTEGER REFERENCES {0}.{'+str(ctr+1)+'}(Id)')
            format_args.append(other['fieldname'])
            format_args.append(other['refers_to'])
            ctr += 2
        for c in d['constraints']:
            field_defs.append(c)
            
        q = 'DROP TABLE IF EXISTS {0}.{1}; CREATE TABLE {0}.{1} (' + ','.join(field_defs) +');'
        format_args = [sql.Identifier(a) for a in format_args]
        cur.execute(sql.SQL(q).format(*format_args))
        con.commit()
        rs.append('Created table '+d['tablename'])
        
    return('</p><p>'.join(rs))
        


