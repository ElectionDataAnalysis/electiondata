#!/usr/bin/python3
# under construction
# db_routines/Create_CDF_db/__init__.py
# TODO may need to add "NOT NULL" requirements per CDF

from psycopg2 import sql
import db_routines as dbr


def create_common_data_format_schema (con,cur,schema_name):
    """ schema_name example: 'cdf'; Creates schema with that name on the given db connection and cursor"""

    # create the blank schema
    new_schema_created = dbr.create_schema(schema_name)
    if new_schema_created:
        # create a sequence for unique id values across all tables
        dbr.query('CREATE SEQUENCE {}.id_seq;',[schema_name],[],con,cur)
    
        # create and fill enumeration tables
        enumeration_path = 'CDF_schema_def_info/enumerations/'
        for t in ['IdentifierType','CountItemStatus','ReportingUnitType','ElectionType','CountItemType']:
            q = 'DROP TABLE IF EXISTS {0}.{1}; CREATE TABLE {0}.{1} ("Id" BIGINT DEFAULT nextval(\'{0}.id_seq\') PRIMARY KEY,Txt TEXT UNIQUE NOT NULL); '
            dbr.query(q,[schema_name,t],[],con,cur)
            dbr.fill_enum_table(schema_name,t,enumeration_path + t + '.txt',con,cur)
    
        # create all other tables, in set order because of foreign keys
        with open('CDF_schema_def_info/tables.txt','r') as f:
            table_ds = eval(f.read())
        for d in table_ds:
            field_defs = ['"Id" BIGINT DEFAULT nextval(\'{0}.id_seq\') PRIMARY KEY']
            format_args = [schema_name,d['tablename']]
            ctr = 2     # counter to track sql.Identifiers
            for f in d['fields']:
                field_defs.append( '{'+str(ctr)+'} ' + f['datatype'] )
                format_args.append( f['fieldname'])
                ctr += 1
            for e in d['enumerations']:
                field_defs.append( '{'+str(ctr)+'} INTEGER REFERENCES {0}.{'+str(ctr+1)+'}("Id"), {'+str(ctr+2)+'} TEXT' )
                format_args.append(e+'_Id')
                format_args.append(e)
                format_args.append('Other'+e)
                ctr += 3
            for other in d['other_element_refs']:
                field_defs.append('{'+str(ctr)+'} INTEGER REFERENCES {0}.{'+str(ctr+1)+'}("Id")')
                format_args.append(other['fieldname'])
                format_args.append(other['refers_to'])
                ctr += 2
            for f_list in d['unique_constraints']:
                field_defs.append('UNIQUE ('+ ','.join([ '{' + str(ctr + f_list.index(f) ) + '}' for f in f_list]) +')')
                format_args += f_list
                ctr += len(f_list)
            for fname in d['not_null_fields']:
                field_defs.append('CHECK ({'+str(ctr)+'} IS NOT NULL)')
                format_args.append(fname)
                ctr += 1
        
        # 'UNIQUE ("ForeignId", "IdentifierType_Id", "OtherIdentifierType")'
                
            q = 'DROP TABLE IF EXISTS {0}.{1}; CREATE TABLE {0}.{1} (' + ','.join(field_defs) +');'
            dbr.query(q,format_args,[],con,cur)
    return



