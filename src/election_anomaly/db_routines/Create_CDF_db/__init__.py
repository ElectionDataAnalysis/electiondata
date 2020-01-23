#!/usr/bin/python3
# under construction
# db_routines/Create_CDF_db/__init__.py
# TODO may need to add "NOT NULL" requirements per CDF

import db_routines as dbr
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, Date, MetaData, ForeignKey, CheckConstraint, UniqueConstraint
from sqlalchemy.engine import reflection
from sqlalchemy.orm import sessionmaker
import os

def create_schema(session,name):
    if eng.dialect.has_schema(eng, name):
        recreate = input('WARNING: schema ' + name + ' already exists; erase and recreate (y/n)?\n')
        if recreate == 'y':
            session.bind.engine.execute(sqlalchemy.schema.DropSchema(name,cascade=True))
            session.bind.engine.execute(sqlalchemy.schema.CreateSchema(name))
            print('New schema created: ' + name)
            new_schema_created = True
        else:
            print('Schema preserved: '+ name)
            new_schema_created = False
            insp = reflection.Inspector.from_engine(eng)
            tablenames = insp.get_table_names(schema=name)
            viewnames = insp.get_view_names(schema=name)
            if tablenames:
                print('WARNING: Some tables exist: \n\t'+'\n\t'.join([name + '.' + t for t in tablenames]))
            if viewnames:
                print('WARNING: Some views exist: \n\t' + '\n\t'.join([name + '.' + t for t in viewnames]))

    else:
        session.bind.engine.execute(sqlalchemy.schema.CreateSchema(name))
        print('New schema created: ' + name)
        new_schema_created = True
    session.commit()
    return new_schema_created


def create_common_data_format_schema_SQLALCHEMY(session,schema,dirpath='CDF_schema_def_info/'):
    """ schema example: 'cdf'; Creates cdf tables in the given schema"""
    create_schema(session,schema)
    eng = session.bind
    metadata = MetaData(bind=eng,schema=schema)

    #%% create the single sequence for all db ids
    id_seq = sqlalchemy.Sequence('id_seq', metadata=metadata,schema=schema)

    #%% create enumeration tables and push to db
    print('Creating enumeration tables')
    table_list = ['IdentifierType', 'CountItemStatus', 'ReportingUnitType', 'ElectionType', 'CountItemType'] # TODO this list should not be hard-coded
    for t in table_list:
        print('\t'+t)
        #exec(t + '= Table(\'' + t + '\',metadata, Column(\'Id\',Integer, id_seq,server_default=id_seq.next_value(),primary_key=True), Column(\'Txt\',String),schema = \'' + schema + '\')')
        exec('Table(\'' + t + '\',metadata, Column(\'Id\',Integer, id_seq,server_default=id_seq.next_value(),primary_key=True), Column(\'Txt\',String),schema = \'' + schema + '\')')
    metadata.create_all()

    #%% create all other tables, in set order because of foreign keys
    fpath = dirpath + 'tables.txt'
    print('Creating other tables, from ' + fpath + ':')
    with open(fpath, 'r') as f:
        table_def_list = eval(f.read())
    for table_def in table_def_list:
        name = table_def[0]
        field_d = table_def[1]
        print('\t'+ name)
        col_string_list = ['Column(\''+ f['fieldname'] + '\',' + f['datatype'] + ')' for f in field_d['fields']] + ['Column(\'' + e + '_Id\',ForeignKey(\'' + schema + '.' + e + '.Id\')),Column(\'Other' + e + '\',String)' for e in field_d['enumerations']] + ['Column(\'' + oer['fieldname'] + '\',ForeignKey(\'' + schema + '.' + oer['refers_to'] + '.Id\'))' for oer in field_d['other_element_refs']] + ['CheckConstraint(\'"' + nnf + '" IS NOT NULL\',name = \'' + field_d['short_name'] + '_' + nnf + '_not_null\' )' for nnf in field_d['not_null_fields']] + ['UniqueConstraint(' + ','.join(['\'' + x +'\'' for x in uc]) + ',name=\'' + field_d['short_name'] + '_ux' + str(field_d['unique_constraints'].index(uc)) + '\')' for uc in field_d['unique_constraints']]
        table_creation_string = 'Table(\''+ name + '\',metadata,Column(\'Id\',Integer,id_seq,server_default=id_seq.next_value(),primary_key=True),' + ','.join(col_string_list) + ', schema=\'' + schema + '\')'
        exec(table_creation_string)
        metadata.create_all()
    return metadata

def fill_cdf_enum_tables(session,meta,dirpath = 'CDF_schema_def_info/'):
    """takes lines of text from file and inserts each line into the txt field of the enumeration table"""
    if not dirpath[-1] == '/':
        dirpath += '/'
    print('Filling enumeration tables:')
    for tfile in os.listdir(dirpath + 'enumerations/'):
        fpath = dirpath + 'enumerations/' + tfile
        table = meta.tables[meta.schema + '.' + tfile[:-4]]
        print(table.key)
        with open(fpath, 'r') as f:
            entries = f.read().splitlines()
        for entry in entries:
            ins = table.insert().values(Txt=entry)
            session.execute(ins)
    session.commit()
    # TODO
    return

def list_enum_tables(dirpath = 'CDF_schema_def_info/'):
    enum_files = os.listdir(dirpath+'enumerations/')
    enum_tables = [ x[:-4] for x in enum_files]
    return enum_tables

def create_common_data_format_schema (con,cur,schema_name):
    """ schema_name example: 'cdf'; Creates schema with that name on the given db connection and cursor"""

    # create the blank schema
    new_schema_created = dbr.create_schema(schema_name)
    if new_schema_created:
        # create a sequence for unique id values across all tables
        dbr.query('CREATE SEQUENCE {}.id_seq;',[schema_name],[],con,cur)
    
        # create and fill enumeration tables
        print('\tCreating enumeration tables')
        enumeration_path = 'CDF_schema_def_info/enumerations/'
        for t in ['IdentifierType','CountItemStatus','ReportingUnitType','ElectionType','CountItemType']:
            q = 'DROP TABLE IF EXISTS {0}.{1}; CREATE TABLE {0}.{1} ("Id" BIGINT DEFAULT nextval(\'{0}.id_seq\') PRIMARY KEY,"Txt" TEXT UNIQUE NOT NULL); '
            dbr.query(q,[schema_name,t],[],con,cur) # note: UNIQUE in query automatically creates index.

            dbr.fill_enum_table(schema_name,t,enumeration_path + t + '.txt',con,cur) # TODO document purpose or remove

    
        # create all other tables, in set order because of foreign keys
        print('\tCreating other tables, from CDF_schema_def_info/tables.txt')
        with open('CDF_schema_def_info/tables.txt','r') as f:
            table_def_list = eval(f.read())
        for table_def in table_def_list:
            print('Processing table '+ table_def[0])
            field_defs = ['"Id" BIGINT DEFAULT nextval(\'{0}.id_seq\') PRIMARY KEY']
            format_args = [schema_name,table_def[0]]
            ctr = 2     # counter to track sql.Identifiers
            for f in table_def[1]['fields']:
                field_defs.append( '{'+str(ctr)+'} ' + f['datatype'] )
                format_args.append( f['fieldname'])
                ctr += 1
            for e in table_def[1]['enumerations']:
                field_defs.append( '{'+str(ctr)+'} INTEGER REFERENCES {0}.{'+str(ctr+1)+'}("Id"), {'+str(ctr+2)+'} TEXT' )
                format_args.append(e+'_Id')
                format_args.append(e)
                format_args.append('Other'+e)
                ctr += 3
            for other in table_def[1]['other_element_refs']:
                field_defs.append('{'+str(ctr)+'} INTEGER REFERENCES {0}.{'+str(ctr+1)+'}("Id")')
                format_args.append(other['fieldname'])
                format_args.append(other['refers_to'])
                ctr += 2
            for fname in table_def[1]['not_null_fields']:
                field_defs.append('CHECK ({'+str(ctr)+'} IS NOT NULL)')
                format_args.append(fname)
                ctr += 1

            q = 'DROP TABLE IF EXISTS {0}.{1}; CREATE TABLE {0}.{1} (' + ','.join(field_defs) +');'
            dbr.query(q,format_args,[],con,cur)

            # create unique indices
            for f_list in table_def[1]['unique_constraints']:
                print('\tCreating index on '+str(f_list))
                constraint_name = table_def[0]+'__'+'_'.join(f_list) + '_index'
                [f_slots,f_sql_ids] = zip(*[ ['{'+str(index)+'}',value] for index,value in enumerate(f_list,3)])
                q = 'CREATE UNIQUE INDEX {2} ON {0}.{1} ('+','.join(f_slots)+')'
                dbr.query(q,[schema_name,table_def[0],constraint_name]+list(f_sql_ids),[],con,cur)
    return

if __name__ == '__main__':

    eng,meta = dbr.sql_alchemy_connect(paramfile='../../../local_data/database.ini')
    Session = sessionmaker(bind=eng)
    session = Session()

    schema='test'
    metadata = create_common_data_format_schema_SQLALCHEMY(session, schema,dirpath = '../../CDF_schema_def_info/')
    fill_cdf_enum_tables(session,metadata,dirpath='../../CDF_schema_def_info/')
    print ('Done!')

