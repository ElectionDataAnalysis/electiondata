#!/usr/bin/python3
# under construction
# db_routines/Create_CDF_db/__init__.py
# TODO may need to add "NOT NULL" requirements per CDF
# TODO add OfficeContestJoin table (e.g., Presidential contest has two offices)

import db_routines as dbr
import sqlalchemy
from sqlalchemy import MetaData, Table, Column,CheckConstraint,UniqueConstraint,Integer,String,Date,ForeignKey
# NB: imports above are used within string argument to exec()
from sqlalchemy.orm import sessionmaker
import os
import pandas as pd


def create_common_data_format_tables(session,dirpath='CDF_schema_def_info/',delete_existing=False):
    """ schema example: 'cdf'; Creates cdf tables in the given schema
    (or directly in the db if schema == None)
    e_table_list is a list of enumeration tables for the CDF, e.g., ['ReportingUnitType','CountItemType', ... ]
    Does *not* fill enumeration tables.
    """
    eng = session.bind
    metadata = MetaData(bind=eng)

    # create the single sequence for all db ids
    id_seq = sqlalchemy.Sequence('id_seq', metadata=metadata)

    # create enumeration tables and push to db
    e_table_list = enum_table_list(dirpath)
    for t in e_table_list:
        if t=='BallotMeasureSelection':
            txt_col='Selection'
        else:
            txt_col='Txt'
        Table(t,metadata,Column('Id',Integer,id_seq,server_default=id_seq.next_value(),primary_key=True),
              Column(txt_col,String,unique=True))
    metadata.create_all()

    # create all other tables, in set order because of foreign keys
    table_list = ['ExternalIdentifier', 'ReportingUnit', 'Party', 'Election', 'Office', 'CandidateContest',
                  'BallotMeasureContest', 'Candidate', 'VoteCount', 'SelectionElectionContestVoteCountJoin',
                  'CandidateSelection', 'ElectionContestJoin', 'ComposingReportingUnitJoin',
                  'BallotMeasureContestSelectionJoin', 'CandidateContestSelectionJoin','_datafile']
    table_path = os.path.join(dirpath, 'Tables')
    # assert set(table_list) == set(os.listdir('{}Tables'.format(dirpath))), \
    #     f'Set of tables to create does not match set of tables in {dirpath}Tables directory'


    assert set(table_list) == set(os.listdir(table_path)), \
    f'Set of tables to create does not match set of tables in {dirpath}Tables directory'

    for element in table_list:
        #with open('{}Tables/{}/short_name.txt'.format(dirpath,element),'r') as f:
        with open(os.path.join(table_path, element, 'short_name.txt'), 'r') as f:
            short_name=f.read().strip()

        df = {}
        file_list = os.listdir(os.path.join(table_path, element))
        flist = [ f[:-4] for f in file_list] # drop '.txt'
        for f in flist: # TODO picks up short_name too, unnecessary
            #df[f] =  pd.read_csv('{}Tables/{}/{}.txt'.format(dirpath,element,f),sep='\t')
            df[f] =  pd.read_csv(os.path.join(table_path,element,f+'.txt'),sep='\t') #consider creating element_path at the start of the loop and why not run a loop over file_list



        field_col_list = [Column(r['fieldname'],eval(r['datatype'])) for i,r in df['fields'].iterrows()]
        null_constraint_list = [CheckConstraint('"{}" IS NOT NULL'.format(r['not_null_fields']),name='{}_{}_not_null'.format(short_name,r['not_null_fields'])) for i,r in df['not_null_fields'].iterrows()]
        other_elt_list = [Column(r['fieldname'],ForeignKey(f'{r["refers_to"]}.Id'))
            for i,r in df['other_element_refs'].iterrows()]
        # unique constraints
        df['unique_constraints']['arg_list'] = df['unique_constraints']['unique_constraint'].str.split(',')
        unique_constraint_list = [UniqueConstraint( * r['arg_list'],name='{}_ux{}'.format(short_name,i)) for i,r in df['unique_constraints'].iterrows()]

        enum_id_list = [Column('{}_Id'.format(r['enumeration']),ForeignKey(f'{r["enumeration"]}.Id'))
                         for i,r in df['enumerations'].iterrows()]
        enum_other_list = [Column('Other{}'.format(r['enumeration']),String) for i,r in df['enumerations'].iterrows()]
        Table(element,metadata,
          Column('Id',Integer,id_seq,server_default=id_seq.next_value(),primary_key=True),
              * field_col_list, * enum_id_list, * enum_other_list,
              * other_elt_list, * null_constraint_list, * unique_constraint_list)

    metadata.create_all()
    session.flush()
    return metadata


# TODO should we somewhere check consistency of enumeration_table_list and the files in enumerations/ ? Is the file enumeration_table_list ever used?
def enum_table_list(dirpath= 'CDF_schema_def_info'):
    #if not dirpath[-1] == '/': dirpath += '\''
    enum_path = os.path.join(dirpath, 'enumerations' )
    file_list = os.listdir(enum_path)
    for f in file_list:
        assert f[-4:] == '.txt', 'File name in ' + dirpath + 'enumerations/ not in expected form: ' + f
    enum_table_list = [f[:-4] for f in file_list]
    return enum_table_list


def fill_cdf_enum_tables(session,schema,dirpath= 'CDF_schema_def_info'):
    """takes lines of text from file and inserts each line into the txt field of the enumeration table"""
    #if not dirpath[-1] == '/': dirpath += '\''
    e_table_list = enum_table_list(dirpath)
    for f in e_table_list:
        if f == 'BallotMeasureSelection':
            txt_col='Selection'
        else:
            txt_col='Txt'
        #dframe = pd.read_csv('{}enumerations/{}.txt'.format(dirpath,f),header=None,names = [txt_col])
        dframe = pd.read_csv(os.path.join(dirpath, 'enumerations', f + '.txt'), header=None, names=[txt_col])
        dframe.to_sql(f,session.bind,schema=schema,if_exists='append',index=False)
    session.flush()
    return e_table_list

