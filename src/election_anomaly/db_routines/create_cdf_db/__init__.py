# TODO may need to add "NOT NULL" requirements per CDF
# TODO add OfficeContestJoin table (e.g., Presidential contest has two offices)
# TODO consistency check on SelectionElectionContestVoteCountJoin to make sure ElectionContestJoin_Id
#  and ContestSelectionJoin_Id share a contest? Should this happen during the rollup process?

import sqlalchemy
from sqlalchemy import MetaData, Table, Column,CheckConstraint,UniqueConstraint,Integer,String,ForeignKey, Date
import os
import pandas as pd


def create_common_data_format_tables(session,dirpath='CDF_schema_def_info/'):
    """ schema example: 'cdf'; Creates cdf tables in the given schema
    (or directly in the db if schema == None)
    e_table_list is a list of enumeration tables for the CDF, e.g., ['ReportingUnitType','CountItemType', ... ]
    Does *not* fill enumeration tables.
    """
    eng = session.bind
    metadata = MetaData(bind=eng)

    # create the single sequence for all db ids
    id_seq = sqlalchemy.Sequence('id_seq', metadata=metadata)

    # create enumeration tables 
    e_table_list = enum_table_list(dirpath)
    for t in e_table_list:
        create_table(metadata,id_seq,t,'enumerations',dirpath,eng,session)
    
    # create element tables (cdf and metadata) and push to db
    element_path = os.path.join(dirpath, 'elements')
    elements_to_process = [f for f in os.listdir(element_path) if f[0] != '.']
    # dynamic list of elements whose tables haven't been created yet
    while elements_to_process:
        element = elements_to_process[0]
        # check foreign keys; if any refers to an elt yet to be processed, change to that elt
        #  note that any foreign keys for elements are to other elements, so it's OK to do this without considering
        #  joins first or concurrently.
        foreign_keys = pd.read_csv(os.path.join(element_path,element,'foreign_keys.txt'),sep='\t')
        for i,r in foreign_keys.iterrows():
            fk_set = set(r['refers_to'].split(';'))    # lists all targets of the foreign key r['fieldname']
            try:
                element = [e for e in fk_set if e in elements_to_process].pop()
                break
            except IndexError:
                pass
        # create db table for element
        create_table(metadata,id_seq,element,'elements',dirpath,eng,session)
        # remove element from list of yet-to-be-processed
        elements_to_process.remove(element)

    # create join tables
    # TODO check for foreign keys, as above
    # check for foreign keys
    join_path = os.path.join(dirpath,'joins')
    joins_to_process = [f for f in os.listdir(join_path) if f[0] != '.']
    while joins_to_process:
        j = joins_to_process[0]
        # check foreign keys; if any refers to an elt yet to be processed, change to that elt
        #  note that any foreign keys for elements are to other elements, so it's OK to do this without considering
        #  joins first or concurrently.
        foreign_keys = pd.read_csv(os.path.join(join_path,j,'foreign_keys.txt'),sep='\t')
        for i,r in foreign_keys.iterrows():
            fk_set = set(r['refers_to'].split(';'))  # lists all targets of the foreign key r['fieldname']
            try:
                j = [e for e in fk_set if e in joins_to_process].pop()
                break
            except IndexError:
                pass
        # create db table for element
        create_table(metadata,id_seq,j,'joins',dirpath,eng,session)
        # remove element from list of yet-to-be-processed
        joins_to_process.remove(j)

    # push all tables to db
    metadata.create_all()
    session.flush()
    return metadata


def create_table(metadata,id_seq,name,table_type,dirpath,engine,session):
    t_path = os.path.join(dirpath,table_type,name)
    if table_type == 'elements':
        with open(os.path.join(t_path, 'short_name.txt'), 'r') as f:
            short_name=f.read().strip()

        # read info from files into dataframes
        df = {}
        for filename in ['enumerations','fields','foreign_keys','not_null_fields','unique_constraints']:
            df[filename] = pd.read_csv(os.path.join(t_path,f'{filename}.txt'),sep='\t')

        # define table
        df['fields']['datatype'].replace({'Encoding':'String'},inplace=True)
        field_col_list = [Column(r['fieldname'],eval(r['datatype'])) for i,r in df['fields'].iterrows()]
        null_constraint_list = [
            CheckConstraint(
                f'"{r["not_null_fields"]}" IS NOT NULL',name=f'{short_name}_{r["not_null_fields"]}_not_null')
            for i,r in df['not_null_fields'].iterrows()]
        # omit 'foreign keys' that refer to more than one table,
        #  e.g. Contest_Id to BallotMeasureContest and CandidateContest
        foreign_key_list = [Column(r['fieldname'],ForeignKey(f'{r["refers_to"]}.Id'))
                          for i,r in df['foreign_keys'].iterrows() if ';' not in r['refers_to']]
        # unique constraints
        df['unique_constraints']['arg_list'] = df['unique_constraints']['unique_constraint'].str.split(',')
        unique_constraint_list = [UniqueConstraint(*r['arg_list'],name=f'{short_name}_ux{i}')
                                  for i,r in df['unique_constraints'].iterrows()]
        enum_id_list = [Column(f'{r["enumeration"]}_Id',ForeignKey(f'{r["enumeration"]}.Id'))
                        for i,r in df['enumerations'].iterrows()]
        enum_other_list = [Column(f'Other{r["enumeration"]}',String) for i,r in df['enumerations'].iterrows()]
        Table(name,metadata,
              Column('Id',Integer,id_seq,server_default=id_seq.next_value(),primary_key=True),
              * field_col_list, * enum_id_list, * enum_other_list,
              * foreign_key_list, * null_constraint_list, * unique_constraint_list)

    elif table_type == 'enumerations':
        if name == 'BallotMeasureSelection':
            txt_col = 'Selection'
        else:
            txt_col = 'Txt'
        Table(name,metadata,Column('Id',Integer,id_seq,server_default=id_seq.next_value(),primary_key=True),
              Column(txt_col,String,unique=True))
    
    elif table_type == 'joins':
        with open(os.path.join(t_path, 'short_name.txt'), 'r') as f:
            short_name=f.read().strip()
        # read info from files into dataframes
        
        fk = pd.read_csv(os.path.join(t_path,'foreign_keys.txt'),sep='\t')

        # define table
        col_list = [Column(r['fieldname'],Integer) for i,r in fk.iterrows()]
        null_constraint_list = [
            CheckConstraint(
                f'"{r["fieldname"]}" IS NOT NULL',name=f'{short_name}_{r["fieldname"]}_not_null')
            for i,r in fk.iterrows()]
        # omit 'foreign keys' that refer to more than one table,
        #  e.g. Contest_Id to BallotMeasureContest and CandidateContest
        true_foreign_key_list = [Column(r['fieldname'],ForeignKey(f'{r["refers_to"]}.Id'))
                          for i,r in fk.iterrows() if ';' not in r['refers_to']]

        Table(name,metadata,
              Column('Id',Integer,id_seq,server_default=id_seq.next_value(),primary_key=True),
              * col_list,
              * true_foreign_key_list, * null_constraint_list)
    else:
        raise Exception(f'table_type {table_type} not recognized')
    return


def enum_table_list(dirpath='CDF_schema_def_info'):
    enum_path = os.path.join(dirpath, 'enumerations')
    file_list = os.listdir(enum_path)
    for f in file_list:
        assert f[-4:] == '.txt', 'File name in ' + dirpath + 'enumerations/ not in expected form: ' + f
    e_table_list = [f[:-4] for f in file_list]
    return e_table_list


def fill_cdf_enum_tables(session,schema,dirpath='CDF_schema_def_info'):
    """takes lines of text from file and inserts each line into the txt field of the enumeration table"""
    e_table_list = enum_table_list(dirpath)
    for f in e_table_list:
        if f == 'BallotMeasureSelection':
            txt_col='Selection'
        else:
            txt_col='Txt'
        dframe = pd.read_csv(os.path.join(dirpath, 'enumerations', f + '.txt'), header=None, names=[txt_col])
        dframe.to_sql(f,session.bind,schema=schema,if_exists='append',index=False)
    session.flush()
    return e_table_list


def reset_db(session, dirpath):
    """ Resets DB to a clean state with no tables/sequences.
    Used if a DB is created for a user but not populated, for example."""

    eng = session.bind
    conn = eng.connect()
    conn.execute('DROP SEQUENCE IF EXISTS id_seq CASCADE;')
    session.commit()

    # create enumeration tables 
    e_table_list = enum_table_list(dirpath)
    for table in e_table_list:
        conn.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
        session.commit()
    
    element_path = os.path.join(dirpath, 'elements')
    elements_to_process = [f for f in os.listdir(element_path) if f[0] != '.']
    # dynamic list of elements whose tables haven't been created yet
    for table in elements_to_process:
        conn.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
        session.commit()

    join_path = os.path.join(dirpath,'joins')
    joins_to_process = [f for f in os.listdir(join_path) if f[0] != '.']
    for table in joins_to_process:
        conn.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
        session.commit()
    conn.close()