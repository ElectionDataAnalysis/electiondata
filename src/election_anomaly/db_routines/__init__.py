#!/usr/bin/python3
# db_routines/__init__.py

import psycopg2
import sqlalchemy
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from psycopg2 import sql
import sqlalchemy as db
import sqlalchemy_utils
from election_anomaly import user_interface as ui
from configparser import MissingSectionHeaderError
import pandas as pd
from election_anomaly import munge_routines as mr
import re
from election_anomaly.db_routines import create_cdf_db as db_cdf
import os


def get_database_names(con):
    """Return dataframe with one column called `datname` """
    names = pd.read_sql('SELECT datname FROM pg_database',con)
    return names


def create_database(con,cur,db_name):
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    q = "DROP DATABASE IF EXISTS {0}"
    sql_ids = [db_name]
    out1 = query(q,sql_ids,[],con,cur)

    q = "CREATE DATABASE {0}"
    out2 = query(q,sql_ids,[],con,cur)
    return out1,out2


def append_to_composing_reporting_unit_join(session,ru):
    """<ru> is a dframe of reporting units, with cdf internal name in column 'Name'.
    cdf internal name indicates nesting via semicolons `;`.
    This routine calculates the nesting relationships from the Names and uploads to db.
    Returns the *all* composing-reporting-unit-join data from the db.
    By convention, a ReportingUnit is it's own ancestor (ancestor_0)."""
    ru['split'] = ru['Name'].apply(lambda x:x.split(';'))
    ru['length'] = ru['split'].apply(len)
    
    # pull ReportingUnit to get ids matched to names
    ru_cdf = pd.read_sql_table('ReportingUnit',session.bind,index_col=None)
    ru_static = ru.copy()

    # add db Id column to ru_static, if it's not already there
    if 'Id' not in ru.columns:
        ru_static = ru_static.merge(ru_cdf[['Name','Id']],on='Name',how='left')

    # create a list of rows to append to the ComposingReportingUnitJoin element
    cruj_dframe_list = []
    for i in range(ru['length'].max()):
        # check that all components of all Reporting Units are themselves ReportingUnits
        ru_for_cruj = ru_static.copy()  # start fresh, without detritus from previous i

        # get name of ith ancestor
        #  E.g., ancestor_0 is largest ancestor (i.e., shortest string, often the state); ancestor_1 is the second-largest parent, etc.
        ru_for_cruj[f'ancestor_{i}'] = ru_static['split'].apply(lambda x:';'.join(x[:i+1]))
        # get Id of ith ancestor
        ru_for_cruj = ru_for_cruj.merge(ru_cdf,left_on=f'ancestor_{i}',right_on='Name',
                                        suffixes=['',f'_{i}'])
        cruj_dframe_list.append(ru_for_cruj[['Id',f'Id_{i}']].rename(
            columns={'Id':'ChildReportingUnit_Id',f'Id_{i}':'ParentReportingUnit_Id'}))
    if cruj_dframe_list:
        cruj_dframe = pd.concat(cruj_dframe_list)
        cruj_dframe, e = dframe_to_sql(cruj_dframe,session,'ComposingReportingUnitJoin')
    else:
        cruj_dframe = pd.read_sql_table('ComposingReportingUnitJoin',session.bind)
    session.flush()
    return cruj_dframe


def establish_connection(paramfile, db_name='postgres'):
    """Check for DB and relevant tables; if they don't exist, return
    error message"""
    try:
        params = ui.config(paramfile)
    except MissingSectionHeaderError as e:
        return {'message': 'database.ini file not found suggested location.'}
    params['dbname']=db_name
    try:
        con = psycopg2.connect(**params)
    except psycopg2.OperationalError as e:
        return {'message': 'Unable to establish connection to database.'}

    # Look for tables
    engine = sql_alchemy_connect(paramfile, db_name)
    elems, enums, joins, o = get_cdf_db_table_names(engine)

    # All tables except "Others" must be created. Essentially looks for
    # a "complete" database.
    if not elems or not enums or not joins:
        return {'message': 'Required tables not found.'}

    con.close()
    return None


def create_new_db(project_root, paramfile, db_name):
    # get connection to default postgres DB to create new one
    try:    
        params = ui.config(paramfile)
        params['dbname'] = 'postgres'
        con = psycopg2.connect(**params)
    except:
        # Can't connect to the default postgres database, so there
        # seems to be something wrong with connection. Fail here.
        print('Unable to find database. Exiting.')
        quit()
    cur = con.cursor()
    db_df = get_database_names(con)

    eng = sql_alchemy_connect(paramfile=paramfile,db_name=db_name)
    Session = sqlalchemy.orm.sessionmaker(bind=eng)
    sess = Session()

    # DB already exists.
    # TODO if DB exists, check that desired_db has right format?
    if db_name in db_df.datname.unique():
        # Clean out DB
        db_cdf.reset_db(sess,
            os.path.join(project_root,'election_anomaly','CDF_schema_def_info'))
    else:
        create_database(con, cur, db_name)

    # load cdf tables
    db_cdf.create_common_data_format_tables(
        sess,dirpath=os.path.join(project_root,'election_anomaly','CDF_schema_def_info'))
    db_cdf.fill_cdf_enum_tables(
        sess,None,dirpath=os.path.join(project_root,'election_anomaly/CDF_schema_def_info/'))
    con.close()


def sql_alchemy_connect(paramfile=None,db_name='postgres'):
    """Returns an engine and a metadata object"""
    if not paramfile:
        paramfile = ui.pick_paramfile()
    params = ui.config(paramfile)
    if db_name != 'postgres':
        params['dbname'] = db_name
    # We connect with the help of the PostgreSQL URL
    url = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    url = url.format(**params)

    # The return value of create_engine() is our connection object
    engine = db.create_engine(url, client_encoding='utf8')
    return engine


def add_integer_cols(session,table,col_list):
    add = ','.join([f' ADD COLUMN "{c}" INTEGER' for c in col_list])
    q = f'ALTER TABLE "{table}" {add}'
    sql_ids=[]
    strs = []
    raw_query_via_sqlalchemy(session,q,sql_ids,strs)
    return


def drop_cols(session,table,col_list):
    drop = ','.join([f' DROP COLUMN "{c}"' for c in col_list])
    q = f'ALTER TABLE "{table}" {drop}'
    sql_ids=[]
    strs = []
    raw_query_via_sqlalchemy(session,q,sql_ids,strs)
    return


def get_cdf_db_table_names(eng):
    """This is postgresql-specific"""
    db_columns = pd.read_sql_table('columns',eng,schema='information_schema')
    public = db_columns[db_columns.table_schema=='public']
    cdf_elements = set()
    cdf_enumerations = set()
    cdf_joins = set()
    others = set()
    for t in public.table_name.unique():
        # main_routines element name string
        if t[0] == '_':
            others.add(t)
        elif t[-4:] == 'Join':
            cdf_joins.add(t)
        else:
            # main_routines columns
            cols = public[public.table_name == t].column_name.unique()
            if set(cols) == {'Id','Txt'} or set(cols) == {'Id','Selection'}:
                cdf_enumerations.add(t)
            else:
                cdf_elements.add(t)
    # TODO order cdf_elements and cdf_joins by references to one another
    return cdf_elements, cdf_enumerations, cdf_joins, others


def read_enums_from_db_table(sess,element):
	"""Returns list of enum names (e.g., 'CountItemType') for the given <element>.
	Identifies enums by the Other{enum} column name (e.g., 'OtherCountItemType)"""
	df = pd.read_sql_table(element,sess.bind,index_col='Id')
	other_cols = [x for x in df.columns if x[:5] == 'Other']
	enums = [x[5:] for x in other_cols]
	return enums


# TODO combine query() and raw_query_via_sqlalchemy()?
def query(q,sql_ids,strs,con,cur):  # needed for some raw queries, e.g., to create db and schemas
    format_args = [sql.Identifier(a) for a in sql_ids]
    cur.execute(sql.SQL(q).format(*format_args),strs)
    con.commit()
    if cur.description:
        return cur.fetchall()
    else:
        return None


def raw_query_via_sqlalchemy(session,q,sql_ids,strs):
    connection = session.bind.connect()
    con = connection.connection
    cur = con.cursor()
    format_args = [sql.Identifier(a) for a in sql_ids]
    cur.execute(sql.SQL(q).format(*format_args),strs)
    con.commit()
    if cur.description:
        return_item = cur.fetchall()
    else:
        return_item = None
    cur.close()
    con.close()
    return return_item


def get_enumerations(session,element):
    """Returns a list of enumerations referenced in the <element> element"""
    q = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='{element}';
    """
    col_df = pd.read_sql(q,session.bind)
    maybe_enum_list = [x[:-3] for x in col_df.column_name if x[-3:] == '_Id']
    enum_list = [x for x in maybe_enum_list if f'Other{x}' in col_df.column_name.unique()]
    return enum_list


def get_foreign_key_df(session,element):
    """Returns a dataframe whose index is the name of the field in the <element> element, with columns
    foreign_table_name and foreign_column_name"""
    q = f"""
        SELECT
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
        FROM
        information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='{element}';
    """
    fk_df = pd.read_sql(q,session.bind,index_col='column_name')
    return fk_df


def add_foreign_key_name_col(sess,df,foreign_key_col,foreign_key_element,drop_old=False):
    df_copy = df.copy()
    fk_element_df = pd.read_sql_table(foreign_key_element,sess.bind,index_col='Id')
    name_field = get_name_field(foreign_key_element)
    fk_element_df.rename(columns={name_field:foreign_key_col[:-3]},inplace=True)
    df_copy = df_copy.merge(
        fk_element_df[[foreign_key_col[:-3]]],how='left',left_on=foreign_key_col,right_index=True)
    if drop_old:
        df_copy.drop(foreign_key_col,axis=1,inplace=True)
    return df_copy


def dframe_to_sql(
        dframe: pd.DataFrame, session, element: str, index_col: str= 'Id', flush: bool=True,
        raw_to_votecount: bool=False, return_records: str='all') -> [pd.DataFrame, str]:
    """
    Given a dataframe <dframe >and an existing cdf db element <element>>, clean <dframe>
    (i.e., drop any columns that are not in <element>, add null columns to match any missing columns)
    append records any new records to the corresponding element in the db (and commit!)
    Return the updated dataframe, including all rows from the db and all from the dframe.
    <return_records> is a flag defaulting to "all" (return all records in db)
    but can be set to "original" to return only the records from the input <dframe>.

    """
    # pull copy of existing element
    target = pd.read_sql_table(element, session.bind, index_col=index_col)
    # VoteCount element gets added columns during raw data upload, needs special treatment

    if dframe.empty:
        if return_records == 'original':
            return dframe, None
        else:
            return target, None
    if raw_to_votecount:
        # join with ECSVCJ
        secvcj = pd.read_sql_table('ElectionContestSelectionVoteCountJoin',session.bind,index_col=None)
        # drop columns that don't belong, but were temporarily created in order
        #  to get VoteCount_Id correctly into ECSVCJ
        target=target.drop(['ElectionContestJoin_Id','ContestSelectionJoin_Id','_datafile_Id'],axis=1)
        target=target.merge(secvcj,left_on='Id',right_on='VoteCount_Id')
        target=target.drop(['Id','VoteCount_Id'],axis=1)
    df_to_db = dframe.copy()
    df_to_db.drop_duplicates(inplace=True)
    if 'Count' in df_to_db.columns:
        # catch anything not an integer (e.g., in MD 2018g upload)
        # TODO is this still necessary, given cast_cols_as_int?
        df_to_db.loc[:,'Count']=df_to_db['Count'].astype('int64',errors='ignore')

    # partition the columns
    dframe_only_cols = [x for x in dframe.columns if x not in target.columns]
    target_only_cols = [x for x in target.columns if x not in dframe.columns]
    intersection_cols = [x for x in target.columns if x in dframe.columns]

    # remove columns that don't exist in target element
    df_to_db = df_to_db.drop(dframe_only_cols, axis=1)

    # add columns that exist in target element but are missing from original dframe
    for c in target_only_cols:
        df_to_db.loc[:,c] = None

    # make sure datatypes of df_to_db match the types of target
    for c in intersection_cols:
        if target[c].dtype != df_to_db[c].dtype:
            df_to_db[c] = df_to_db[c].astype(target[c].dtype,errors='ignore')

    appendable = pd.concat([target,target,df_to_db],sort=False).drop_duplicates(keep=False)
    # note: two copies of target ensures none of the original rows will be appended.

    # drop the Id column
    if 'Id' in appendable.columns:
        appendable = appendable.drop('Id',axis=1)

    error_string = None
    try:
        appendable.to_sql(element, session.bind, if_exists='append', index=False)
    except sqlalchemy.exc.IntegrityError as e:
        # FIXME: target, pulled from DB, has datetime, while dframe has date,
        #  so record might look like same-name-different-date when it isn't really
        # FIXME: IntegrityError will fail silently because it broke the dataload
        error_string = f'Error uploading {element} to {session.bind.url}: {e}'

    if element == 'ReportingUnit' and not appendable.empty:
        append_to_composing_reporting_unit_join(session,appendable)

    up_to_date_dframe = pd.read_sql_table(element, session.bind)
    up_to_date_dframe = format_dates(up_to_date_dframe)

    if raw_to_votecount:
        # need to drop rows that were read originally from target -- these will have null ElectionContestJoin_Id
        up_to_date_dframe=up_to_date_dframe[up_to_date_dframe['ElectionContestJoin_Id'].notnull()]
    if flush:
        session.flush()
    if return_records == 'original':
        # TODO get rid of rows not in dframe by taking inner join
        up_to_date_dframe = dframe.merge(
            up_to_date_dframe,left_on=intersection_cols,right_on=intersection_cols,how='inner').drop(
            target_only_cols,axis=1)
    return up_to_date_dframe, error_string


def format_dates(dframe):
    """ensure any date columns are pulled in 2020-05-20 format"""
    df = dframe.copy()
    # TODO is 'datetime64[ns]' the only thing that will show up?
    for c in df.columns[df.dtypes=='datetime64[ns]']:
        df[c] = f'{df[c].dt.date}'
    return df


def name_from_id(session,element,idx):
    name_field = get_name_field(element)
    q = f"""SELECT "{name_field}" FROM "{element}" WHERE "Id" = {idx}"""
    name_df = pd.read_sql(q,session.bind)
    try:
        name = name_df.loc[0,name_field]
    except KeyError:
        # if no record with Id = <idx> was found
        name = None
    return name


def name_to_id(session,element,name):
    name_field = get_name_field(element)
    q = f"""SELECT "Id" FROM "{element}" WHERE "{name_field}" = '{name}' """
    idx_df = pd.read_sql(q,session.bind)
    try:
        idx = idx_df.loc[0,'Id']
    except KeyError:
        # if no record with name <name> was found
        idx = None
    return idx


def get_name_field(element):
    # TODO pull from db or filesystem instead of hard coding
    if element == 'BallotMeasureSelection':
        field = 'Selection'
    elif element == 'Candidate':
        field = 'BallotName'
    elif element in ['CountItemType','ElectionType','IdentifierType','ReportingUnitType']:
        field = 'Txt'
    elif element == '_datafile':
        field = 'short_name'
    else:
        field = 'Name'
    return field


def truncate_table(session, table_name):
    session.execute(f'TRUNCATE TABLE "{table_name}" CASCADE')
    session.commit()
    return


def get_input_options(session, input):
    """Returns a list of response options based on the input"""
    # input comes as a pythonic (snake case) input, need to 
    # change to match DB element naming format
    name_parts = input.split('_')
    search_str = "".join([name_part.capitalize() for name_part in name_parts])

    if search_str in ['BallotMeasureContest', 'CandidateContest', 'Election',
        'Office', 'Party', 'ReportingUnit']:
        column_name = 'Name'
        table_search = True
    elif search_str in ['CountItemStatus', 'CountItemType', 'ElectionType',
        'IdentifierType', 'ReportingUnitType']:
        column_name = 'Txt'
        table_search = True
    elif search_str == 'BallotMeasureSelection':
        column_name = 'Selection'
        table_search = True
    elif search_str == 'Candidate':
        column_name = 'BallotName'
        table_search = True
    else:
        search_str = search_str.lower()
        table_search = False

    if table_search:
        result = session.execute(f'SELECT "{column_name}" FROM "{search_str}";')
        return [r[0] for r in result]
    else:
        result = session.execute(f' \
            SELECT "Name" FROM "ReportingUnit" ru \
            JOIN "ReportingUnitType" rut on ru."ReportingUnitType_Id" = rut."Id" \
            WHERE rut."Txt" = \'{search_str}\'')
        return [r[0] for r in result]


def get_datafile_info(session, results_file):
    q = session.execute(f'''
        SELECT "Id", "Election_Id" 
        FROM _datafile 
        WHERE short_name = '{results_file}'
        ''').fetchall()
    try:
        return q[0]
    except IndexError:
        print(f'No record named {results_file} found in _datafile table in {session.bind.url}')
        return [0,0]