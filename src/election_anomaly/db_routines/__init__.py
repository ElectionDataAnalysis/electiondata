#!/usr/bin/python3
# db_routines/__init__.py

import psycopg2
import sqlalchemy
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql
import sqlalchemy as db
import user_interface as ui
from configparser import MissingSectionHeaderError
import pandas as pd
import os
import munge_routines as mr
import re


class CdfDbException(Exception):
    pass


def get_database_names(con,cur):
    names = pd.DataFrame(query('SELECT datname FROM pg_database',[],[],con,cur))
    return names


def create_database(con,cur,db_name):
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    q = "DROP DATABASE IF EXISTS {0}"
    sql_ids = [db_name]
    out1 = query(q,sql_ids,[],con,cur)

    q = "CREATE DATABASE {0}"
    out2 = query(q,sql_ids,[],con,cur)
    return out1,out2


# TODO where should this happen?
def fill_composing_reporting_unit_join(session):
    print('Filling ComposingReportingUnitJoin table, i.e., recording nesting relations of ReportingUnits')
    ru_dframe = pd.read_sql_table('ReportingUnit',session.bind,'cdf',index_col=None)
    cruj_dframe = append_to_composing_reporting_unit_join(session,ru_dframe)
    return cruj_dframe


def append_to_composing_reporting_unit_join(session,ru):
    """<ru> is a dframe of reporting units, with cdf internal name in column 'Name'.
    cdf internal name indicates nesting via semicolons.
    This routine calculates the nesting relationships from the Names and uploads to db.
    Returns the *all* CRUJ data from the db."""
    ru['split'] = ru['Name'].apply(lambda x:x.split(';'))
    ru['length'] = ru['split'].apply(len)
    
    # pull ReportingUnit to get ids matched to names
    ru_cdf = pd.read_sql_table('ReportingUnit',session.bind,index_col=None)
    ru_static = ru.copy()
    # get Id of the child reporting unit, if it's not already there
    if 'Id' not in ru.columns:
        ru_static = ru_static.merge(ru_cdf[['Name','Id']],on='Name',how='left')
    cruj_dframe_list = []
    for i in range(ru['length'].max() - 1):
        # check that all components of all Reporting Units are themselves ReportingUnits
        ru_for_cruj = ru_static.copy()  # start fresh, without detritus from previous i

        # get name of ith ancestor
        ru_for_cruj['ancestor_{}'.format(i)] = ru_static['split'].apply(lambda x:';'.join(x[:-i - 1]))
        # get Id of ith ancestor
        ru_for_cruj = ru_for_cruj.merge(ru_cdf,left_on='ancestor_{}'.format(i),right_on='Name',
                                        suffixes=['','_' + str(i)])
        cruj_dframe_list.append(ru_for_cruj[['Id','Id_{}'.format(i)]].rename(
            columns={'Id':'ChildReportingUnit_Id','Id_{}'.format(i):'ParentReportingUnit_Id'}))
    if cruj_dframe_list:
        cruj_dframe = pd.concat(cruj_dframe_list)
        cruj_dframe = dframe_to_sql(cruj_dframe,session,'ComposingReportingUnitJoin')
    else:
        cruj_dframe = pd.read_sql_table('ComposingReportingUnitJoin',session.bind)
    session.flush()
    return cruj_dframe


def establish_connection(paramfile = '../jurisdictions/database.ini',db_name='postgres'):
    """Return a db connection object; if <paramfile> fails,
    return corrected <paramfile>"""
    try:
        params = ui.config(paramfile)
    except MissingSectionHeaderError as e:
        new_param = input(f'{e}\nEnter path to correct parameter file\n')
        con, paramfile = establish_connection(new_param,db_name)
        return con, paramfile
    if db_name != 'postgres':
        params['dbname']=db_name
    con = psycopg2.connect(**params)
    return con, paramfile


def sql_alchemy_connect(schema=None,paramfile=None,db_name='postgres'):
    """Returns an engine and a metadata object"""
    if not paramfile:
        paramfile = ui.pick_paramfile()
    params = ui.config(paramfile)
    if db_name != 'postgres': params['dbname'] = db_name
    # We connect with the help of the PostgreSQL URL
    url = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    url = url.format(**params)

    # The return value of create_engine() is our connection object
    engine = db.create_engine(url, client_encoding='utf8')

    # We then bind the connection to MetaData()
    meta = db.MetaData(bind=engine, reflect=True,schema=schema)

    return engine


def add_integer_cols(session,table,col_list):
    add = ','.join([f' ADD COLUMN "{c}" INTEGER' for c in col_list])
    q = f'ALTER TABLE "{table}" {add}'
    sql_ids=[]
    strs = []
    raw_query_via_SQLALCHEMY(session,q,sql_ids,strs)
    return


def drop_cols(session,table,col_list):
    drop = ','.join([f' DROP COLUMN "{c}"' for c in col_list])
    q = f'ALTER TABLE "{table}" {drop}'
    sql_ids=[]
    strs = []
    raw_query_via_SQLALCHEMY(session,q,sql_ids,strs)
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
        # test table name string
        if t[0] == '_':
            others.add(t)
        elif t[-4:] == 'Join':
            cdf_joins.add(t)
        else:
            # test columns
            cols = public[public.table_name == t].column_name.unique()
            if set(cols) == {'Id','Txt'} or set(cols) == {'Id','Selection'}:
                cdf_enumerations.add(t)
            else:
                cdf_elements.add(t)
    # TODO order cdf_elements and cdf_joins by references to one another
    return cdf_elements, cdf_enumerations, cdf_joins, others


# TODO use this?
def order_by_ref(elements,table_type,project_root):
    """
    Return <table_list> sorted by foreign key references from
    the definitions in the CDF_schema_def_info/<table_type> directory
    (Ideally this should pull from database instead, which requires
    true foreign keys everywhere.)
    """
    dir = os.path.join(project_root,'election_anomaly/CDF_schema_def_info',table_type)
    ok_list = []
    elements_to_process = list(elements)
    while elements_to_process:
        element = elements_to_process[0]
        # check foreign keys; if any refers to an elt yet to be processed, change to that elt
        #  note that any foreign keys for elements are to other elements, so it's OK to do this without considering
        #  joins first or concurrently.
        foreign_keys = pd.read_csv(os.path.join(dir,element,'foreign_keys.txt'),sep='\t')
        for i,r in foreign_keys.iterrows():
            fk_set = set(r['refers_to'].split(';'))    # lists all targets of the foreign key r['fieldname']
            try:
                element = [e for e in fk_set if e in elements_to_process].pop()
                break
            except IndexError:
                pass
        # append element to ok_list
        ok_list.append(element)
        # remove element from list of yet-to-be-processed
        elements_to_process.remove(element)

    return ok_list


def read_enums_from_db_table(sess,element):
	"""Returns list of enum names (e.g., 'CountItemType') for the given <element>.
	Identifies enums by the Other{enum} column name (e.g., 'OtherCountItemType)"""
	df = pd.read_sql_table(element,sess.bind,index_col='Id')
	other_cols = [x for x in df.columns if x[:5] == 'Other']
	enums = [x[5:] for x in other_cols]
	return enums


# TODO combine query() and raw_query_via_SQLALCHEMY()?
def query(q,sql_ids,strs,con,cur):  # needed for some raw queries, e.g., to create db and schemas
    format_args = [sql.Identifier(a) for a in sql_ids]
    cur.execute(sql.SQL(q).format(*format_args),strs)
    con.commit()
    if cur.description:
        return cur.fetchall()
    else:
        return None


def raw_query_via_SQLALCHEMY(session,q,sql_ids,strs):
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
    """Returns a list of enumerations referenced in the <element> table"""
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
    """Returns a dataframe whose index is the name of the field in the <element> table, with columns
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


def dframe_to_sql(dframe,session,table,index_col='Id',flush=True,raw_to_votecount=False,return_records='all'):
    """
    Given a dataframe <dframe >and an existing cdf db table <table>>, clean <dframe>
    (i.e., drop any columns that are not in <table>, add null columns to match any missing columns)
    append records any new records to the corresponding table in the db (and commit!)
    Return the updated dataframe, including all rows from the db and all from the dframe.
    <return_records> is a flag defaulting to "all" (return all records in db)
    but can be set to "original" to return only the records from the input <dframe>.

    """
    # pull copy of existing table
    target = pd.read_sql_table(table,session.bind,index_col=index_col)
    # VoteCount table gets added columns during raw data upload, needs special treatment

    if dframe.empty:
        if return_records == 'original':
            return dframe
        else:
            return target
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
        # TODO bug: catch anything not an integer (e.g., in MD 2018g upload)
        df_to_db.loc[:,'Count']=df_to_db['Count'].astype('int64',errors='ignore')

    # partition the columns
    dframe_only_cols = [x for x in dframe.columns if x not in target.columns]
    target_only_cols = [x for x in target.columns if x not in dframe.columns]
    intersection_cols = [x for x in target.columns if x in dframe.columns]

    # remove columns that don't exist in target table
    df_to_db = df_to_db.drop(dframe_only_cols, axis=1)

    # add columns that exist in target table but are missing from original dframe
    for c in target_only_cols:
        df_to_db.loc[:,c] = None

    appendable = pd.concat([target,target,df_to_db],sort=False).drop_duplicates(keep=False)
    # note: two copies of target ensures none of the original rows will be appended.

    # drop the Id column
    if 'Id' in appendable.columns:
        appendable = appendable.drop('Id',axis=1)
    try:
        appendable.to_sql(table, session.bind, if_exists='append', index=False)
    except sqlalchemy.exc.IntegrityError as e:
        ignore = input(f'Some record insertions into table {table} failed.\n'
                       f'It may be that the record(s) is already in the table (probably harmless).\n'
                       f'It may be due to non-unique names (might be a problem).\nContinue anyway (y/n)?\n')
        if ignore != 'y':
            ignore = input(f'Specific error is: {e}. \nContinue anyway (y/n)?\n')
            if ignore != 'y':
                raise e
    if table == 'ReportingUnit' and not appendable.empty:
        append_to_composing_reporting_unit_join(session,appendable)

    up_to_date_dframe = pd.read_sql_table(table,session.bind)
    up_to_date_dframe = format_dates(up_to_date_dframe)

    if raw_to_votecount:
        # need to drop rows that were read originally from target -- these will have null ElectionContestJoin_Id
        up_to_date_dframe=up_to_date_dframe[up_to_date_dframe['ElectionContestJoin_Id'].notnull()]
    if flush:
        session.flush()
    if return_records == 'original':
        # TODO get rid of rows not in dframe by taking inner join
        id_enhanced_dframe = dframe.merge(
            up_to_date_dframe,left_on=intersection_cols,right_on=intersection_cols,how='inner').drop(
            target_only_cols,axis=1)
        return id_enhanced_dframe
    else:
        return up_to_date_dframe


def format_dates(dframe):
    """ensure any date columns are pulled in 2020-05-20 format"""
    df = dframe.copy()
    # TODO is 'datetime64[ns]' the only thing that will show up?
    for c in df.columns[df.dtypes=='datetime64[ns]']:
        df[c] = f'{df[c].dt.date}'
    return df


def save_one_to_db(session,element,record):
    """Create a record in the <element> table corresponding to the info in the
    dictionary <record>, which is in <field>:<value> form, using db (not user-friendly)
    fields -- i.e., ids for enums and foreign keys -- excluding the Id field.
    On error, offers user chance to re-enter information"""
    # initialize
    enum_plaintext_dict = fk_plaintext_dict = {}
    changed = False
    ok = False

    # define regex for pulling info from IntegrityError
    pattern = re.compile(r'Key \((?P<fields>.+)\)=\((?P<values>.+)\) already exists.')
    while not ok:
        problems = []
        if record == {}:
            problems.append('No data given for insert.')
        try:
            df = pd.DataFrame({k:[v] for k,v in record.items()})
            df.to_sql(element,session.bind,if_exists='append',index=False)
            enum_plaintext_dict = mr.enum_plaintext_dict_from_db_record(session,element,record)
            fk_plaintext_dict = mr.fk_plaintext_dict_from_db_record(
                session,element,record,excluded=enum_plaintext_dict.keys())
            db_idx = name_to_id(session,element,record[get_name_field(element)])
        except sqlalchemy.exc.IntegrityError as e:
            if 'duplicate key value violates unique constraint' in e.orig.pgerror:
                m = pattern.search(e.orig.pgerror)
                field_str = m.group("fields")
                value_str = m.group("values")
                use_existing = input(f'Record already exists with value(s)\n\t{value_str}\n'
                                     f'in field(s)\n\t{field_str}\n'
                                     f'Use existing record (y/n?\n')
                if use_existing == 'y':
                    # pull record from db
                    fields = field_str.split(',')
                    values = value_str.split(',')
                    dupe_d = {fields[i]:values[i] for i in range(len(fields))}
                    db_idx, record = ui.pick_record_from_db(
                        session,element,known_info_d=dupe_d)
                    enum_plaintext_dict = mr.enum_plaintext_dict_from_db_record(session,element,record)
                    fk_plaintext_dict = mr.fk_plaintext_dict_from_db_record(
                        session,element,record,excluded=enum_plaintext_dict.keys())
                    ok = True
                else:
                    problems.append(f'Database integrity error: {e}')
            else:
                problems.append(f'Database integrity error: {e}')

        if problems:
            ui.report_problems(problems)
            print(f'Enter an alternative {element}')
            # TODO depending on problem, offer choice from db or filesystem?
            record, enum_plaintext_dict, fk_plaintext_dict = ui.get_record_info_from_user(
                session,element,known_info_d=record,mode='database')
            changed = True
        else:
            ok = True
    session.flush()
    return db_idx, record, enum_plaintext_dict, fk_plaintext_dict, changed


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


if __name__ == '__main__':

    print('Done')


