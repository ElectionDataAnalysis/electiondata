#!/usr/bin/python3
# database/__init__.py

import psycopg2
import sqlalchemy
import sqlalchemy.orm
import io
import csv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path
import numpy as np

# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from psycopg2 import sql
import sqlalchemy as db
import datetime
from election_data_analysis import user_interface as ui
from configparser import MissingSectionHeaderError
import pandas as pd
from election_data_analysis import munge as m
import re
from election_data_analysis.database import create_cdf_db as db_cdf
import os
from sqlalchemy import MetaData, Table, Column, Integer, Float
from typing import Optional, List
from election_data_analysis import user_interface as ui

states = """Alabama
Alaska
Arizona
Arkansas
California
Colorado
Connecticut
Delaware
Florida
Georgia
Hawaii
Idaho
Illinois
Indiana
Iowa
Kansas
Kentucky
Louisiana
Maine
Maryland
Massachusetts
Michigan
Minnesota
Mississippi
Missouri
Montana
Nebraska
Nevada
New Hampshire
New Jersey
New Mexico
New York
North Carolina
North Dakota
Ohio
Oklahoma
Oregon
Pennsylvania
Rhode Island
South Carolina
South Dakota
Tennessee
Texas
Utah
Vermont
Virginia
Washington
West Virginia
Wisconsin
Wyoming
American Samoa
Guam
Northern Mariana Islands
Puerto Rico
US Virgin Islands"""

db_pars = ["host", "port", "dbname", "user", "password"]

contest_types_model = [
    "state",
    "congressional",
    "judicial",
    "state-house",
    "state-senate",
]


def get_database_names(con):
    """Return dataframe with one column called `datname` """
    names = pd.read_sql("SELECT datname FROM pg_database", con)
    return names


def remove_database(params: dict) -> Optional[dict]:
    # initialize error dictionary
    db_err = None

    # connect to postgres, not to the target database
    postgres_params = params.copy()
    postgres_params["dbname"] = "postgres"
    try:
        with psycopg2.connect(**postgres_params) as conn:
            with conn.cursor() as cur:
                conn.autocommit = True

                # close any active sessions
                q = sql.SQL(
                    """SELECT pg_terminate_backend(pg_stat_activity.pid) 
                    FROM pg_stat_activity 
                    WHERE pg_stat_activity.datname = %s 
                    AND pid <> pg_backend_pid();"""
                )
                cur.execute(q, (params["dbname"],))

                # drop database
                q = sql.SQL(
                    "DROP DATABASE IF EXISTS {dbname}"
                ).format(dbname=sql.Identifier(params["dbname"]))
                cur.execute(q)
    except Exception as e:
        db_err = ui.add_new_error(
            db_err,
            "system",
            "database.remove_database",
            f"Error dropping database {params}:\n{e}",
        )

    return db_err


def create_database(con, cur, db_name):
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    q = sql.SQL("DROP DATABASE IF EXISTS {db_name}").format(
        db_name=sql.Identifier(db_name)
    )
    cur.execute(q)
    con.commit()
    if cur.description:
        out1 = cur.fetchall()
    else:
        out1 = None

    q = sql.SQL("CREATE DATABASE {db_name}").format(db_name=sql.Identifier(db_name))
    cur.execute(q)
    con.commit()
    if cur.description:
        out2 = cur.fetchall()
    else:
        out2 = None
    return out1, out2


# TODO move to more appropriate module?
def append_to_composing_reporting_unit_join(engine, ru):
    """<ru> is a dframe of reporting units, with cdf internal name in column 'Name'.
    cdf internal name indicates nesting via semicolons `;`.
    This routine calculates the nesting relationships from the Names and uploads to db.
    Returns the *all* composing-reporting-unit-join data from the db.
    By convention, a ReportingUnit is it's own ancestor (ancestor_0)."""
    working = ru.copy()
    if not working.empty:
        working["split"] = working["Name"].apply(lambda x: x.split(";"))
        working["length"] = working["split"].apply(len)

        # pull ReportingUnit to get ids matched to names
        ru_cdf = pd.read_sql_table("ReportingUnit", engine, index_col=None)
        ru_static = working.copy()

        # add db Id column to ru_static, if it's not already there
        if "Id" not in working.columns:
            ru_static = ru_static.merge(ru_cdf[["Name", "Id"]], on="Name", how="left")

        # create a list of rows to append to the ComposingReportingUnitJoin element
        cruj_dframe_list = []
        for i in range(working["length"].max()):
            # check that all components of all Reporting Units are themselves ReportingUnits
            ru_for_cruj = (
                ru_static.copy()
            )  # start fresh, without detritus from previous i

            # get name of ith ancestor
            #  E.g., ancestor_0 is largest ancestor (i.e., shortest string, often the state); ancestor_1 is the second-largest parent, etc.
            ru_for_cruj[f"ancestor_{i}"] = ru_static["split"].apply(
                lambda x: ";".join(x[: i + 1])
            )
            # get Id of ith ancestor
            ru_for_cruj = ru_for_cruj.merge(
                ru_cdf, left_on=f"ancestor_{i}", right_on="Name", suffixes=["", f"_{i}"]
            )
            cruj_dframe_list.append(
                ru_for_cruj[["Id", f"Id_{i}"]].rename(
                    columns={
                        "Id": "ChildReportingUnit_Id",
                        f"Id_{i}": "ParentReportingUnit_Id",
                    }
                )
            )
        if cruj_dframe_list:
            cruj_dframe = pd.concat(cruj_dframe_list)
            insert_to_cdf_db(engine, cruj_dframe, "ComposingReportingUnitJoin")

    cruj_dframe = pd.read_sql_table("ComposingReportingUnitJoin", engine)

    return cruj_dframe


def test_connection(paramfile="run_time.ini", dbname=None) -> (bool, dict):
    """Check for DB and relevant tables; if they don't exist, return
    error message"""

    # initialize error dictionary
    err = None

    try:
        params = ui.get_runtime_parameters(
            required_keys=db_pars, param_file=paramfile, header="postgresql"
        )[0]
    except MissingSectionHeaderError as e:
        return {"message": "database.ini file not found suggested location."}

    # use dbname from paramfile, unless dbname is passed
    if dbname:
        params["dbname"] = dbname

    # check connection before proceeding
    try:
        con = psycopg2.connect(**params)
        con.close()
    except psycopg2.OperationalError as e:
        err = ui.add_new_error(
            err,
            "system",
            "database.test_connection",
            f"Error connecting to database: {e}",
        )
        return False, err

    # Look for tables
    try:
        engine, new_err = sql_alchemy_connect(paramfile)
        if new_err:
            err = ui.consolidate_errors(err, new_err)
            engine.dispose()
            return False, err
        elems, enums, joins, o = get_cdf_db_table_names(engine)
        # All tables except "Others" must be created. Essentially looks for
        # a "complete" database.
        if not elems or not enums or not joins:
            err = ui.add_new_error(
                err,
                "system",
                "database.test_connection",
                "Required tables not found in database",
            )
            engine.dispose()
            return False, err
        engine.dispose()

    except Exception as e:
        err = ui.add_new_error(
            err,
            "system",
            "database.test_connection",
            f"Unexpected exception while connecting to database: {e}",
        )
        return False, err
    # if no errors found, return True
    return True, err


def create_or_reset_db(
    param_file: str = "run_time.ini",
    dbname: Optional[str] = None,
) -> Optional[dict]:
    """if no dbname is given, name will be taken from param_file"""

    project_root = Path(__file__).absolute().parents[1]
    params, err = ui.get_runtime_parameters(
        required_keys=db_pars, param_file=param_file, header="postgresql"
    )
    if err:
        return err

    # use dbname from param_file, unless another dbname was given
    if dbname is None:
        dbname = params["dbname"]

    # get connection to default postgres DB to create new DB
    try:
        postgres_params = params
        postgres_params["dbname"] = "postgres"
        con = psycopg2.connect(**postgres_params)
    except Exception as e:
        # Can't connect to the default postgres database, so there
        # seems to be something wrong with connection. Fail here.
        print(f"Error connecting to database. Exiting.")
        quit()
        con = None  # to keep syntax-checker happy

    cur = con.cursor()
    db_df = get_database_names(con)

    # if dbname already exists.
    if dbname in db_df.datname.unique():
        # reset DB to blank
        eng_new, err = sql_alchemy_connect(param_file, dbname=dbname)
        Session_new = sqlalchemy.orm.sessionmaker(bind=eng_new)
        sess_new = Session_new()
        db_cdf.reset_db(
            sess_new,
            os.path.join(project_root, "CDF_schema_def_info"),
        )
    else:
        create_database(con, cur, dbname)
        eng_new, err = sql_alchemy_connect(param_file, dbname=dbname)
        Session_new = sqlalchemy.orm.sessionmaker(bind=eng_new)
        sess_new = Session_new()

    # TODO tech debt: does reset duplicate work here?
    # load cdf tables
    db_cdf.create_common_data_format_tables(
        sess_new,
        dirpath=os.path.join(project_root, "CDF_schema_def_info"),
    )
    db_cdf.fill_standard_tables(
        sess_new,
        None,
        dirpath=os.path.join(project_root, "CDF_schema_def_info"),
    )
    con.close()
    return err


def sql_alchemy_connect(
    param_file: str = "run_time.ini", dbname: Optional[str] = None
) -> (sqlalchemy.engine, Optional[dict]):
    """Returns an engine and a metadata object"""
    params, err = ui.get_runtime_parameters(
        required_keys=db_pars, param_file=param_file, header="postgresql"
    )
    if err:
        return None, err

    # if dbname was given, use it instead of name in paramfile
    if dbname:
        params["dbname"] = dbname

    # We connect with the help of the PostgreSQL URL
    url = "postgresql://{user}:{password}@{host}:{port}/{dbname}"
    url = url.format(**params)

    # The return value of create_engine() is our connection object
    engine = db.create_engine(
        url, client_encoding="utf8", pool_size=20, max_overflow=40
    )
    return engine, err


def create_db_if_not_ok(dbname: Optional[str] = None) -> Optional[dict]:
    # create db if it does not already exist and have right tables
    ok, err = test_connection(dbname=dbname)
    if not ok:
        create_or_reset_db(dbname=dbname)
    return err


def get_cdf_db_table_names(eng):
    """This is postgresql-specific"""
    db_columns = pd.read_sql_table("columns", eng, schema="information_schema")
    public = db_columns[db_columns.table_schema == "public"]
    cdf_elements = set()
    cdf_enumerations = set()
    cdf_joins = set()
    others = set()
    for t in public.table_name.unique():
        # main_routines element name string
        if t[0] == "_":
            others.add(t)
        elif t[-4:] == "Join":
            cdf_joins.add(t)
        else:
            # main_routines columns
            cols = public[public.table_name == t].column_name.unique()
            if set(cols) == {"Id", "Txt"} or set(cols) == {"Id", "Selection"}:
                cdf_enumerations.add(t)
            else:
                cdf_elements.add(t)
    # TODO order cdf_elements and cdf_joins by references to one another
    return cdf_elements, cdf_enumerations, cdf_joins, others


def name_from_id(cursor, element, idx):
    name_field = get_name_field(element)
    q = sql.SQL('SELECT {name_field} FROM {element} WHERE "Id" = %s').format(
        name_field=sql.Identifier(name_field), element=sql.Identifier(element)
    )
    try:
        cursor.execute(q, [idx])
        name = cursor.fetchall()[0][0]
    except KeyError:
        # if no record with Id = <idx> was found
        name = None
    return name


def name_to_id_cursor(cursor, element, name) -> Optional[int]:
    if element == "CandidateContest":
        q = sql.SQL(
            'SELECT "Id" FROM "Contest" where "Name" = %s AND contest_type = \'Candidate\''
        )
    elif element == "BallotMeasureContest":
        q = sql.SQL(
            'SELECT "Id" FROM "Contest" where "Name" = %s AND contest_type = \'BallotMeasure\''
        )
    else:
        name_field = get_name_field(element)
        q = sql.SQL('SELECT "Id" FROM {element} where {name_field} = %s').format(
            element=sql.Identifier(element), name_field=sql.Identifier(name_field)
        )
    cursor.execute(q, [name])
    try:
        idx = cursor.fetchone()[0]
    except Exception:
        # if no record with name <name> was found
        idx = None
    return idx


def name_to_id(session, element, name) -> int:
    """ Condition can be a field/value pair, e.g., ('contest_type','Candidate')"""
    connection = session.bind.raw_connection()
    cursor = connection.cursor()
    idx = name_to_id_cursor(cursor, element, name)

    connection.close()
    return idx


def get_name_field(element):
    if element in [
        "CountItemType",
        "ElectionType",
        "IdentifierType",
        "ReportingUnitType",
    ]:
        field = "Txt"
    elif element in ["CandidateSelection", "BallotMeasureSelection"]:
        field = "Id"
    elif element == "Candidate":
        field = "BallotName"
    elif element == "_datafile":
        field = "short_name"
    else:
        field = "Name"
    return field


def insert_to_cdf_db(
    engine, df, element, sep="\t", encoding="utf_8", timestamp=None
) -> str:
    """Inserts any new records in <df> into <element>; if <element> has a timestamp column
    it must be specified in <timestamp>; <df> must have columns matching <element>,
    except Id and <timestamp> if any. Returns an error message (or None)"""

    working = df.copy()
    if element == "Candidate":
        # regularize name and drop dupes
        working["BallotName"] = m.regularize_candidate_names(working["BallotName"])
        working.drop_duplicates(inplace=True)

    # initialize connection and cursor
    connection = engine.raw_connection()
    cursor = connection.cursor()

    # identify new ReportingUnits, must later enter nesting info in db
    if element == "ReportingUnit":
        working = m.clean_strings(working, ["Name"])
        # find any new RUs
        matched_with_old = append_id_to_dframe(
            engine, working, "ReportingUnit", {"Name": "Name"}
        )

    # name temp table by username and timestamp to avoid conflict
    temp_table = table_named_to_avoid_conflict(engine, "__temp_insert")

    if element in [
        "BallotMeasureSelection",
        "BallotMeasureContest",
        "CandidateSelection",
        "CandidateContest",
    ]:
        # create temp table with Id column
        q = sql.SQL(
            "CREATE TABLE {temp_table} AS TABLE {element} WITH NO DATA;"
        ).format(element=sql.Identifier(element), temp_table=sql.Identifier(temp_table))
    else:
        # create temp table without Id
        q = sql.SQL(
            'CREATE TABLE {temp_table} AS TABLE {element} WITH NO DATA; ALTER TABLE {temp_table} DROP COLUMN "Id";'
        ).format(element=sql.Identifier(element), temp_table=sql.Identifier(temp_table))

    cursor.execute(q)
    if timestamp:
        # drop timestamp column
        q = sql.SQL("ALTER TABLE {temp_table} DROP COLUMN {ts_col}").format(
            ts_col=sql.Identifier(timestamp), temp_table=temp_table
        )
        cursor.execute(q)
        working = working.drop([timestamp], axis=1)
    connection.commit()

    # make sure datatypes of working match the types of target
    # get set <mixed_int> of cols with integers & nulls and kludge only those
    temp_columns, type_map = get_column_names(cursor, temp_table)
    mixed_int = [
        c  # TODO Selection_Id was numerical but not int here for AZ (xml)
        for c in temp_columns
        if type_map[c] == "integer" and working[c].dtype != "int64"
    ]
    for c in mixed_int:
        # set nulls to 0 (kludge because pandas can't have NaN in 'int64' column)
        working[c] = working[c].fillna(0).astype("int64", errors="ignore")

    # Prepare data
    output = io.StringIO()
    temp_only_cols = [c for c in temp_columns if c not in working.columns]

    # add any missing columns needed for temp table to working
    for c in temp_only_cols:
        working = m.add_constant_column(working, c, None)
    working[temp_columns].drop_duplicates().to_csv(
        output,
        sep=sep,
        header=False,
        encoding=encoding,
        index=False,
        quoting=csv.QUOTE_MINIMAL,
    )
    # set current position for the StringIO object to the beginning of the string
    output.seek(0)

    # Insert data
    q_copy = sql.SQL("COPY {temp_table} FROM STDOUT").format(
        temp_table=sql.Identifier(temp_table)
    )
    try:
        cursor.copy_expert(q_copy, output)
        connection.commit()

        #  undo kludge (see above) setting 0 values to nulls inside db in temp table
        for c in mixed_int:
            q_kludge = sql.SQL(
                "UPDATE {temp_table} SET {c} = NULL WHERE {c} = 0"
            ).format(temp_table=sql.Identifier(temp_table), c=sql.Identifier(c))
            cursor.execute(q_kludge)
        connection.commit()  # TODO when Selection_Id was in mixed_int, this emptied temp table, why?

        # insert records from temp table into <element> table
        q = sql.SQL(
            "INSERT INTO {t}({fields}) SELECT * FROM {temp_table} ON CONFLICT DO NOTHING"
        ).format(
            t=sql.Identifier(element),
            fields=sql.SQL(",").join([sql.Identifier(x) for x in temp_columns]),
            temp_table=sql.Identifier(temp_table),
        )
        cursor.execute(q)
        connection.commit()
        error_str = None
    except Exception as e:
        print(e)
        error_str = f"{e}"

    # remove temp table
    q = sql.SQL("DROP TABLE {temp_table}").format(temp_table=sql.Identifier(temp_table))
    cursor.execute(q)

    if element == "ReportingUnit":
        # check get RUs not matched and process them
        mask = matched_with_old.ReportingUnit_Id > 0
        new_rus = matched_with_old[~mask]
        if not new_rus.empty:
            append_to_composing_reporting_unit_join(engine, new_rus)

    connection.commit()
    cursor.close()
    connection.close()
    return error_str


def table_named_to_avoid_conflict(engine, prefix: str) -> str:
    p = re.compile("postgresql://([^:]+)")
    user_name = p.findall(str(engine.url))[0]
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    temp_table = f"{prefix}_{user_name}_{ts}"
    return temp_table


def append_id_to_dframe(
    engine: sqlalchemy.engine,
    df: pd.DataFrame,
    element: str,
    col_map: Optional[dict] = None,
) -> pd.DataFrame:
    """Using <col_map> to map columns of <df> onto defining columns of <table>, returns
    a copy of <df> with appended column <table>_Id. Unmatched items returned with null value for <table>_Id"""
    if col_map is None:
        col_map = {element: get_name_field(element)}

    if element == "Candidate":
        # regularize names
        for k, v in col_map.items():
            if v == "BallotName" and k in df.columns:
                df[k] = m.regularize_candidate_names(df[k])

    connection = engine.raw_connection()

    temp_table = table_named_to_avoid_conflict(engine, "__temp_append")

    df_cols = list(col_map.keys())

    # create temp db table with info from df, without index
    id_cols = [c for c in df.columns if c[-3:] == "_Id"]
    df, err_df = m.clean_ids(df, id_cols)
    df[df_cols].fillna("").to_sql(temp_table, engine, index_label="dataframe_index")
    # TODO fillna('') probably redundant

    # join <table>_Id
    on_clause = sql.SQL(" AND ").join(
        [
            sql.SQL("t.{t_col} = tt.{tt_col}").format(
                t_col=sql.Identifier(col_map[c]), tt_col=sql.Identifier(c)
            )
            for c in df_cols
        ]
    )

    q = sql.SQL(
        "SELECT t.*, tt.dataframe_index FROM {tt} tt LEFT JOIN {t} t ON {on_clause}"
    ).format(
        tt=sql.Identifier(temp_table), t=sql.Identifier(element), on_clause=on_clause
    )
    w = pd.read_sql_query(q, connection).set_index("dataframe_index")

    # drop temp db table
    q = sql.SQL("DROP TABLE {temp_table}").format(temp_table=sql.Identifier(temp_table))
    cur = connection.cursor()
    cur.execute(q)
    connection.commit()
    connection.close()
    df_appended = df.join(w[["Id"]]).rename(columns={"Id": f"{element}_Id"})
    df_appended, err_df = m.clean_ids(df_appended, "Id")
    return df_appended


def get_column_names(cursor, table: str) -> (list, dict):
    q = sql.SQL(
        """SELECT column_name, data_type FROM information_schema.columns 
        WHERE table_schema = 'public' AND table_name = %s"""
    )
    cursor.execute(q, [table])
    results = cursor.fetchall()
    col_list = [x for (x, y) in results]
    type_map = {x: y for (x, y) in results}
    return col_list, type_map


def add_records_to_selection_table(engine, n: int) -> list:
    "Returns a list of the Ids of the inserted records"
    id_list = []
    connection = engine.raw_connection()
    cursor = connection.cursor()
    q = sql.SQL('INSERT INTO "Selection" DEFAULT VALUES RETURNING "Id";')
    for k in range(n):
        cursor.execute(q)
        id_list.append(cursor.fetchall()[0][0])
    connection.commit()
    cursor.close()
    connection.close()
    return id_list


def vote_type_list(cursor, datafile_list: list, by: str = "Id") -> (list, str):
    if len(datafile_list) == 0:
        return [], "No vote types found because no datafiles listed"

    q = sql.SQL(
        """
        SELECT distinct CIT."Txt"
        FROM "VoteCount" VC
        LEFT JOIN _datafile d on VC."_datafile_Id" = d."Id"
        LEFT JOIN "CountItemType" CIT on VC."CountItemType_Id" = CIT."Id"
        WHERE d.{by} in %s
    """
    ).format(by=sql.Identifier(by))
    try:
        cursor.execute(q, [tuple(datafile_list)])
        vt_list = [x for (x,) in cursor.fetchall()]
        err_str = None
    except Exception as exc:
        err_str = f"Database error pulling list of vote types: {exc}"
        vt_list = None
    return vt_list, err_str


def data_file_list(
    cursor, election_id, reporting_unit_id: Optional[int] = None, by="Id"
):
    q = sql.SQL(
        """SELECT distinct d.{by} FROM _datafile d WHERE d."Election_Id" = %s"""
    ).format(by=sql.Identifier(by))
    if reporting_unit_id:
        q += sql.SQL(""" AND d."ReportingUnit_Id" = %s""")
        id_tup = (election_id, reporting_unit_id)
    else:
        id_tup = (election_id,)
    try:
        cursor.execute(q, id_tup)
        df_list = [x for (x,) in cursor.fetchall()]
        err_str = None
    except Exception as exc:
        err_str = f"Database error pulling list of datafiles with election id in {election_id}: {exc}"
        df_list = None
    return df_list, err_str


def active_vote_types_from_ids(cursor, election_id=None, jurisdiction_id=None):
    if election_id:
        if jurisdiction_id:
            q = """SELECT distinct cit."Txt"
                FROM "VoteCount" vc LEFT JOIN "CountItemType" cit
                ON vc."CountItemType_Id" = cit."Id"
                LEFT JOIN "ComposingReportingUnitJoin" cruj
                on cruj."ChildReportingUnit_Id" = vc."ReportingUnit_Id"
                AND cruj."ChildReportingUnit_Id" = vc."ReportingUnit_Id"
                WHERE vc."Election_Id" = %s AND cruj."ParentReportingUnit_Id" = %s 
                """
            str_vars = (election_id, jurisdiction_id)
        else:  # if election_id but no jurisdiction_id
            q = """SELECT distinct cit."Txt"
                FROM "VoteCount" vc LEFT JOIN "CountItemType" cit
                ON vc."CountItemType_Id" = cit."Id"
                LEFT JOIN "ComposingReportingUnitJoin" cruj
                on cruj."ChildReportingUnit_Id" = vc."ReportingUnit_Id"
                AND cruj."ChildReportingUnit_Id" = vc."ReportingUnit_Id"
                WHERE vc."Election_Id" = %s 
                """
            str_vars = (election_id,)

    elif jurisdiction_id:   # if jurisdiction_id but no election_id
        q = """SELECT distinct cit."Txt"
            FROM "VoteCount" vc LEFT JOIN "CountItemType" cit
            ON vc."CountItemType_Id" = cit."Id"
            LEFT JOIN "ComposingReportingUnitJoin" cruj
            on cruj."ChildReportingUnit_Id" = vc."ReportingUnit_Id"
            AND cruj."ChildReportingUnit_Id" = vc."ReportingUnit_Id"
            WHERE cruj."ParentReportingUnit_Id" = %s 
            """
        str_vars = (jurisdiction_id,)
    else:
        q = """SELECT distinct cit."Txt"
                FROM "VoteCount" vc LEFT JOIN "CountItemType" cit
                ON vc."CountItemType_Id" = cit."Id"
                LEFT JOIN "ComposingReportingUnitJoin" cruj
                on cruj."ChildReportingUnit_Id" = vc."ReportingUnit_Id"
                AND cruj."ChildReportingUnit_Id" = vc."ReportingUnit_Id"
                """
        str_vars = tuple()

    cursor.execute(q,str_vars)

    aa = cursor.fetchall()
    active_list = [x for (x,) in aa]
    return active_list


def active_vote_types(session, election, jurisdiction):
    """Gets a list of the vote types for the given election and jurisdiction"""

    election_id = name_to_id(session,"Election",election)
    jurisdiction_id = name_to_id(session, "ReportingUnit", jurisdiction)
    connection = session.bind.raw_connection()
    cursor = connection.cursor()
    active_list = active_vote_types_from_ids(cursor, election_id=election_id, jurisdiction_id=jurisdiction_id)
    cursor.close()
    connection.close()
    return active_list


def remove_vote_counts(connection, cursor, id: int, active_confirm: bool = True) -> str:
    """Remove all VoteCount data from a particular file, and remove that file from _datafile"""
    try:
        q = 'SELECT "Id", file_name, download_date, created_at, is_preliminary FROM _datafile WHERE _datafile."Id"=%s;'
        cursor.execute(q, [id])
        (datafile_id, file_name, download_date, created_at, preliminary) = cursor.fetchall()[0]
        record = "\n\t".join([
            f"datafile_id: {datafile_id}",
            f"file_name: {file_name}",
            f"download_date: {download_date}",
            f"datafile_id: {datafile_id}",
            f"created_at: {created_at}",
            f"preliminary: {preliminary}",
        ])
    except KeyError as exc:
        return f"No datafile found with Id = {id}"
    if active_confirm:
        confirm = input(
            f"Confirm: delete all VoteCount data from this results file (y/n)?\n\t{record}\n"
        )
    # if active_confirm is False, consider it confirmed.
    else:
        confirm = "y"
    if confirm == "y":
        try:
            q = 'DELETE FROM "VoteCount" where "_datafile_Id"=%s;Delete from _datafile where "Id"=%s;'
            cursor.execute(q, [id, id])
            connection.commit()
            print(f'{file_name}: VoteCounts deleted from results file\n')
            err_str = None
        except Exception as exc:
            err_str = f"{file_name}: Error deleting data: {exc}"
            print(err_str)
    else:
        err_str = f"{file_name}: Deletion not confirmed by user\n"
        print(err_str)
    return err_str


def get_input_options(session, input, verbose):
    """Returns a list of response options based on the input"""
    # input comes as a pythonic (snake case) input, need to
    # change to match DB element naming format
    name_parts = input.split("_")
    search_str = "".join([name_part.capitalize() for name_part in name_parts])

    if search_str in [
        "Contest",
        "Election",
        "Office",
        "Party",
        "ReportingUnit",
        "BallotMeasureSelection",
    ]:
        column_name = "Name"
        table_search = True
    elif search_str in [
        "CountItemStatus",
        "CountItemType",
        "ElectionType",
        "IdentifierType",
        "ReportingUnitType",
    ]:
        column_name = "Txt"
        table_search = True
    elif search_str == "Candidate":
        column_name = "BallotName"
        table_search = True
    elif search_str in [
        "CandidateContest",
        "BallotMeasureContest",
    ]:
        pass
    # TODO: do we need a subdivision_type?
    else:
        search_str = search_str.lower()
        table_search = False

    connection = session.bind.raw_connection()
    cursor = connection.cursor()
    if not verbose:
        if table_search:
            q = sql.SQL("SELECT {column_name} FROM {search_str};").format(
                column_name=sql.Identifier(column_name),
                search_str=sql.Identifier(search_str),
            )
            cursor.execute(q)
        else:
            q = sql.SQL(
                """
                SELECT "BallotName" 
                FROM "Candidate"
                WHERE "BallotName" ~* %s
            """
            )
            cursor.execute(q, [search_str])
        result = cursor.fetchall()
        connection.close()
        return [r[0] for r in result]
    else:
        # election result are handled differently than the rest of the flow because
        # it's the first selection made
        if search_str == "Election":
            result = session.execute(
                f"""
                SELECT  e."Id" AS parent, "Name" AS name, "Txt" as type
                FROM    "VoteCount" vc
                        JOIN "Election" e ON vc."Election_Id" = e."Id"
                        JOIN "ElectionType" et ON e."ElectionType_Id" = et."Id"
                WHERE   "Name" != 'none or unknown'
                GROUP BY e."Id", "Name", "Txt"
                ORDER BY LEFT("Name", 4) DESC, RIGHT("Name", LENGTH("Name") - 5)
            """
            )
            result_df = pd.DataFrame(result)
            result_df.columns = result.keys()
            return package_display_results(result_df)
        elif search_str == "jurisdiction":
            q = sql.SQL(
                """
                WITH states(states) AS (
                    SELECT {states} 
                )
                , unnested AS (
                    SELECT    UNNEST(regexp_split_to_array(states, '\n')) AS jurisdiction
                    FROM    states
                )

                , ordered AS (
                    SELECT  *, ROW_NUMBER() OVER() AS order_by
                    FROM    unnested u
                )
 				, crossed AS (
					SELECT	"Id", "Name", jurisdiction, 
                            ROW_NUMBER() OVER(ORDER BY o.order_by ASC, order_by, LEFT("Name", 4) DESC, RIGHT("Name", LENGTH("Name") - 5) ASC) as order_by
                    FROM	"Election" e
                    		CROSS JOIN ordered o
                    WHERE	"Name" != 'none or unknown'
                    ORDER BY o.order_by ASC, order_by, LEFT("Name", 4) DESC, RIGHT("Name", LENGTH("Name") - 5) ASC
                )
                , crossed_with_state_id as (
                	SELECT	c.*, ru."Id" as jurisdiction_id
                	FROM	crossed c
                    		LEFT JOIN "ReportingUnit" ru ON c.jurisdiction = ru."Name"
                )
                SELECT  "Name" as parent,
                        jurisdiction AS name, 
                        CASE WHEN d."ReportingUnit_Id" IS null THEN false ELSE true END AS type
                FROM    crossed_with_state_id s
                        LEFT JOIN (SELECT DISTINCT "Election_Id", "ReportingUnit_Id" FROM _datafile) d 
                        ON s."Id" = d."Election_Id" AND s.jurisdiction_id = d."ReportingUnit_Id"
                ORDER BY order_by
            """
            ).format(states=sql.Literal(states))
        elif search_str == "BallotMeasureContest":
            # parent_id is reporting unit, type is reporting unit type
            q = sql.SQL(
                """
                SELECT  ru."Name" AS parent,
                        c."Name" AS name, rut."Txt" AS type
                FROM    "BallotMeasureContest" bmc
                        JOIN "ReportingUnit" ru ON bmc."ElectionDistrict_Id" = ru."Id"
                        JOIN "ReportingUnitType" rut ON ru."ReportingUnitType_Id" = rut."Id"
                        JOIN "Contest" c on bmc."Id" = c."Id"
                WHERE    contest_type = 'BallotMeasure'
                ORDER BY c."Name"
            """
            )
        elif search_str == "CandidateContest":
            q = sql.SQL(
                """
                SELECT  ru."Name" AS parent,
                        c."Name" AS name, rut."Txt" AS type
                FROM    "CandidateContest" cc
                        JOIN "Office" o ON cc."Office_Id" = o."Id"
                        JOIN "ReportingUnit" ru ON o."ElectionDistrict_Id" = ru."Id"
                        JOIN "ReportingUnitType" rut ON ru."ReportingUnitType_Id" = rut."Id"
                        JOIN "Contest" c on cc."Id" = c."Id"
                WHERE    contest_type = 'Candidate'
                ORDER BY c."Name"
            """
            )
        elif search_str == "Candidate":
            q = sql.SQL(
                """
                SELECT  DISTINCT ct."Name" AS parent, c."BallotName" as name, 
                        p."Name" as type
                FROM    "Candidate" c
                        JOIN "CandidateSelection" cs ON c."Id" = cs."Candidate_Id"
                        JOIN "Party" p ON cs."Party_Id" = p."Id"
                        JOIN "VoteCount" vc on cs."Id" = vc."Selection_Id"
                        JOIN "CandidateContest" cc ON vc."Contest_Id" = cc."Id"
                        JOIN "Contest" ct on cc."Id" = ct."Id"
                ORDER BY c."BallotName"
            """
            )
        else:
            # parent_id is candidate_id, type is combo of party and contest name
            q = sql.SQL(
                """
                SELECT  DISTINCT ct."Name" AS parent, c."BallotName" as name, 
                        p."Name" as type
                FROM    "Candidate" c
                        JOIN "CandidateSelection" cs ON c."Id" = cs."Candidate_Id"
                        JOIN "Party" p ON cs."Party_Id" = p."Id"
                        JOIN "VoteCount" vc on cs."Id" = vc."Selection_Id"
                        JOIN "CandidateContest" cc ON vc."Contest_Id" = cc."Id"
                        JOIN "Contest" ct on cc."Id" = ct."Id"
                WHERE   c."BallotName" ~* {candidate}
            """
            ).format(candidate=sql.Literal(search_str))
        cursor.execute(q)
        result = cursor.fetchall()
        cursor.close()
        return result


def candidate_to_id(session, name):
    """fuzzy string matching on name field, may return multiple results"""
    name_field = get_name_field("Candidate")
    q = f"""SELECT "Id" FROM "{element}" WHERE "{name_field}" = '{name}' """
    idx_df = pd.read_sql(q, session.bind)
    try:
        idx = idx_df.loc[0, "Id"]
    except KeyError:
        # if no record with name <name> was found
        idx = None
    return idx


def package_display_results(data):
    """takes a result set and packages into JSON to return"""
    results = []
    for i, row in data.iterrows():
        if row[1] in ui.contest_type_mappings:
            row[1] = ui.contest_type_mappings[row[1]]
        temp = {"parent": row[0], "name": row[1], "type": row[2], "order_by": i + 1}
        results.append(temp)
    return results


def get_filtered_input_options(session, input_str, filters):
    df_cols = ["parent", "name", "type"]
    # election selection is handled separately because it's the first choice.
    if input_str == "jurisdiction":
        result = get_input_options(session, "jurisdiction", verbose=True)
        result_df = pd.DataFrame(result)
        result_df.columns = df_cols
        df = result_df[result_df["parent"].isin(filters)]
    # contest_type is a special case because we don't have a contest_type table.
    # instead, this is the reporting unit type of the election district
    elif input_str == "contest_type":
        contest_df = get_relevant_contests(session, filters)
        contest_types = contest_df["type"].unique()
        contest_types.sort()
        data = {
            "parent": [filters[0] for contest_type in contest_types],
            "name": contest_types,
            "type": [None for contest_type in contest_types],
        }
        df = pd.DataFrame(data=data)
    elif input_str == "contest":
        contest_type = list(set(contest_types_model) & set(filters))[0]

        connection = session.bind.raw_connection()
        cursor = connection.cursor()
        reporting_unit_id = list_to_id(session, "ReportingUnit", filters)
        reporting_unit = name_from_id(cursor, "ReportingUnit", reporting_unit_id)
        connection.close()

        contest_type_df = pd.DataFrame(
            [
                {
                    "parent": reporting_unit,
                    "name": f"All {ui.contest_type_mappings[contest_type]}",
                    "type": contest_type,
                }
            ]
        )
        contest_df = get_relevant_contests(session, filters)
        contest_df = contest_df[contest_df["type"].isin(filters)]
        df = pd.concat([contest_type_df, contest_df])
    # Assume these others are candidate searching. This is handled differently
    # because the results variable is structured slightly differently
    elif input_str == "subdivision_type":
        # TODO: refactor this ugly mess
        hierarchy_df = pd.read_sql_table(
            "ComposingReportingUnitJoin", session.bind, index_col="Id"
        )
        unit_df = pd.read_sql_table("ReportingUnit", session.bind, index_col="Id")
        hierarchy_df = hierarchy_df.merge(
            unit_df, how="inner", left_on="ParentReportingUnit_Id", right_on="Id"
        )
        hierarchy_df = hierarchy_df[hierarchy_df["Name"].isin(filters)]
        hierarchy_df = hierarchy_df.merge(
            unit_df,
            how="inner",
            left_on="ChildReportingUnit_Id",
            right_on="Id",
            suffixes=["_x", None],
        )[unit_df.columns]
        unit_type_df = pd.read_sql_table(
            "ReportingUnitType", session.bind, index_col="Id"
        )
        hierarchy_df = hierarchy_df.merge(
            unit_type_df, how="inner", left_on="ReportingUnitType_Id", right_on="Id"
        )
        subdivision_types = list(hierarchy_df["Txt"].unique())
        # Currently we don't distinguish between "location" RU types (like county)
        # and "office" RU types (like state-senate, judicial). For now, we're
        # hard-coding the location types to keep, though this may change in the future.
        types_to_keep = ["county", "precinct", "ward"]
        subdivision_types = list(set(subdivision_types) & set(types_to_keep))
        subdivision_types.sort()
        data = {
            "parent": [filters[0] for subdivision_types in subdivision_types],
            "name": subdivision_types,
            "type": [None for subdivision_types in subdivision_types],
        }
        df = pd.DataFrame(data=data)
    elif input_str == "election":
        election_df = get_relevant_election(session, filters)
        elections = list(election_df["Name"].unique())
        elections.sort(reverse=True)
        data = {
            "parent": [filters[0] for election in elections],
            "name": elections,
            "type": [None for election in elections],
        }
        df = pd.DataFrame(data=data)
        df[["year", "election_type"]] = df["name"].str.split(" ", expand=True)
        df.sort_values(["year", "election_type"], ascending=[False, True], inplace=True)
        df.drop(columns=["year", "election_type"], inplace=True)
    elif input_str == "category":
        election_id = list_to_id(session, "Election", filters)
        reporting_unit_id = list_to_id(session, "ReportingUnit", filters)

        # get the census data
        connection = session.bind.raw_connection()
        cursor = connection.cursor()
        election = name_from_id(cursor, "Election", election_id)
        census_df = read_external(cursor, int(election[0:4]), reporting_unit_id, ["Label"])
        cursor.close()
        if census_df.empty:
            census = []
        else:
            census = ["Census data"]

        type_df = read_vote_count(
            session,
            election_id,
            reporting_unit_id,
            ["CountItemType_Id"],
            ["CountItemType_Id"],
        )
        count_type_ids = type_df["CountItemType_Id"].unique()
        count_types_df = pd.read_sql_table(
            "CountItemType", session.bind, index_col="Id"
        )
        count_types = list(
            count_types_df[count_types_df.index.isin(count_type_ids)]["Txt"].unique()
        )
        count_types.sort()
        data = {
            "parent": [filters[0] for count_type in count_types]
            + [filters[0] for count_type in count_types]
            + [filters[0] for count_type in count_types]
            + [filters[0] for c in census],
            "name": [f"Candidate {count_type}" for count_type in count_types]
            + [f"Contest {count_type}" for count_type in count_types]
            + [f"Party {count_type}" for count_type in count_types]
            + [c for c in census],
            "type": [None for count_type in count_types]
            + [None for count_type in count_types]
            + [None for count_type in count_types]
            + [None for c in census],
        }
        df = pd.DataFrame(data=data)
    # check if it's looking for a count of contests
    elif input_str == "count" and bool([f for f in filters if f.startswith("Contest")]):
        election_id = list_to_id(session, "Election", filters)
        reporting_unit_id = list_to_id(session, "ReportingUnit", filters)
        df = read_vote_count(
            session,
            election_id,
            reporting_unit_id,
            ["ReportingUnitName", "ContestName", "unit_type"],
            ["parent", "name", "type"],
        )
        df = df.sort_values(["parent", "name"]).reset_index(drop=True)
    # check if it's looking for a count of candidates
    elif input_str == "count" and bool(
        [f for f in filters if f.startswith("Candidate")]
    ):
        election_id = list_to_id(session, "Election", filters)
        reporting_unit_id = list_to_id(session, "ReportingUnit", filters)
        df_unordered = read_vote_count(
            session,
            election_id,
            reporting_unit_id,
            ["ContestName", "BallotName", "PartyName", "unit_type"],
            ["parent", "name", "type", "unit_type"],
        )
        df = clean_candidate_names(df_unordered)
        df = df[["parent", "name", "unit_type"]].rename(columns={"unit_type": "type"})
    # check if it's looking for census data
    elif input_str == "count" and "Census data" in filters:
        election_id = list_to_id(session, "Election", filters)
        reporting_unit_id = list_to_id(session, "ReportingUnit", filters)
        connection = session.bind.raw_connection()
        cursor = connection.cursor()
        election = name_from_id(cursor, "Election", election_id)
        df = read_external(
            cursor, int(election[0:4]), reporting_unit_id, ["Source", "Label", "Category"]
        )
        cursor.close()
    # check if it's looking for a count by party
    elif input_str == "count":
        election_id = list_to_id(session, "Election", filters)
        reporting_unit_id = list_to_id(session, "ReportingUnit", filters)
        df = read_vote_count(
            session,
            election_id,
            reporting_unit_id,
            ["PartyName", "unit_type"],
            ["parent", "type"],
        )
        df["name"] = df["parent"].str.replace(" Party", "") + " " + df["type"]
        df = df[df_cols].sort_values(["parent", "type"])
    else:
        election_id = list_to_id(session, "Election", filters)
        reporting_unit_id = list_to_id(session, "ReportingUnit", filters)
        df_unordered = read_vote_count(
            session,
            election_id,
            reporting_unit_id,
            ["ContestName", "BallotName", "PartyName", "unit_type"],
            ["parent", "name", "type", "unit_type"],
        )
        df_unordered = df_unordered[df_unordered["unit_type"].isin(filters)].copy()
        df_filtered = df_unordered[
            df_unordered["name"].str.contains(input_str, case=False)
        ].copy()
        df = clean_candidate_names(df_filtered[df_cols].copy())
    # TODO: handle the "All" and "other" options better
    # TODO: handle sorting numbers better
    return package_display_results(df)


def get_relevant_election(session, filters):
    unit_df = pd.read_sql_table("ReportingUnit", session.bind, index_col="Id")
    unit_df = unit_df[unit_df["Name"].isin(filters)]
    election_ids = pd.read_sql_table("_datafile", session.bind, index_col="Id").merge(
        unit_df, how="inner", left_on="ReportingUnit_Id", right_on="Id"
    )["Election_Id"]
    election_df = pd.read_sql_table("Election", session.bind, index_col="Id")
    election_df = election_df[election_df.index.isin(election_ids)]
    return election_df


def get_relevant_contests(session, filters):
    """expects the filters list to have an election and jurisdiction.
    finds all contests for that combination."""
    election_id = list_to_id(session, "Election", filters)
    reporting_unit_id = list_to_id(session, "ReportingUnit", filters)
    contest_df = read_vote_count(
        session, election_id, reporting_unit_id, ["ContestName"], ["contest_name"]
    )

    result = get_input_options(session, "candidate_contest", True)
    result_df = pd.DataFrame(result)
    result_df.columns = ["parent", "name", "type"]
    result_df = result_df.merge(
        contest_df, how="inner", left_on="name", right_on="contest_name"
    )[result_df.columns]
    return result_df


def get_jurisdiction_hierarchy(session, jurisdiction_id):
    """get type of reporting unit one level down from jurisdiction.
    Omit particular types that are contest types, not true reporting unit types"""
    q = sql.SQL(
        """
        SELECT  *
        FROM    (
        SELECT  rut."Id", 1 AS ordering
        FROM    "ComposingReportingUnitJoin" cruj
                JOIN "ReportingUnit" ru on cruj."ChildReportingUnit_Id" = ru."Id"
                JOIN "ReportingUnitType" rut on ru."ReportingUnitType_Id" = rut."Id"
                CROSS JOIN (
                    SELECT  ARRAY_LENGTH(regexp_split_to_array("Name", ';'), 1) AS len 
                    FROM    "ReportingUnit" WHERE "Id" = %s
                ) l
        WHERE   rut."Txt" not in %s
                AND ARRAY_LENGTH(regexp_split_to_array("Name", ';'), 1) = len + 1
                AND "ParentReportingUnit_Id" = %s
        UNION
        -- This union accommodates Alaska without breaking other states
        SELECT  rut."Id", 2 AS ordering
        FROM    "ComposingReportingUnitJoin" cruj
                JOIN "ReportingUnit" ru on cruj."ChildReportingUnit_Id" = ru."Id"
                JOIN "ReportingUnitType" rut on ru."ReportingUnitType_Id" = rut."Id"
        WHERE   rut."Txt" not in (    
                    'state',
                    'congressional',
                    'judicial',
                    'state-senate'
                )
                AND ARRAY_LENGTH(regexp_split_to_array("Name", ';'), 1) = 2
                AND "ParentReportingUnit_Id" = %s
        ) c
        ORDER BY ordering
        LIMIT   1
    """
    )
    connection = session.bind.raw_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(q, [
            jurisdiction_id, tuple(contest_types_model), jurisdiction_id, jurisdiction_id
        ])
        result = cursor.fetchall()
        subdivision_type_id = result[0][0]
    except:
        subdivision_type_id = None
    cursor.close()
    return subdivision_type_id


def get_candidate_votecounts(session, election_id, top_ru_id, subdivision_type_id):
    connection = session.bind.raw_connection()
    cursor = connection.cursor()
    q = sql.SQL(
        """
        WITH RECURSIVE unit_hierarchy AS (
            SELECT  c.*
            FROM    "ComposingReportingUnitJoin" c
            WHERE   "ParentReportingUnit_Id" = %s
            UNION
            SELECT  c.*
            FROM    unit_hierarchy h
                    JOIN "ComposingReportingUnitJoin" c ON h."ChildReportingUnit_Id" = c."ParentReportingUnit_Id"
                    JOIN "ReportingUnit" ru ON c."ParentReportingUnit_Id" = ru."Id"
            WHERE   ru."ReportingUnitType_Id" = %s
        )
        , unit_hierarchy_named AS (
            SELECT  DISTINCT c."ParentReportingUnit_Id", c."ChildReportingUnit_Id", 
                    pru."Name" AS "ParentName", pru."ReportingUnitType_Id" AS "ParentReportingUnitType_Id",
                    cru."Name" AS "ChildName", cru."ReportingUnitType_Id" AS "ChildReportingUnitType_Id"
            FROM    unit_hierarchy c
                    JOIN "ReportingUnit" pru ON c."ParentReportingUnit_Id" = pru."Id"
                    JOIN "ReportingUnit" cru ON c."ChildReportingUnit_Id" = cru."Id"
        )
            SELECT  vc."Id" AS "VoteCount_Id", "Count", "CountItemType_Id",
                    vc."ReportingUnit_Id", "Contest_Id", "Selection_Id",
                    vc."Election_Id", IntermediateRU."ParentReportingUnit_Id",
                    IntermediateRU."ChildName" AS "Name", IntermediateRU."ChildReportingUnitType_Id" AS "ReportingUnitType_Id",
                    IntermediateRU."ParentName" AS "ParentName", IntermediateRU."ParentReportingUnitType_Id" AS "ParentReportingUnitType_Id",
                    CIT."Txt" AS "CountItemType", C."Name" AS "Contest",
                    Cand."BallotName" AS "Selection", "ElectionDistrict_Id", Cand."Id" AS "Candidate_Id", "contest_type",
                    EDRUT."Txt" AS "contest_district_type", p."Name" as Party
                    FROM unit_hierarchy_named IntermediateRU
                    JOIN "VoteCount" vc ON IntermediateRU."ChildReportingUnit_Id" = vc."ReportingUnit_Id"
                        AND IntermediateRU."ParentReportingUnitType_Id" = %s AND vc."Election_Id" = %s
                    JOIN "Contest" C ON vc."Contest_Id" = C."Id" AND C.contest_type = 'Candidate'
                    JOIN "CandidateSelection" CS ON CS."Id" = vc."Selection_Id"
                    JOIN "Candidate" Cand ON CS."Candidate_Id" = Cand."Id"
                    JOIN "CountItemType" CIT ON vc."CountItemType_Id" = CIT."Id"
                    JOIN "CandidateContest" ON C."Id" = "CandidateContest"."Id"
                    JOIN "Office" O ON "CandidateContest"."Office_Id" = O."Id"
                    JOIN "ReportingUnit" ED ON O."ElectionDistrict_Id" = ED."Id"
                    JOIN "ReportingUnitType" EDRUT ON ED."ReportingUnitType_Id" = EDRUT."Id"
                    JOIN "Party" p on CS."Party_Id" = p."Id"
    """
    )
    cursor.execute(
        q, [top_ru_id, subdivision_type_id, subdivision_type_id, election_id]
    )
    result = cursor.fetchall()
    result_df = pd.DataFrame(result)
    result_df.columns = [
        "VoteCount_Id",
        "Count",
        "CountItemType_Id",
        "ReportingUnit_Id",
        "Contest_Id",
        "Selection_Id",
        "Election_Id",
        "ParentReportingUnit_Id",
        "Name",
        "ReportingUnitType_Id",
        "ParentName",
        "ParentReportingUnitType_Id",
        "CountItemType",
        "Contest",
        "Selection",
        "ElectionDistrict_Id",
        "Candidate_Id",
        "contest_type",
        "contest_district_type",
        "Party",
    ]
    return result_df


def get_contest_with_unknown(session, election_id, top_ru_id) -> List[str]:
    connection = session.bind.raw_connection()
    cursor = connection.cursor()

    q = sql.SQL("""select distinct c."Name"
from "VoteCount" vc
left join "Contest" c on vc."Contest_Id" = c."Id"
left join "CandidateSelection" cs on cs."Id" = vc."Selection_Id"
left join "Candidate" can on cs."Candidate_Id" = can."Id"
left join "ReportingUnit" child_ru on vc."ReportingUnit_Id" = child_ru."Id"
left join "ComposingReportingUnitJoin" cruj on child_ru."Id" = cruj."ChildReportingUnit_Id"
where
    can."BallotName" = 'none or unknown'
 and vc."Election_Id" = %s
and cruj."ParentReportingUnit_Id" = %s
;""")
    cursor.execute(
        q, (election_id, top_ru_id)
    )
    result = cursor.fetchall()
    contests = [x[0] for x in result]
    return contests


def export_rollup_from_db(
    session,
    top_ru: str,
    election: str,
    sub_unit_type: str,
    contest_type: str,
    datafile_list: iter,
    by: str = "Id",
    exclude_redundant_total: bool = False,
    by_vote_type: bool = False,
    contest: Optional[str] = None
) -> (pd.DataFrame, Optional[str]):
    """Return a dataframe of rolled-up results and an error string.
    If by_vote_type, return separate rows for each vote type.
    If exclude_redundant_total then, if both total and other vote types are given, exclude total"""

    connection = session.bind.raw_connection()
    cursor = connection.cursor()
    # define the 'where' sql clause based on restrictions from parameters
    # and the string variables to be passed to query
    restrict = ""
    group_and_order_by = """C."Name", EDRUT."Txt", Cand."BallotName", IntermediateRU."Name" """
    string_vars = [election, top_ru, sub_unit_type, tuple(datafile_list)]

    if by_vote_type:
        count_item_type_sql = sql.SQL("CIT.{txt}").format(txt=sql.Identifier("Txt"))
        group_and_order_by += """, CIT."Txt" """
    else:
        count_item_type_sql = sql.SQL("'total'")

    if exclude_redundant_total:
        election_id = name_to_id_cursor(cursor, "Election", election)
        jurisdiction_id = name_to_id_cursor(cursor, "ReportingUnit", top_ru)
        active = active_vote_types_from_ids(cursor, election_id=election_id, jurisdiction_id=jurisdiction_id)
        if len(active) > 1 and 'total' in active:
            restrict += """ AND CIT."Txt" != 'total' """

    if contest:
        restrict += """ AND C."Name" = %s """
        string_vars.append(contest)

    columns = [
        "contest_type",
        "contest",
        "contest_district_type",
        "selection",
        "reporting_unit",
        "count_item_type",
        "count",
    ]
    if contest_type == "Candidate":
        q = sql.SQL(
            """
        SELECT 'Candidate' contest_type,
            C."Name" "Contest",
            EDRUT."Txt" contest_district_type,
            Cand."BallotName" "Selection",
            IntermediateRU."Name" "ReportingUnit",
            {count_item_type_sql} "CountItemType",
            sum(vc."Count") "Count"
        FROM "VoteCount" vc
        LEFT JOIN _datafile d on vc."_datafile_Id" = d."Id"
        LEFT JOIN "Contest" C on vc."Contest_Id" = C."Id"
        LEFT JOIN "CandidateSelection" CS on CS."Id" = vc."Selection_Id"
        LEFT JOIN "Candidate" Cand on CS."Candidate_Id" = Cand."Id"
        LEFT JOIN "Election" e on vc."Election_Id" = e."Id"
        -- sum over all children
        LEFT JOIN "ReportingUnit" ChildRU on vc."ReportingUnit_Id" = ChildRU."Id"
        LEFT JOIN "ComposingReportingUnitJoin" CRUJ_sum on ChildRU."Id" = CRUJ_sum."ChildReportingUnit_Id"
        -- roll up to the intermediate RUs
        LEFT JOIN "ReportingUnit" IntermediateRU on CRUJ_sum."ParentReportingUnit_Id" =IntermediateRU."Id"
        LEFT JOIN "ReportingUnitType" IntermediateRUT on IntermediateRU."ReportingUnitType_Id" = IntermediateRUT."Id"
        -- intermediate RUs must nest in top RU
        LEFT JOIN "ComposingReportingUnitJoin" CRUJ_top on IntermediateRU."Id" = CRUJ_top."ChildReportingUnit_Id"
        LEFT JOIN "ReportingUnit" TopRU on CRUJ_top."ParentReportingUnit_Id" = TopRU."Id"
        LEFT JOIN "CountItemType" CIT on vc."CountItemType_Id" = CIT."Id"
        LEFT JOIN "CandidateContest" on C."Id" = "CandidateContest"."Id"
        LEFT JOIN "Office" O on "CandidateContest"."Office_Id" = O."Id"
        LEFT JOIN "ReportingUnit" ED on O."ElectionDistrict_Id" = ED."Id"
        LEFT JOIN "ReportingUnitType" EDRUT on ED."ReportingUnitType_Id" = EDRUT."Id"
        WHERE C.contest_type = 'Candidate'
            AND e."Name" = %s -- election name
            AND TopRU."Name" = %s  -- top RU
             AND %s in (IntermediateRUT."Txt", IntermediateRU."OtherReportingUnitType")  -- intermediate_reporting_unit_type
           AND d.{by} in %s  -- tuple of datafile short_names (if by='short_name) or Ids (if by="Id")
            {restrict}
        GROUP BY {group_and_order_by}
        ORDER BY {group_and_order_by};
        """
        ).format(
            count_item_type_sql=count_item_type_sql,
            by=sql.Identifier(by),
            group_and_order_by=sql.SQL(group_and_order_by),
            restrict=sql.SQL(restrict),
        )

    elif contest_type == "BallotMeasure":
        q = sql.SQL(
            """
        SELECT 'Candidate' contest_type,
            C."Name" "Contest",
            EDRUT."Txt" contest_district_type,
            BMS."Name" "Selection",
            IntermediateRU."Name" "ReportingUnit",
            CIT."Txt" "CountItemType",
            sum(vc."Count") "Count"
        FROM "VoteCount" vc
        LEFT JOIN _datafile d on vc."_datafile_Id" = d."Id"
        LEFT JOIN "Contest" C on vc."Contest_Id" = C."Id"
        LEFT JOIN "BallotMeasureContest" BMC on vc."Contest_Id" = BMC."Id"
        LEFT JOIN "BallotMeasureSelection" BMS on BMS."Id" = vc."Selection_Id"
        LEFT JOIN "Election" e on vc."Election_Id" = e."Id"
        -- sum over all children
        LEFT JOIN "ReportingUnit" ChildRU on vc."ReportingUnit_Id" = ChildRU."Id"
        LEFT JOIN "ComposingReportingUnitJoin" CRUJ_sum on ChildRU."Id" = CRUJ_sum."ChildReportingUnit_Id"
        -- roll up to the intermediate RUs
        LEFT JOIN "ReportingUnit" IntermediateRU on CRUJ_sum."ParentReportingUnit_Id" =IntermediateRU."Id"
        LEFT JOIN "ReportingUnitType" IntermediateRUT on IntermediateRU."ReportingUnitType_Id" = IntermediateRUT."Id"
        -- intermediate RUs must nest in top RU
        LEFT JOIN "ComposingReportingUnitJoin" CRUJ_top on IntermediateRU."Id" = CRUJ_top."ChildReportingUnit_Id"
        LEFT JOIN "ReportingUnit" TopRU on CRUJ_top."ParentReportingUnit_Id" = TopRU."Id"
        LEFT JOIN "CountItemType" CIT on vc."CountItemType_Id" = CIT."Id"
        LEFT JOIN "ReportingUnit" ED on BMC."ElectionDistrict_Id" = ED."Id"
        LEFT JOIN "ReportingUnitType" EDRUT on ED."ReportingUnitType_Id" = EDRUT."Id"
        WHERE C.contest_type = 'BallotMeasure'
            AND e."Name" = %s -- election name
            AND TopRU."Name" = %s  -- top RU
            AND %s in (IntermediateRUT."Txt", IntermediateRU."OtherReportingUnitType")  -- intermediate_reporting_unit_type
            AND d.{by} in %s  -- tuple of datafile short_names
            {restrict}
        GROUP BY {group_and_order_by}
        ORDER BY {group_and_order_by}
        ;
        """
        ).format(
            count_item_type_sql=count_item_type_sql,
            by=sql.Identifier(by),
            group_and_order_by=sql.SQL(group_and_order_by),
            restrict=sql.SQL(restrict),
        )
    else:
        err_str = f"Unrecognized contest_type: {contest_type}. No results exported"
        return pd.DataFrame(columns=columns), err_str
    try:
        cursor.execute(q, string_vars)
        results = cursor.fetchall()
        results_df = pd.DataFrame(results)
        if not results_df.empty:
            results_df.columns = columns
        err_str = None
    except Exception as exc:
        results_df = pd.DataFrame()
        err_str = f"No results exported due to database error: {exc}"
    cursor.close()
    return results_df, err_str


def read_vote_count(
    session,
    election_id,
    reporting_unit_id,
    fields,
    aliases,
):
    """The VoteCount table is the only place that maps contests to a specific
    election. But this table is the largest one, so we don't want to use pandas methods
    to read into a DF and then filter"""
    q = sql.SQL(
        """
        SELECT  DISTINCT {fields}
        FROM    (
                    SELECT  "Id" as "VoteCount_Id", "Contest_Id", "Selection_Id",
                            "ReportingUnit_Id", "Election_Id", "CountItemType_Id", "Count"
                    FROM    "VoteCount"
                ) vc
                JOIN (SELECT "Id", "Name" as "ContestName" , contest_type as "ContestType" FROM "Contest") con on vc."Contest_Id" = con."Id"
                JOIN "ComposingReportingUnitJoin" cruj ON vc."ReportingUnit_Id" = cruj."ChildReportingUnit_Id"
                JOIN "CandidateSelection" cs ON vc."Selection_Id" = cs."Id"
                JOIN "Candidate" c on cs."Candidate_Id" = c."Id"
                JOIN (SELECT "Id", "Name" AS "PartyName" FROM "Party") p ON cs."Party_Id" = p."Id"
                JOIN "CandidateContest" cc ON con."Id" = cc."Id"
                JOIN (SELECT "Id", "Name" as "OfficeName", "ElectionDistrict_Id" FROM "Office") o on cc."Office_Id" = o."Id"
                -- this reporting unit info refers to the districts (state house, state senate, etc)
                JOIN (SELECT "Id", "Name" AS "ReportingUnitName", "ReportingUnitType_Id" FROM "ReportingUnit") ru on o."ElectionDistrict_Id" = ru."Id"
                JOIN (SELECT "Id", "Txt" AS unit_type FROM "ReportingUnitType") rut on ru."ReportingUnitType_Id" = rut."Id"
                -- this reporting unit info refers to the geopolitical divisions (county, state, etc)
                JOIN (SELECT "Id" as "GP_Id", "Name" AS "GPReportingUnitName", "ReportingUnitType_Id" AS "GPReportingUnitType_Id" FROM "ReportingUnit") gpru on vc."ReportingUnit_Id" = gpru."GP_Id"
                JOIN (SELECT "Id", "Txt" AS "GPType" FROM "ReportingUnitType") gprut on gpru."GPReportingUnitType_Id" = gprut."Id"
                JOIN (SELECT "Id", "Name" as "ElectionName", "ElectionType_Id" FROM "Election") e on vc."Election_Id" = e."Id"
                JOIN (SELECT "Id", "Txt" as "ElectionType" FROM "ElectionType") et on e."ElectionType_Id" = et."Id"
                JOIN (SELECT "Id", "Txt" as "CountItemType" FROM "CountItemType") cit on vc."CountItemType_Id" = cit."Id"
        WHERE   "Election_Id" = %s
                AND "ParentReportingUnit_Id" = %s
        """
    ).format(
        fields=sql.SQL(",").join(sql.Identifier(field) for field in fields),
    )
    connection = session.bind.raw_connection()
    cursor = connection.cursor()
    cursor.execute(q, [election_id, reporting_unit_id])
    results = cursor.fetchall()
    results_df = pd.DataFrame(results, columns=aliases)
    return results_df


def list_to_id(session, element, names) -> int:
    """ takes a list of names of various element types and returns a single ID """
    for name in names:
        id = name_to_id(session, element, name)
        if id:
            return id
    return None


def clean_candidate_names(df):
    """takes a df that has contest, candidate name, and party in the columns. Cleans the
    data as described in https://github.com/ElectionDataAnalysis/election_data_analysis/issues/207"""
    # Get first letter of each word in the party name except for "Party"
    # if "Party" is not in the name, then it's "None"
    cols = df.columns
    df_cols = ["parent", "name", "type"]
    extra_cols = [col for col in cols if col not in df_cols]
    extra_df = df[extra_cols]
    df = df[df_cols]
    df["party"] = df["type"].str.split(" ")
    df["party"] = np.where(
        df["party"].str.contains("party", case=False),
        df["party"]
        .map(lambda x: x[0:-1])
        .map(lambda words: "".join([word[0] for word in words])),
        "None",
    )

    # create the abbreviated contest name
    df["contest"] = df["parent"].str.replace(r"\(.*\)", "")
    df["jurisdiction"] = df["contest"].map(lambda x: x[0:2])
    mask_us_pres = df["contest"].str.contains("president", case=False)
    mask_us_sen = (df["jurisdiction"] == "US") & (
        df["contest"].str.contains("senate", case=False)
    )
    mask_us_house = (df["jurisdiction"] == "US") & (
        df["contest"].str.contains("house", case=False)
    )
    mask_st_sen = (df["jurisdiction"] != "US") & (
        df["contest"].str.contains("senate", case=False)
    )
    mask_st_house = (df["jurisdiction"] != "US") & (
        df["contest"].str.contains("house", case=False)
    )
    df["chamber"] = None
    df.loc[mask_us_pres, "chamber"] = "Pres"
    df.loc[mask_us_sen, "chamber"] = "Sen"
    df.loc[mask_us_house, "chamber"] = "House"
    df.loc[mask_st_sen, "chamber"] = "S"
    df.loc[mask_st_house, "chamber"] = "H"
    df["chamber"] = df["chamber"].fillna("unknown")
    df["district"] = df["contest"].str.extract(r"(\d+)")
    df["contest_short"] = ""
    df["contest_short"] = np.where(
        df["chamber"] != "unknown",
        df[df.columns[5:]].apply(lambda x: "".join(x.dropna().astype(str)), axis=1),
        df["contest_short"],
    )
    df["contest_short"] = np.where(
        df["chamber"] == "unknown",
        df["contest"]
        .str.split(" ")
        .map(lambda words: "".join([word[0:3] for word in words if word != "of"])),
        df["contest_short"],
    )
    df["name"] = df[["name", "party", "contest_short"]].apply(
        lambda x: " - ".join(x.dropna().astype(str)), axis=1
    )
    df = df.sort_values(by=["contest_short", "party", "name"])
    df = df[df_cols].merge(extra_df, how="inner", left_index=True, right_index=True)
    df.reset_index(drop=True, inplace=True)
    return df


def data_file_download(cursor, election_id: int, reporting_unit_id: int) -> int:
    q = sql.SQL("""
        SELECT  MAX(download_date)::text as download_date
        FROM    _datafile d
        WHERE   d."Election_Id" = %s
                AND d."ReportingUnit_Id" = %s
    """
    )
    try:
        cursor.execute(q, [election_id, reporting_unit_id])
        return cursor.fetchall()[0][0]
    except Exception as exc:
        return None


def is_preliminary(cursor, election_id, jurisdiction_id):
    """ get the preliminary flag from the _datafile table.
    Since this flag doesn't exist yet, parsing the election name for
    2020 because we expect all data for 2020 to be preliminary for awhile."""
    q = sql.SQL("""
        SELECT  DISTINCT is_preliminary
        FROM    _datafile
        WHERE   "Election_Id" = %s
                AND "ReportingUnit_Id" = %s
        ORDER BY is_preliminary
        LIMIT   1
    """
    )
    try:
        cursor.execute(q, [election_id, jurisdiction_id])
        results = cursor.fetchall()
        return results[0][0]
    except Exception as exc:
        election = name_from_id(cursor, "Election", election_id)
        if election.startswith("2020 General"):
            return True
        return False


def read_external(cursor, election_year: int, top_ru_id: int, fields: list, restrict=None):
    if restrict:
        census = f"""AND "Label" = '{restrict}'"""
    else:
        census = ""
    q = sql.SQL("""
        SELECT  DISTINCT "Category", "InCategoryOrder", {fields}
        FROM    "External"
        WHERE   "ElectionYear" = %s
                AND "TopReportingUnit_Id" = %s
                {census}
        ORDER BY "Category", "InCategoryOrder"
    """
    ).format(
        fields=sql.SQL(",").join(sql.Identifier(field) for field in fields),
        census=sql.SQL(census),
    )
    try:
        cursor.execute(q, [election_year, top_ru_id])
        results = cursor.fetchall()
        results_df = pd.DataFrame(results, columns=["Category", "InCategoryOrder"] + fields)
        # return unique columns (by name, not value)
        return results_df.loc[:, ~results_df.columns.duplicated()][fields]
    except Exception as exc:
        return pd.DataFrame()

def presidential_candidates(session, election_id, top_ru_id, contest_id) -> List[str]:
    connection = session.bind.raw_connection()
    cursor = connection.cursor()

    q = sql.SQL("""select distinct can."BallotName"
from "VoteCount" vc
left join "Contest" c on vc."Contest_Id" = c."Id"
left join "CandidateSelection" cs on cs."Id" = vc."Selection_Id"
left join "Candidate" can on cs."Candidate_Id" = can."Id"
left join "ReportingUnit" child_ru on vc."ReportingUnit_Id" = child_ru."Id"
left join "ComposingReportingUnitJoin" cruj on child_ru."Id" = cruj."ChildReportingUnit_Id"
where
    c."Id" = %s
 and vc."Election_Id" = %s
and cruj."ParentReportingUnit_Id" = %s
;""")
    cursor.execute(
        q, (contest_id,election_id, top_ru_id,)
    )
    result = cursor.fetchall()
    contests = [x[0] for x in result]
    return contests

def get_contest_with_unknown_parties(session, election_id, top_ru_id) -> List[str]:
    connection = session.bind.raw_connection()
    cursor = connection.cursor()

    q = sql.SQL("""select distinct c."Name"
from "VoteCount" vc
left join "Contest" c on vc."Contest_Id" = c."Id"
left join "CandidateSelection" cs on cs."Id" = vc."Selection_Id"
left join "Candidate" can on cs."Candidate_Id" = can."Id"
left join "Party" p on cs."Party_Id" = p."Id"
left join "ReportingUnit" child_ru on vc."ReportingUnit_Id" = child_ru."Id"
left join "ComposingReportingUnitJoin" cruj on child_ru."Id" = cruj."ChildReportingUnit_Id"
where
    p."Name"='none or unknown'
 and vc."Election_Id" = %s
and cruj."ParentReportingUnit_Id" = %s
;""")
    cursor.execute(
        q, (election_id, top_ru_id)
    )
    result = cursor.fetchall()
    contests = [x[0] for x in result]
    return contests
