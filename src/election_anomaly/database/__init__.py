#!/usr/bin/python3
# database/__init__.py

import psycopg2
import sqlalchemy
import sqlalchemy.orm
import io
import csv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from psycopg2 import sql
import sqlalchemy as db
import datetime
from election_anomaly import user_interface as ui
from configparser import MissingSectionHeaderError
import pandas as pd
from election_anomaly import munge as m
import re
from election_anomaly.database import create_cdf_db as db_cdf
import os
import numpy as np
from sqlalchemy import MetaData, Table, Column, Integer, Float
from election_anomaly import analyze as a

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

db_pars = [
    "host",
    "port",
    "dbname",
    "user",
    "password"
]

def get_database_names(con):
    """Return dataframe with one column called `datname` """
    names = pd.read_sql("SELECT datname FROM pg_database", con)
    return names


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


def establish_connection(paramfile="run_time.ini", dbname=None):
    """Check for DB and relevant tables; if they don't exist, return
    error message"""
    try:
        params = ui.get_runtime_parameters(
            db_pars, param_file=paramfile, header="postgresql"
        )[0]
    except MissingSectionHeaderError as e:
        return {"message": "database.ini file not found suggested location."}
    if dbname:
        params["dbname"] = dbname
    try:
        con = psycopg2.connect(**params)
    except psycopg2.OperationalError as e:
        return {"message": "Unable to establish connection to database."}

    # Look for tables
    engine = sql_alchemy_connect(paramfile)
    elems, enums, joins, o = get_cdf_db_table_names(engine)

    # All tables except "Others" must be created. Essentially looks for
    # a "complete" database.
    if not elems or not enums or not joins:
        return {"message": "Required tables not found."}

    con.close()
    return None


def create_new_db(project_root, param_file="run_time.ini"):
    # get connection to default postgres DB to create new one
    try:
        params = ui.get_runtime_parameters(db_pars, param_file, "postgresql")[0]
        db_name = params["dbname"] 
        params["dbname"] = "postgres"
        con = psycopg2.connect(**params)
    except:
        # Can't connect to the default postgres database, so there
        # seems to be something wrong with connection. Fail here.
        print("Unable to find database. Exiting.")
        quit()
    cur = con.cursor()
    db_df = get_database_names(con)

    eng = sql_alchemy_connect(param_file, dbname=params["dbname"])
    Session = sqlalchemy.orm.sessionmaker(bind=eng)
    sess = Session()

    # DB already exists.
    # TODO if DB exists, check that desired_db has right format?
    if db_name in db_df.datname.unique():
        # Clean out DB
        db_cdf.reset_db(
            sess, os.path.join(project_root, "election_anomaly", "CDF_schema_def_info")
        )
    else:
        create_database(con, cur, db_name)

    # load cdf tables
    db_cdf.create_common_data_format_tables(
        sess,
        dirpath=os.path.join(project_root, "election_anomaly", "CDF_schema_def_info"),
    )
    db_cdf.fill_standard_tables(
        sess,
        None,
        dirpath=os.path.join(project_root, "election_anomaly/CDF_schema_def_info/"),
    )
    con.close()


def sql_alchemy_connect(
    paramfile: str = "run_time.ini", dbname: str = "postgres"
) -> sqlalchemy.engine:
    """Returns an engine and a metadata object"""
    params = ui.get_runtime_parameters(
        db_pars, param_file=paramfile, header="postgresql"
    )[0]
    if dbname != "postgres":
        params["dbname"] = dbname
    # We connect with the help of the PostgreSQL URL
    url = "postgresql://{user}:{password}@{host}:{port}/{dbname}"
    url = url.format(**params)

    # The return value of create_engine() is our connection object
    engine = db.create_engine(url, client_encoding="utf8")
    return engine


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


def name_to_id(session, element, name) -> int:
    """ Condition can be a field/value pair, e.g., ('contest_type','Candidate')"""
    connection = session.bind.raw_connection()
    cursor = connection.cursor()
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
    engine, df, element, sep="\t", encoding="iso-8859-1", timestamp=None
) -> str:
    """Inserts any new records in <df> into <element>; if <element> has a timestamp column
    it must be specified in <timestamp>; <df> must have columns matching <element>, except Id and <timestamp> if any"""

    # initialize connection and cursor
    working = m.generic_clean(df)
    connection = engine.raw_connection()
    cursor = connection.cursor()

    # identify new ReportingUnits, must later enter nesting info in db
    if element == "ReportingUnit":
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
        c
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
        connection.commit()

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
        # check Id column for '' or 0 (indicating not matched)
        if pd.api.types.is_numeric_dtype(matched_with_old[f"{element}_Id"]):
            new_rus = matched_with_old[matched_with_old[f"{element}_Id"] == 0]
        else:
            new_rus = matched_with_old[matched_with_old[f"{element}_Id"] == ""]
        if not new_rus.empty:
            append_to_composing_reporting_unit_join(engine, new_rus)

    connection.commit()
    cursor.close()
    connection.close()
    return error_str


def table_named_to_avoid_conflict(engine, prefix: str) -> str:
    p = re.compile("postgresql://([^:]+)")
    user_name = p.findall(str(engine.url))[0]
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
    temp_table = f"{prefix}_{user_name}_{ts}"
    return temp_table


def append_id_to_dframe(
    engine: sqlalchemy.engine, df: pd.DataFrame, table, col_map=None
) -> pd.DataFrame:
    """Using <col_map> to map columns of <df> onto defining columns of <table>, returns
    a copy of <df> with appended column <table>_Id. Unmatched items returned with null value for <table>_Id"""
    if col_map is None:
        col_map = {table: get_name_field(table)}
    connection = engine.raw_connection()

    temp_table = table_named_to_avoid_conflict(engine, "__temp_append")

    df_cols = list(col_map.keys())

    # create temp db table with info from df, without index
    df = m.generic_clean(df)
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
        tt=sql.Identifier(temp_table), t=sql.Identifier(table), on_clause=on_clause
    )
    w = m.generic_clean(pd.read_sql_query(q, connection).set_index("dataframe_index"))

    # drop temp db table
    q = sql.SQL("DROP TABLE {temp_table}").format(temp_table=sql.Identifier(temp_table))
    cur = connection.cursor()
    cur.execute(q)
    connection.commit()
    connection.close()
    return df.join(w[["Id"]]).rename(columns={"Id": f"{table}_Id"})


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


def export_rollup_to_csv(
    cursor,
    top_ru: str,
    sub_unit_type: str,
    contest_type: str,
    datafile_list: iter,
    out_path: str,
    sep: str = "\t",
    by: str = "Id",
    exclude_total: bool = False,
) -> str:

    if exclude_total:
        restrict = """ AND CIT."Txt" != 'total' """
    else:
        restrict = ""
    if contest_type == "Candidate":
        q = sql.SQL(
            """ COPY
            (SELECT
               'Candidate' contest_type,
               C."Name" "Contest",
               EDRUT."Txt" contest_district_type,
               Cand."BallotName" "Selection",
              IntermediateRU."Name" "ReportingUnit",
               CIT."Txt" "CountItemType",
               sum(vc."Count") "Count"
        FROM "VoteCount" vc
        LEFT JOIN _datafile d on vc."_datafile_Id" = d."Id"
        LEFT JOIN "Contest" C on vc."Contest_Id" = C."Id"
        LEFT JOIN "CandidateSelection" CS on CS."Id" = vc."Selection_Id"
        LEFT JOIN "Candidate" Cand on CS."Candidate_Id" = Cand."Id"
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
            AND TopRU."Name" = %s  -- top RU
            AND IntermediateRUT."Txt" = %s  -- intermediate_reporting_unit_type
            AND d.{by} in %s  -- tuple of datafile short_names
            {restrict}
        GROUP BY
               C."Name",
               EDRUT."Txt",
               Cand."BallotName",
              IntermediateRU."Name",
               CIT."Txt"
        ORDER BY
               C."Name",
               EDRUT."Txt",
               Cand."BallotName",
              IntermediateRU."Name",
               CIT."Txt")
        TO %s DELIMITER %s CSV HEADER;
        """
        ).format(by=sql.Identifier(by), restrict=sql.SQL(restrict))

    elif contest_type == "BallotMeasure":
        q = sql.SQL(
            """ COPY
            (SELECT
               'Candidate' contest_type,
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
            AND TopRU."Name" = %s  -- top RU
            AND IntermediateRUT."Txt" = %s  -- intermediate_reporting_unit_type
            AND d.{by} in %s  -- tuple of datafile short_names
            {restrict}
        GROUP BY
               C."Name",
               EDRUT."Txt",
               BMS."Name",
              IntermediateRU."Name",
               CIT."Txt"
        ORDER BY
               C."Name",
               EDRUT."Txt",
               BMS."Name",
              IntermediateRU."Name",
               CIT."Txt")
        TO %s DELIMITER %s CSV HEADER;
        """
        ).format(by=sql.Identifier(by), restrict=sql.SQL(restrict))
    else:
        err_str = f"Unrecognized contest_type: {contest_type}. No results exported"
        return err_str
    try:
        cursor.execute(q, [top_ru, sub_unit_type, tuple(datafile_list), out_path, sep])
        print(f"Results exported to {out_path}")
        err_str = None
    except Exception as exc:
        err_str = f"No results exported due to database error: {exc}"
    return err_str


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


def data_file_list(cursor, election_id_list, by="Id"):
    q = sql.SQL(
        """SELECT distinct d.{by} FROM _datafile d WHERE d."Election_Id" in %s"""
    ).format(by=sql.Identifier(by))
    try:
        cursor.execute(q, [tuple(election_id_list)])
        df_list = [x for (x,) in cursor.fetchall()]
        err_str = None
    except Exception as exc:
        err_str = f"Database error pulling list of datafiles with election id in {election_id_list}: {exc}"
        df_list = None
    return df_list, err_str


def remove_vote_counts(connection, cursor, id: int) -> str:
    """Remove all VoteCount data from a particular file, and remove that file from _datafile"""
    try:
        q = 'SELECT * FROM _datafile WHERE _datafile."Id"=%s;'
        cursor.execute(q, [id])
        record = cursor.fetchall()[0]
    except KeyError as exc:
        return f"No datafile found with Id = {id}"

    confirm = input(
        f"Confirm: delete all VoteCount data from this results file: {record} (y/n)?"
    )
    if confirm == "y":
        try:
            q = 'DELETE FROM "VoteCount" where "_datafile_Id"=%s;Delete from _datafile where "Id"=%s;'
            cursor.execute(q, [id, id])
            connection.commit()
            print(f'VoteCounts deleted from results file {record["short_name"]}')
            err_str = None
        except Exception as exc:
            err_str = f"Error deleting data: {exc}"
    else:
        err_str = "Deletion not confirmed by user"
    return err_str


def get_input_options(session, input, verbose):
    """Returns a list of response options based on the input"""
    # input comes as a pythonic (snake case) input, need to
    # change to match DB element naming format
    name_parts = input.split("_")
    search_str = "".join([name_part.capitalize() for name_part in name_parts])

    if search_str in [
        "BallotMeasureContest",
        "CandidateContest",
        "Election",
        "Office",
        "Party",
        "ReportingUnit",
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
    elif search_str == "BallotMeasureSelection":
        column_name = "Selection"
        table_search = True
    elif search_str == "Candidate":
        column_name = "BallotName"
        table_search = True
    # TODO: do we need a subdivision_type?
    else:
        search_str = search_str.lower()
        table_search = False

    if not verbose:
        connection = session.bind.raw_connection()
        cursor = connection.cursor()
        if table_search:
            q1 = sql.SQL("SELECT {column_name} FROM {search_str};").format(
                column_name=sql.Identifier(column_name),
                search_str=sql.Identifier(search_str),
            )
            cursor.execute(q1)
            result = cursor.fetchall()
        else:
            q2 = sql.SQL(
                'SELECT "Name" FROM "ReportingUnit" ru JOIN "ReportingUnitType" rut on ru."ReportingUnitType_Id" = rut."Id" WHERE rut."Txt" = %s'
            )
            cursor.execute(q2, [search_str])
            result = cursor.fetchall()
        connection.close()
        return [r[0] for r in result]
    else:
        # jurisction result are handled differently than the rest of the flow because
        # it's the first selection made
        if search_str == "jurisdiction":
            result = session.execute(
                f"""
                WITH states(states) AS (
                    SELECT  '{states}'
                )
                , unnested AS (
                    SELECT    UNNEST(regexp_split_to_array(states, '\n')) AS states
                    FROM    states
                )
                , ordered AS (
                    SELECT    *, ROW_NUMBER() OVER() AS order_by
                    FROM    unnested u
                )
                SELECT    states as parent,
                        states AS name, 
                        CASE WHEN "Id" IS null THEN false ELSE true END AS type
                FROM    ordered o
                        LEFT JOIN "ReportingUnit" ru ON o.states = ru."Name"
                ORDER BY order_by
            """
            )
            result_df = pd.DataFrame(result)
            result_df.columns = result.keys()
            return package_display_results(result_df)
        elif search_str == "BallotMeasureContest":
            # parent_id is reporting unit, type is reporting unit type
            result = session.execute(
                f"""
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
            result = session.execute(
                f"""
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
            result = session.execute(
                f"""
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
            result = session.execute(
                f"""
                SELECT  DISTINCT ct."Name" AS parent, c."BallotName" as name, 
                        p."Name" as type
                FROM    "Candidate" c
                        JOIN "CandidateSelection" cs ON c."Id" = cs."Candidate_Id"
                        JOIN "Party" p ON cs."Party_Id" = p."Id"
                        JOIN "VoteCount" vc on cs."Id" = vc."Selection_Id"
                        JOIN "CandidateContest" cc ON vc."Contest_Id" = cc."Id"
                        JOIN "Contest" ct on cc."Id" = ct."Id"
                WHERE   c."BallotName" ILIKE '%{search_str}%'
            """
            )
        return result


def get_datafile_info(session, results_file):
    q = session.execute(
        f"""
        SELECT "Id", "Election_Id" 
        FROM _datafile 
        WHERE file_name = '{results_file}'
        """
    ).fetchall()
    try:
        return q[0]
    except IndexError:
        print(
            f"No record named {results_file} found in _datafile table in {session.bind.url}"
        )
        return [0, 0]
    return q[0]


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
        temp = {"parent": row[0], "name": row[1], "type": row[2], "order_by": i + 1}
        results.append(temp)
    return results


def get_filtered_input_options(session, input_str, filters):
    # jurisdiction selection is handled separately because it's the first choice.
    contest_df = get_relevant_contests(session, filters)
    # contest_type is a special case because we don't have a contest_type table.
    # instead, this is the reporting unit type of the election district
    if input_str == "contest_type":
        contest_types = contest_df["type"].unique()
        contest_types.sort()
        data = {
            "parent": [filters[0] for contest_type in contest_types],
            "name": contest_types,
            "type": [None for contest_type in contest_types],
        }
        df = pd.DataFrame(data=data)
    elif input_str == "contest":
        df = contest_df[contest_df["type"].isin(filters)]
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
        subdivision_types = hierarchy_df["Txt"].unique()
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
    elif input_str == "category":
        election_df = get_relevant_election(session, filters)
        election_df = election_df[election_df["Name"].isin(filters)]
        count_type_ids = (
            pd.read_sql_table("VoteCount", session.bind, index_col="Id")
            .merge(election_df, how="inner", left_on="Election_Id", right_index=True)[
                "CountItemType_Id"
            ]
            .unique()
        )
        count_types_df = pd.read_sql_table(
            "CountItemType", session.bind, index_col="Id"
        )
        count_types = list(
            count_types_df[count_types_df.index.isin(count_type_ids)]["Txt"].unique()
        )
        count_types.sort()
        data = {
            "parent": [filters[0] for count_type in count_types]
            + [filters[0] for count_type in count_types],
            "name": [f"{count_type} candidates" for count_type in count_types]
            + [f"{count_type} contests" for count_type in count_types],
            "type": [None for count_type in count_types]
            + [None for count_type in count_types],
        }
        df = pd.DataFrame(data=data)
    elif input_str == "count":
        election_df = get_relevant_election(session, filters)
        hierarchy_df = pd.read_sql_table(
            "ComposingReportingUnitJoin", session.bind, index_col="Id"
        )
        unit_df = pd.read_sql_table("ReportingUnit", session.bind, index_col="Id")
        hierarchy_df = hierarchy_df.merge(
            unit_df, how="inner", left_on="ParentReportingUnit_Id", right_on="Id"
        )
        hierarchy_df = hierarchy_df[hierarchy_df["Name"].isin(filters)]
        selection_ids = (
            pd.read_sql_table("VoteCount", session.bind, index_col="Id")
            .merge(election_df, how="inner", left_on="Election_Id", right_index=True)
            .merge(
                hierarchy_df,
                how="inner",
                left_on="ReportingUnit_Id",
                right_on="ChildReportingUnit_Id",
            )["Selection_Id"]
            .unique()
        )
        candidate_selection_df = pd.read_sql_table(
            "CandidateSelection", session.bind, index_col="Id"
        )
        # this has candidate IDs and contest IDs
        candidate_contest_df = candidate_selection_df[
            candidate_selection_df.index.isin(selection_ids)
        ][candidate_selection_df.columns].reset_index()
        # then we get the cnadidate names themselves
        candidate_names_df = pd.read_sql_table(
            "Candidate", session.bind, index_col="Id"
        )
        # and this has candidates, but in name form
        candidates = get_input_options(session, "candidate", True)
        candidates_df = pd.DataFrame(candidates)
        candidates_df.columns = candidates.keys()
        candidate_names_df = candidate_names_df.merge(
            candidate_contest_df, how="inner", left_index=True, right_on="Candidate_Id"
        )
        candidates_df = candidates_df.merge(
            candidate_names_df, how="inner", left_on="name", right_on="BallotName"
        )
        df = contest_df.merge(
            candidates_df,
            how="inner",
            left_on="name",
            right_on="parent",
            suffixes=["_x", None],
        )[candidates_df.columns]
    else:
        candidates = get_input_options(session, input_str, True)
        candidates_df = pd.DataFrame(candidates)
        candidates_df.columns = candidates.keys()
        candidates_df = candidates_df.merge(
            contest_df,
            how="inner",
            left_on="parent",
            right_on="name",
            suffixes=[None, "_y"],
        )
        df = (
            candidates_df.groupby(["parent", "type"])["name"]
            .apply(list)
            .apply(str)
            .reset_index()
            .sort_values("parent")
        )
        df.columns = ["parent", "type", "name"]
        df = df[["parent", "name", "type"]]
        # clean the name column
        df["name"] = (
            df["name"]
            .str.replace("\['", "")
            .str.replace("'\]", "")
            .str.replace("', '", "; ")
        )
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
    """Gets all contests for a selected jurisdiction, held in filters"""

    # Get a DF of parent, child reporting Units, filtered on the jurisdiciton
    # selected by the user
    hierarchy_df = pd.read_sql_table(
        "ComposingReportingUnitJoin", session.bind, index_col="Id"
    )
    unit_df = pd.read_sql_table("ReportingUnit", session.bind, index_col="Id")
    hierarchy_df = hierarchy_df.merge(
        unit_df, how="inner", left_on="ParentReportingUnit_Id", right_on="Id"
    )
    hierarchy_df = hierarchy_df[hierarchy_df["Name"].isin(filters)]
    hierarchy_df.drop(
        columns=[
            "ParentReportingUnit_Id",
            "ReportingUnitType_Id",
            "OtherReportingUnitType",
        ],
        inplace=True,
    )
    hierarchy_df = hierarchy_df.merge(
        unit_df, how="inner", left_on="ChildReportingUnit_Id", right_on="Id"
    )
    hierarchy_df.rename(columns={"Name_x": "parent", "Name_y": "child"}, inplace=True)
    hierarchy_df.drop(
        columns=[
            "ChildReportingUnit_Id",
            "ReportingUnitType_Id",
            "OtherReportingUnitType",
        ],
        inplace=True,
    )

    units = hierarchy_df["child"].unique()
    result = get_input_options(session, "candidate_contest", True)
    result_df = pd.DataFrame(result)
    result_df.columns = result.keys()
    result_df = result_df[result_df["parent"].isin(units)]
    return result_df


def get_jurisdiction_hierarchy(session, jurisdiction_id, subdivision_type_id):
    q = session.execute(
        f"""
        SELECT  regexp_split_to_array("Name", ';') unit_array
        FROM    "ComposingReportingUnitJoin" j
                JOIN "ReportingUnit" ru ON j."ChildReportingUnit_Id" = ru."Id"
        WHERE   "ParentReportingUnit_Id" = {jurisdiction_id}
                AND "ReportingUnitType_Id" = {subdivision_type_id} 
        LIMIT   1
    """
    ).fetchall()

    unit_portions = q[0][0]
    hierarchy = []
    for i in range(len(unit_portions)):
        unit = ";".join(unit_portions[0 : i + 1])
        q = session.execute(
            f"""
            SELECT    "ReportingUnitType_Id"
            FROM    "ReportingUnit"
            WHERE    "Name" = '{unit}' 
        """
        ).fetchall()
        hierarchy.append(q[0][0])
    return hierarchy


def get_candidate_votecounts(session, election_id, top_ru_id, subdivision_type_id):
    q = f"""
    SELECT  vc."Id" as "VoteCount_Id", "Count", "CountItemType_Id",
            vc."ReportingUnit_Id", "Contest_Id", "Selection_Id",
            vc."Election_Id", "_datafile_Id", IntermediateRU."Id" as "ParentReportingUnit_Id",
            ChildRU."Name", ChildRU."ReportingUnitType_Id",
            IntermediateRU."Name" as "ParentName", IntermediateRU."ReportingUnitType_Id" as "ParentReportingUnitType_Id",
            CIT."Txt" as "CountItemType", C."Name" as "Contest",
            Cand."BallotName" as "Selection", "ElectionDistrict_Id", Cand."Id" as "Candidate_Id", "contest_type",
            EDRUT."Txt" as "contest_district_type"
            FROM "VoteCount" vc
            LEFT JOIN _datafile d on vc."_datafile_Id" = d."Id"
            LEFT JOIN "Contest" C on vc."Contest_Id" = C."Id"
            LEFT JOIN "CandidateSelection" CS on CS."Id" = vc."Selection_Id"
            LEFT JOIN "Candidate" Cand on CS."Candidate_Id" = Cand."Id"
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
                AND TopRU."Id" = {top_ru_id}
                AND IntermediateRU."ReportingUnitType_Id" = {subdivision_type_id}
                AND vc."Election_Id" = {election_id}
    """
    result = session.execute(q)
    result_df = pd.DataFrame(result)
    result_df.columns = result.keys()
    return result_df


def most_recent_election(session, jurisdiction_id):
    q = session.execute(
        f"""
        SELECT  "Election_Id"
        FROM    _datafile d
                JOIN "Election" e on d."Election_Id" = e."Id"
        WHERE   "ReportingUnit_Id" = 274
        ORDER BY "Name" desc
        LIMIT   1
    """
    ).fetchall()
    return q[0][0]