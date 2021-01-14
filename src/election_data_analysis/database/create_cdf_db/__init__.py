# TODO may need to add "NOT NULL" requirements per CDF
# TODO add OfficeContestJoin table (e.g., Presidential contest has two offices)
# TODO consistency check on SelectionElectionContestVoteCountJoin to make sure ElectionContestJoin_Id
#  and ContestSelectionJoin_Id share a contest? Should this happen during the rollup process?

import sqlalchemy as sa
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    CheckConstraint,
    UniqueConstraint,
    Integer,
    String,
    ForeignKey,
    Index,
    Boolean,
)
from sqlalchemy import (
    Date,
    TIMESTAMP,
)  # these are used, even if syntax-checker can't tell
import os
import pandas as pd
from election_data_analysis import database as db


# constants
bmselections = ["Yes", "No", "none or unknown"]


def create_common_data_format_tables(session, dirpath="CDF_schema_def_info/"):
    """schema example: 'cdf'; Creates cdf tables in the given schema
    (or directly in the db if schema == None)
    e_table_list is a list of enumeration tables for the CDF, e.g., ['ReportingUnitType','CountItemType', ... ]
    Does *not* fill enumeration tables.
    """
    eng = session.bind
    metadata = MetaData(bind=eng)

    # create the single sequence for all db ids
    id_seq = sa.Sequence("id_seq", metadata=metadata)

    # create enumeration tables
    e_table_list = enum_table_list(dirpath)
    for t in e_table_list:
        create_table(metadata, id_seq, t, "enumerations", dirpath)

    # create element tables (cdf and metadata) and push to db
    element_path = os.path.join(dirpath, "elements")
    elements_to_process = [f for f in os.listdir(element_path) if f[0] != "."]
    # dynamic list of elements whose tables haven't been created yet
    while elements_to_process:
        element = elements_to_process[0]
        # check foreign keys; if any refers to an elt yet to be processed, change to that elt
        #  note that any foreign keys for elements are to other elements, so it's OK to do this without considering
        #  joins first or concurrently.
        foreign_keys = pd.read_csv(
            os.path.join(element_path, element, "foreign_keys.txt"), sep="\t"
        )
        for i, r in foreign_keys.iterrows():
            fk_set = set(
                r["refers_to"].split(";")
            )  # lists all targets of the foreign key r['fieldname']
            try:
                element = [e for e in fk_set if e in elements_to_process].pop()
                break
            except IndexError:
                pass
        # create indices for efficiency
        if element == "VoteCount":
            create_indices = [
                "CountItemType_Id",
                "OtherCountItemType",
                "ReportingUnit_Id",
                "Contest_Id",
                "Selection_Id",
                "Election_Id",
                "_datafile_Id",
            ]
        elif element == "CandidateSelection":
            create_indices = ["Candidate_Id", "Party_Id"]
        elif element == "ReportingUnit":
            create_indices = ["ReportingUnitType_Id"]
        else:
            # create_indices = [[db.get_name_field(element)]]
            create_indices = None
            # TODO fix for efficiency -- note <contest_type>Contest, <contest_type>Selection may need special treatment

        # create db table for element
        create_table(
            metadata,
            id_seq,
            element,
            "elements",
            dirpath,
            create_indices=create_indices,
        )
        # remove element from list of yet-to-be-processed
        elements_to_process.remove(element)

    # create join tables
    # TODO check for foreign keys, as above
    # check for foreign keys
    join_path = os.path.join(dirpath, "Joins")
    joins_to_process = [f for f in os.listdir(join_path) if f[0] != "."]
    while joins_to_process:
        j = joins_to_process[0]
        if j == "ComposingReportingUnitJoin":
            create_indices = ["ParentReportingUnit_Id", "ChildReportingUnit_Id"]
        else:
            create_indices = None
        # check foreign keys; if any refers to an elt yet to be processed, change to that elt
        #  note that any foreign keys for elements are to other elements, so it's OK to do this without considering
        #  joins first or concurrently.
        foreign_keys = pd.read_csv(
            os.path.join(join_path, j, "foreign_keys.txt"), sep="\t"
        )
        for i, r in foreign_keys.iterrows():
            fk_set = set(
                r["refers_to"].split(";")
            )  # lists all targets of the foreign key r['fieldname']
            try:
                j = [e for e in fk_set if e in joins_to_process].pop()
                break
            except IndexError:
                pass
        # create db table for element
        create_table(metadata, id_seq, j, "Joins", dirpath, create_indices)

        # remove element from list of yet-to-be-processed
        joins_to_process.remove(j)

    # push all tables to db
    metadata.create_all()
    session.flush()
    return metadata


def create_table(
    metadata, id_seq, name, table_type, dirpath, create_indices: list = None
):
    """Each element of the list <create_indices>, should be a list of
    columns on which an index should be created."""
    t_path = os.path.join(dirpath, table_type, name)
    if name == "Selection":
        # Selection table has only Id column
        t = Table(
            name,
            metadata,
            Column(
                "Id",
                Integer,
                id_seq,
                server_default=id_seq.next_value(),
                primary_key=True,
            ),
        )
        Index(f"{t}_parent", t.c.Id)
    elif table_type == "elements":
        with open(os.path.join(t_path, "short_name.txt"), "r") as f:
            short_name = f.read().strip()

        # read info from files into dataframes
        df = {}
        for filename in [
            "enumerations",
            "fields",
            "foreign_keys",
            "not_null_fields",
            "unique_constraints",
        ]:
            df[filename] = pd.read_csv(
                os.path.join(t_path, f"{filename}.txt"), sep="\t"
            )

        # define table
        # content field names
        df["fields"]["datatype"].replace({"Encoding": "String"}, inplace=True)
        field_col_list = [
            Column(r["fieldname"], eval(r["datatype"]))
            for i, r in df["fields"].iterrows()
        ]
        field_col_names = [r["fieldname"] for i, r in df["fields"].iterrows()]

        null_constraint_list = [
            CheckConstraint(
                f'"{r["not_null_fields"]}" IS NOT NULL',
                name=f'{short_name}_{r["not_null_fields"]}_not_null',
            )
            for i, r in df["not_null_fields"].iterrows()
        ]
        # omit 'foreign keys' that refer to more than one table,
        #  e.g. Contest_Id to BallotMeasureContest and CandidateContest
        foreign_key_list = [
            Column(r["fieldname"], ForeignKey(f'{r["refers_to"]}.Id'))
            for i, r in df["foreign_keys"].iterrows()
            if ";" not in r["refers_to"]
        ]
        foreign_ish_keys = [r["fieldname"] for i, r in df["foreign_keys"].iterrows()]

        # enumerations
        enum_id_list = [
            Column(f'{r["enumeration"]}_Id', ForeignKey(f'{r["enumeration"]}.Id'))
            for i, r in df["enumerations"].iterrows()
        ]
        enum_other_list = [
            Column(f'Other{r["enumeration"]}', String)
            for i, r in df["enumerations"].iterrows()
        ]
        enum_id_names = [
            f'{r["enumeration"]}_Id' for i, r in df["enumerations"].iterrows()
        ]
        enum_other_names = [
            f'Other{r["enumeration"]}' for i, r in df["enumerations"].iterrows()
        ]

        # specified unique constraints
        df["unique_constraints"]["arg_list"] = df["unique_constraints"][
            "unique_constraint"
        ].str.split(",")
        unique_constraint_list = [
            UniqueConstraint(*r["arg_list"], name=f"{short_name}_ux{i}")
            for i, r in df["unique_constraints"].iterrows()
        ]

        # require uniqueness for entire record (except `Id` and `timestamp`)
        all_content_fields = (
            field_col_names + enum_id_names + enum_other_names + foreign_ish_keys
        )
        unique_constraint_list.append(
            UniqueConstraint(*all_content_fields, name=f"{short_name}_no_dupes")
        )

        # add timestamp to _datafile
        if name == "_datafile":
            time_stamp_list = [Column("created_at", sa.DateTime, default=sa.func.now())]
        else:
            time_stamp_list = []
        if name in [
            "CandidateContest",
            "CandidateSelection",
            "BallotMeasureContest",
            "BallotMeasureSelection",
        ]:
            # don't want unique Id for these: they will be filled with value
            # from Contest or Selection (parent table)
            t = Table(
                name,
                metadata,
                *field_col_list,
                *enum_id_list,
                *enum_other_list,
                *foreign_key_list,
                *null_constraint_list,
                *unique_constraint_list,
                *time_stamp_list,
            )
            Index(f"{t}_parent", t.c.Id)
        else:
            # for all other tables, want Id to be unique throughout database
            t = Table(
                name,
                metadata,
                Column(
                    "Id",
                    Integer,
                    id_seq,
                    server_default=id_seq.next_value(),
                    primary_key=True,
                ),
                *field_col_list,
                *enum_id_list,
                *enum_other_list,
                *foreign_key_list,
                *null_constraint_list,
                *unique_constraint_list,
                *time_stamp_list,
            )
            Index(f"{t}_parent", t.c.Id)

    elif table_type == "enumerations":
        txt_col = "Txt"
        t = Table(
            name,
            metadata,
            Column(
                "Id",
                Integer,
                id_seq,
                server_default=id_seq.next_value(),
                primary_key=True,
            ),
            Column(txt_col, String, unique=True),
        )
        Index(f"{t}_parent", t.c.Id)

    elif table_type == "Joins":
        with open(os.path.join(t_path, "short_name.txt"), "r") as f:
            short_name = f.read().strip()
        # read info from files into dataframes

        fk = pd.read_csv(os.path.join(t_path, "foreign_keys.txt"), sep="\t")

        # define table
        col_list = [Column(r["fieldname"], Integer) for i, r in fk.iterrows()]
        null_constraint_list = [
            CheckConstraint(
                f'"{r["fieldname"]}" IS NOT NULL',
                name=f'{short_name}_{r["fieldname"]}_not_null',
            )
            for i, r in fk.iterrows()
        ]
        # omit 'foreign keys' that refer to more than one table,
        #  e.g. Contest_Id to BallotMeasureContest and CandidateContest
        true_foreign_key_list = [
            Column(r["fieldname"], ForeignKey(f'{r["refers_to"]}.Id'))
            for i, r in fk.iterrows()
            if ";" not in r["refers_to"]
        ]

        t = Table(
            name,
            metadata,
            Column(
                "Id",
                Integer,
                id_seq,
                server_default=id_seq.next_value(),
                primary_key=True,
            ),
            *col_list,
            *true_foreign_key_list,
            *null_constraint_list,
        )
        Index(f"{t}_parent", t.c.Id)
    else:
        raise Exception(f"table_type {table_type} not recognized")
    # create indices for efficiency
    if create_indices:
        for li in create_indices:
            Index(f"{t}_{li}_idx", t.c[li])
    return


def enum_table_list(dirpath="CDF_schema_def_info"):
    enum_path = os.path.join(dirpath, "enumerations")
    file_list = os.listdir(enum_path)
    for f in file_list:
        assert f[-4:] == ".txt", (
            "File name in " + dirpath + "enumerations/ not in expected form: " + f
        )
    e_table_list = [f[:-4] for f in file_list]
    return e_table_list


def fill_standard_tables(session, schema, dirpath="CDF_schema_def_info"):
    """Fill enumeration and BallotMeasureSelection tables"""
    # fill enumeration tables
    e_table_list = enum_table_list(dirpath)
    for f in e_table_list:
        txt_col = "Txt"
        # takes lines of text from file
        dframe = pd.read_csv(
            os.path.join(dirpath, "enumerations", f + ".txt"),
            header=None,
            names=[txt_col],
        )
        # insert to table
        dframe.to_sql(f, session.bind, schema=schema, if_exists="append", index=False)

    # fill BallotMeasureSelection table
    load_bms(session.bind, bmselections)
    session.flush()
    return e_table_list


def load_bms(engine, bms_list: list):
    bms_df = pd.DataFrame([[s] for s in bms_list], columns=["Name"])

    # Create 3 entries in Selection table
    id_list = db.add_records_to_selection_table(engine, len(bms_list))

    # Create entries in BallotMeasureSelection table
    bms_df["Id"] = pd.Series(id_list, index=bms_df.index)
    temp = pd.concat([pd.read_sql_table("Selection", engine), bms_df], axis=1)
    temp.to_sql("BallotMeasureSelection", engine, if_exists="append", index=False)

    return


def reset_db(session, dirpath):
    """Resets DB to a clean state with no tables/sequences.
    Used if a DB is created for a user but not populated, for example."""

    eng = session.bind
    conn = eng.connect()
    conn.execute("DROP SEQUENCE IF EXISTS id_seq CASCADE;")
    session.commit()

    # create enumeration tables
    e_table_list = enum_table_list(dirpath)
    for table in e_table_list:
        conn.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
        session.commit()

    element_path = os.path.join(dirpath, "elements")
    elements_to_process = [f for f in os.listdir(element_path) if f[0] != "."]
    # dynamic list of elements whose tables haven't been created yet
    for table in elements_to_process:
        conn.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
        session.commit()

    join_path = os.path.join(dirpath, "Joins")
    joins_to_process = [f for f in os.listdir(join_path) if f[0] != "."]
    for table in joins_to_process:
        conn.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
        session.commit()
    conn.close()
