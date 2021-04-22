import os.path

from election_data_analysis import database as db
import pandas as pd
from pandas.api.types import is_numeric_dtype
from typing import Optional, Dict, Any
from election_data_analysis import munge as m
from election_data_analysis import user_interface as ui
from election_data_analysis import preparation as prep
import numpy as np
from pathlib import Path
import csv
import inspect


def recast_options(
    options: Dict[str, str], types: Dict[str, str], munger_name: str
) -> (dict, Dict[str, Any]):
    """Convert a dictionary <options> of string parameter values to typed objects,
    where type is determined by <types>"""
    err: Dict[str, Any] = None
    keys = {k for k in options.keys() if k in types.keys()}
    for k in keys:
        if options[k]:
            if types[k] in ["int", "integer"]:
                try:
                    options[k] = int(options[k])
                except Exception:
                    options[k] = None
                    err = ui.add_new_error(
                        err,
                        "warn-munger",
                        munger_name,
                        f"{k} should be integer but isn't: {options[k]}",
                    )
            elif types[k] == "list-of-integers":
                try:
                    options[k] = [int(s) for s in options[k].split(",")]
                except Exception:
                    options[k] = list()
                    err = ui.add_new_error(
                        err,
                        "warn-munger",
                        munger_name,
                        f"{k} should be list of integers but isn't: {options[k]}",
                    )
            elif types[k] == "str":
                if options[k] == "":
                    # null string is read as None
                    options[k] = None
            elif types[k] == "list-of-strings":
                if options[k] != "":
                    try:
                        options[k] = [s for s in options[k].split(",")]
                    except Exception:
                        options[k] = list()
                        err = ui.add_new_error(
                            err,
                            "warn-munger",
                            munger_name,
                            f"{k} should be list of strings but isn't: {options[k]}",
                        )
                # if the string is empty, assign an empty list
                else:
                    options[k] = list()

            elif types[k] == "string-with-opt-list" and k == "count_location":
                if not options["count_location"]:
                    # if we're munging a lookup file
                    pass
                elif options["file_type"] in ["xml", "json-nested"]:
                    # nothing needs to be broken out for these file types
                    pass
                elif options[k].split(":")[0] == "by_name":
                    options["count_fields_by_name"] = [s for s in options[k][8:].split(",")]
                    options[k] = "by_name"
                elif options[k].split(":")[0] == "by_number":
                    options["count_column_numbers"] = [
                        int(s) for s in options[k][10:].split(",")
                    ]
                    options[k] = "by_number"
    return options, err


class Jurisdiction:
    def load_contests(self, engine, contest_type: str, error: dict) -> dict:
        # read <contest_type>Contests from jurisdiction folder
        element_fpath = os.path.join(
            self.path_to_juris_dir, f"{contest_type}Contest.txt"
        )
        if not os.path.exists(element_fpath):
            error[f"{contest_type}Contest.txt"] = "file not found"
            return error
        df = pd.read_csv(
            element_fpath,
            sep="\t",
            encoding=m.default_encoding,
            quoting=csv.QUOTE_MINIMAL,
        ).fillna("none or unknown")

        # add contest_type column
        df = m.add_constant_column(df, "contest_type", contest_type)

        # add 'none or unknown' record
        df = add_none_or_unknown(df, contest_type=contest_type)

        # dedupe df
        dupes, df = ui.find_dupes(df)
        if not dupes.empty:
            print(
                f"WARNING: duplicates removed from dataframe, may indicate a problem.\n"
            )
            if f"{contest_type}Contest" not in error:
                error[f"{contest_type}Contest"] = {}
            error[f"{contest_type}Contest"]["found_duplicates"] = True

        # insert into in Contest table
        e = db.insert_to_cdf_db(engine, df[["Name", "contest_type"]], "Contest")
        if e:
            error = ui.add_new_error(
                error,
                "warn-system",
                f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                f"On Contest table insertion: {e}",
            )

        # append Contest_Id
        col_map = {"Name": "Name", "contest_type": "contest_type"}
        df = db.append_id_to_dframe(engine, df, "Contest", col_map=col_map)

        if contest_type == "BallotMeasure":
            # append ElectionDistrict_Id, Election_Id
            for fk, ref in [
                ("ElectionDistrict", "ReportingUnit"),
                ("Election", "Election"),
            ]:
                col_map = {fk: "Name"}
                df = (
                    db.append_id_to_dframe(engine, df, ref, col_map=col_map)
                    .rename(columns={f"{ref}_Id": f"{fk}_Id"})
                    .drop(fk, axis=1)
                )

        else:
            # append Office_Id, PrimaryParty_Id
            for fk, ref in [("Office", "Office"), ("PrimaryParty", "Party")]:
                col_map = {fk: "Name"}
                df = db.append_id_to_dframe(engine, df, ref, col_map=col_map).rename(
                    columns={f"{ref}_Id": f"{fk}_Id"}
                )

        # create entries in <contest_type>Contest table
        # commit info in df to <contest_type>Contest table to db
        err = db.insert_to_cdf_db(
            engine, df.rename(columns={"Contest_Id": "Id"}), f"{contest_type}Contest"
        )
        if err:
            if f"{contest_type}Contest" not in error:
                error[f"{contest_type}Contest"] = {}
            error[f"{contest_type}Contest"]["database"] = err
        return error

    def load_juris_to_db(self, session) -> dict:
        """Load info from each element in the Jurisdiction's directory into the db"""
        # load all from Jurisdiction directory (except Contests, dictionary, remark)
        juris_elements = ["ReportingUnit", "Office", "Party", "Candidate", "Election"]

        error = dict()
        for element in juris_elements:
            # read df from Jurisdiction directory
            error = load_juris_dframe_into_cdf(
                session, element, self.path_to_juris_dir, error
            )

        # Load CandidateContests and BallotMeasureContests
        error = dict()
        for contest_type in ["BallotMeasure", "Candidate"]:
            error = self.load_contests(session.bind, contest_type, error)

        if error == dict():
            error = None
        return error

    def __init__(self, path_to_juris_dir):
        self.short_name = Path(path_to_juris_dir).name
        self.path_to_juris_dir = path_to_juris_dir


def ensure_jurisdiction_dir(
    juris_path: str, ignore_empty: bool = False
) -> Optional[dict]:
    # create directory if it doesn't exist
    try:
        Path(juris_path).mkdir(parents=True)
    except FileExistsError:
        pass
    else:
        print(f"Directory created: {juris_path}")

    # ensure the contents of the jurisdiction directory are correct
    err = ensure_juris_files(juris_path, ignore_empty=ignore_empty)
    return err


def ensure_juris_files(juris_path: str, ignore_empty: bool = False) -> Optional[dict]:
    """Check that the jurisdiction files are complete and consistent with one another.
    Check for extraneous files in Jurisdiction directory.
    Assumes Jurisdiction directory exists. Assumes dictionary.txt is in the template file"""

    # package possible errors from this function into a dictionary and return them
    err = None
    juris_name = Path(juris_path).name
    juris_true_name = juris_name.replace("-", " ")

    project_root = Path(__file__).parents[1].absolute()
    templates_dir = os.path.join(
        project_root, "juris_and_munger", "jurisdiction_templates"
    )
    # notify user of any extraneous files
    extraneous = [
        f
        for f in os.listdir(juris_path)
        if f not in os.listdir(templates_dir) and f[0] != "."
    ]
    if extraneous:
        err = ui.add_new_error(
            err,
            "jurisdiction",
            juris_name,
            f"extraneous_files_in_juris_directory {extraneous}",
        )

    template_list = [x[:-4] for x in os.listdir(templates_dir)]

    # reorder template_list, so that first things are created first
    ordered_list = ["dictionary", "ReportingUnit", "Office", "CandidateContest"]
    template_list = ordered_list + [x for x in template_list if x not in ordered_list]

    # ensure necessary all files exist
    for juris_file in template_list:
        # a list of file empty errors
        cf_path = os.path.join(juris_path, f"{juris_file}.txt")
        created = False
        # if file does not already exist in jurisdiction directory, create from template and invite user to fill
        try:
            temp = pd.read_csv(
                os.path.join(templates_dir, f"{juris_file}.txt"),
                sep="\t",
                encoding=m.default_encoding,
            )
        except pd.errors.EmptyDataError:
            if not ignore_empty:
                err = ui.add_new_error(
                    err,
                    "system",
                    f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                    "Template file {" + juris_file + "}.txt has no contents",
                )
            temp = pd.DataFrame()
        # if file does not exist
        if not os.path.isfile(cf_path):
            # create the file
            temp.to_csv(cf_path, sep="\t", index=False, encoding=m.default_encoding)
            created = True

        # if file exists, check format against template
        if not created:
            try:
                cf_df = pd.read_csv(
                    os.path.join(juris_path, f"{juris_file}.txt"),
                    sep="\t",
                    encoding="iso=8859-1",
                    quoting=csv.QUOTE_MINIMAL,
                )
            except pd.errors.ParserError as e:
                err = ui.add_new_error(
                    err,
                    "jurisdiction",
                    juris_name,
                    f"Error reading file {juris_file}.txt: {e}",
                )
                return err

            if set(cf_df.columns) != set(temp.columns):
                print(juris_file)
                cols = "\t".join(temp.columns.to_list())
                err = ui.add_new_error(
                    err,
                    "jurisdiction",
                    juris_name,
                    f"Columns of {juris_file}.txt need to be (tab-separated):\n {cols}\n",
                )

            if juris_file == "dictionary":
                # dedupe the dictionary
                clean_and_dedupe(cf_path)

            else:
                # dedupe the file
                clean_and_dedupe(cf_path)
                # check for problematic null entries
                null_columns = check_nulls(juris_file, cf_path, project_root)
                if null_columns:
                    err = ui.add_new_error(
                        err,
                        "jurisdiction",
                        juris_name,
                        f"Null entries in {juris_file} in columns {null_columns}",
                    )

    # check dependencies
    for juris_file in [x for x in template_list if x != "remark" and x != "dictionary"]:
        # check dependencies
        d, new_err = check_dependencies(juris_path, juris_file)
        if new_err:
            err = ui.consolidate_errors([err, new_err])

    # check ReportingUnit.txt for internal consistency
    new_err = check_ru_file(juris_path, juris_true_name)
    return err


def check_ru_file(juris_path: str, juris_true_name: str) -> Optional[dict]:
    err = None
    ru = prep.get_element(juris_path, "ReportingUnit")

    # create set of all parents, all lead rus
    parents = set()
    leadings = set()
    for _, r in ru.iterrows():
        components = r["Name"].split(";")
        parents.update(
            {";".join(components[: j + 1]) for j in range(len(components) - 1)}
        )
        leadings.update({components[0]})

    # identify and report parents that are missing from ReportingUnit.txt
    missing = [p for p in parents if p not in ru["Name"].unique()]
    missing.sort(reverse=True)
    if missing:
        m_str = "\n".join(missing)
        err = ui.add_new_error(
            err,
            "jurisdiction",
            Path(juris_path).name,
            f"Some parent reporting units are missing from ReportingUnit.txt:\n{m_str}",
        )

    # check that all reporting units start with true name
    bad = [j for j in leadings if j != juris_true_name]
    if bad:
        bad.sort(reverse=True)
        bad_str = "\n".join(bad)
        err = ui.add_new_error(
            err,
            "jurisdiction",
            Path(juris_path).name,
            f"Every ReportingUnit should start with the jurisdiction name. These do not:\n{bad_str}",
        )

    return err


def clean_and_dedupe(f_path: str):
    """Dedupe the file, removing any leading or trailing whitespace and compressing any internal whitespace"""
    # TODO allow specification of unique constraints
    df = pd.read_csv(
        f_path, sep="\t", encoding=m.default_encoding, quoting=csv.QUOTE_MINIMAL
    )

    for c in df.columns:
        if not is_numeric_dtype(df.dtypes[c]):
            df[c].fillna("", inplace=True)
            try:
                df[c] = df[c].apply(m.compress_whitespace)
            except Exception:
                # failure shouldn't break anything
                print(f"No whitespace compression on column {c} of {f_path}")
                pass
    dupes_df, df = ui.find_dupes(df)
    if not dupes_df.empty:
        df.to_csv(f_path, sep="\t", index=False, encoding=m.default_encoding)
    return


def check_nulls(element, f_path, project_root):
    # TODO write description
    # TODO automatically drop null rows
    nn_path = os.path.join(
        project_root,
        "CDF_schema_def_info",
        "elements",
        element,
        "not_null_fields.txt",
    )
    not_nulls = pd.read_csv(nn_path, sep="\t", encoding=m.default_encoding)
    df = pd.read_csv(
        f_path, sep="\t", encoding=m.default_encoding, quoting=csv.QUOTE_MINIMAL
    )

    problem_columns = []

    for nn in not_nulls.not_null_fields.unique():
        # if nn is an Id, name in jurisdiction file is element name
        if nn[-3:] == "_Id":
            nn = nn[:-3]
        n = df[df[nn].isnull()]
        if not n.empty:
            problem_columns.append(nn)
            # drop offending rows
            df = df[df[nn].notnull()]

    return problem_columns


def check_dependencies(juris_dir, element) -> (list, dict):
    """Looks in <juris_dir> to check that every dependent column in <element>.txt
    is listed in the corresponding jurisdiction file. Note: <juris_dir> assumed to exist.
    """
    err = None
    juris_name = Path(juris_dir).name
    d = juris_dependency_dictionary()
    f_path = os.path.join(juris_dir, f"{element}.txt")
    try:
        element_df = pd.read_csv(
            f_path,
            sep="\t",
            index_col=None,
            encoding=m.default_encoding,
            quoting=csv.QUOTE_MINIMAL,
        )
    except FileNotFoundError:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"file doesn't exist: {f_path}",
        )
        return list(), err

    # Find all dependent columns
    dependent = [c for c in element_df.columns if c in d.keys()]
    changed_elements = set()
    for c in dependent:
        target = d[c]
        ed = (
            pd.read_csv(
                os.path.join(juris_dir, f"{element}.txt"),
                sep="\t",
                header=0,
                encoding=m.default_encoding,
                quoting=csv.QUOTE_MINIMAL,
            )
            .fillna("")
            .loc[:, c]
            .unique()
        )

        # create list of elements, removing any nulls
        ru = list(
            pd.read_csv(
                os.path.join(juris_dir, f"{target}.txt"),
                sep="\t",
                encoding=m.default_encoding,
                quoting=csv.QUOTE_MINIMAL,
            )
            .fillna("")
            .loc[:, db.get_name_field(target)]
        )
        try:
            ru.remove(np.nan)
        except ValueError:
            pass

        missing = [x for x in ed if x not in ru]
        # if the only missing is null or blank
        if len(missing) == 1 and missing == [""]:
            # exclude PrimaryParty, which isn't required to be not-null
            if c != "PrimaryParty":
                err = ui.add_new_error(
                    err, "jurisdiction", juris_name, f"Some {c} are null."
                )
        elif missing:
            changed_elements.add(element)
            changed_elements.add(target)
            m_str = "\n".join(missing)
            err = ui.add_new_error(
                err,
                "jurisdiction",
                juris_name,
                f"Every {c} in {element}.txt must be in {target}.txt. Offenders are:\n{m_str}",
            )

    return changed_elements, err


def juris_dependency_dictionary():
    """Certain fields in jurisdiction files refer to other jurisdiction files.
    E.g., ElectionDistricts are ReportingUnits"""
    d = {
        "ElectionDistrict": "ReportingUnit",
        "Office": "Office",
        "PrimaryParty": "Party",
        "Party": "Party",
        "Election": "Election",
    }
    return d


def load_juris_dframe_into_cdf(
    session, element, juris_path, error: Optional[dict]
) -> Optional[dict]:
    """TODO"""

    # define paths
    project_root = Path(__file__).parents[1].absolute()
    cdf_schema_def_dir = os.path.join(
        project_root,
        "CDF_schema_def_info",
    )
    element_file = os.path.join(juris_path, f"{element}.txt")
    enum_file = os.path.join(
        cdf_schema_def_dir, "elements", element, "enumerations.txt"
    )
    fk_file = os.path.join(cdf_schema_def_dir, "elements", element, "foreign_keys.txt")

    # fail if <element>.txt does not exist
    if not os.path.exists(element_file):
        error = ui.add_new_error(
            error,
            "jurisdiction",
            Path(juris_path).name,
            f"File {element}.txt not found",
        )
        return error

    # read info from <element>.txt, filling null fields with 'none or unknown'
    df = pd.read_csv(
        element_file, sep="\t", encoding="utf_8", quoting=csv.QUOTE_MINIMAL
    ).fillna("none or unknown")
    # TODO check that df has the right format

    # add 'none or unknown' record
    df = add_none_or_unknown(df)

    # dedupe df
    dupes, df = ui.find_dupes(df)
    if not dupes.empty:
        error = ui.add_new_error(
            error,
            "warn-jurisdiction",
            Path(juris_path).name,
            f"Duplicates were found in {element}.txt",
        )

    # replace plain text enumerations from file system with id/othertext from db
    if os.path.isfile(enum_file):  # (if not, there are no enums for this element)
        enums = pd.read_csv(enum_file, sep="\t")
        # get all relevant enumeration tables
        for e in enums["enumeration"]:  # e.g., e = "ReportingUnitType"
            cdf_e = pd.read_sql_table(e, session.bind)
            # for every instance of the enumeration in the current table, add id and othertype columns to the dataframe
            if e in df.columns:
                df, non_standard = m.enum_col_to_id_othertext(df, e, cdf_e)
                if non_standard:
                    ns = "\n\t".join(non_standard)
                    error = ui.add_new_error(
                        error,
                        "warn-jurisdiction",
                        Path(juris_path).name,
                        f"Some {e}s are non-standard:\n\t{ns}",
                    )

    # get Ids for any foreign key (or similar) in the table, e.g., Party_Id, etc.
    if os.path.isfile(fk_file):
        foreign_keys = pd.read_csv(fk_file, sep="\t", index_col="fieldname")

        for fn in foreign_keys.index:
            ref = foreign_keys.loc[
                fn, "refers_to"
            ]  # NB: juris elements have no multiple referents (as joins may)
            col_map = {fn[:-3]: db.get_name_field(ref)}
            df = db.append_id_to_dframe(session.bind, df, ref, col_map=col_map).rename(
                columns={f"{ref}_Id": fn}
            )

    # commit info in df to corresponding cdf table to db
    err_string = db.insert_to_cdf_db(session.bind, df, element)
    if err_string:
        error = ui.add_new_error(
            error,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Error loading {element} to database: {err_string}",
        )
    return error


def add_none_or_unknown(df: pd.DataFrame, contest_type: str = None) -> pd.DataFrame:
    new_row = dict()
    for c in df.columns:
        if c == "contest_type":
            new_row[c] = contest_type
        elif c == "NumberElected":
            new_row[c] = 0
        elif df[c].dtype == "O":
            new_row[c] = "none or unknown"
        elif pd.api.types.is_numeric_dtype(df[c]):
            new_row[c] = 0
    # append row to the dataframe
    df = df.append(new_row, ignore_index=True)
    return df


if __name__ == "__main__":
    print("Done (juris_and_munger)!")
