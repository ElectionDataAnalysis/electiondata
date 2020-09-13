import os.path

from election_data_analysis import database as db
import pandas as pd
from pandas.api.types import is_numeric_dtype
from election_data_analysis import munge as m
from election_data_analysis import user_interface as ui
import re
import numpy as np
from pathlib import Path
import csv


# constants
munger_pars_req = ["file_type"]
munger_pars_opt = {
    "header_row_count": "int",
    "field_name_row": "int",
    "field_names_if_no_field_name_row": "list-of-strings",
    "count_columns": "list-of-integers",
    "thousands_separator": "str",
    "encoding": "str",
    "count_of_top_lines_to_skip": "int",
    "columns_to_skip": "list-of-integers",
    "last_header_column_count": "int",
    "column_width": "int",
}


def recast_options(options: dict, types: dict) -> dict:
    keys = {k for k in options.keys() if k in types.keys()}
    for k in keys:
        if types[k] == "int":
            try:
                options[k] = int(options[k])
            except:
                options[k] = None
        if types[k] == "list-of-integers":
            try:
                options[k] = [int(s) for s in options[k].split(",")]
            except:
                options[k] = list()
        if types[k] == "str":
            pass
        if types[k] == "list-of-strings":
            try:
                options[k] = [s for s in options[k].split(",")]
            except:
                options[k] = list()
        if types[k] == "int":
            try:
                options[k] = int(options[k])
            except:
                options[k] = None
    return options


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
            element_fpath, sep="\t", encoding="iso-8859-1", quoting=csv.QUOTE_MINIMAL
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
            if not f"{contest_type}Contest" in error:
                error[f"{contest_type}Contest"] = {}
            error[f"{contest_type}Contest"]["found_duplicates"] = True

        # insert into in Contest table
        e = db.insert_to_cdf_db(engine, df[["Name", "contest_type"]], "Contest")

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


class Munger:
    def get_aux_data(self, aux_data_dir, err) -> (dict, dict):
        """creates dictionary of dataframes, one for each auxiliary datafile.
        DataFrames returned are (multi-)indexed by the primary key(s)"""
        aux_data_dict = {}  # will hold dataframe for each abbreviated file name

        field_list = list(set([x[0] for x in self.auxiliary_fields()]))
        for abbrev in field_list:
            # get munger for the auxiliary file
            munger_path = os.path.join(self.path_to_munger_dir, abbrev)
            aux_mu, mu_err = check_and_init_munger(munger_path)
            if ui.fatal_error(mu_err):
                err = ui.consolidate_errors([err, mu_err])
                return dict(), err

            # find file in aux_data_dir whose name contains the string <afn>
            aux_filename_list = [x for x in os.listdir(aux_data_dir) if abbrev in x]
            if len(aux_filename_list) == 0:
                # TODO check this error
                err = ui.add_new_error(
                    err,
                    "file",
                    aux_data_dir,
                    f"No file found with name containing {abbrev}",
                )
            elif len(aux_filename_list) > 1:
                # TODO check this error
                err = ui.add_new_error(
                    err,
                    "file",
                    aux_data_dir,
                    f"Too many files found with name containing {abbrev}",
                )
            else:
                aux_path = os.path.join(aux_data_dir, aux_filename_list[0])

            # read and clean the auxiliary data file, including setting primary key columns as int
            df, err = ui.read_single_datafile(aux_mu, aux_path, err)

            # cast primary key(s) as int if possible, and set as (multi-)index
            primary_keys = self.aux_meta.loc[abbrev, "primary_key"].split(",")
            df = m.cast_cols_as_int(
                df, primary_keys, error_msg=f"In dataframe for {abbrev}"
            )
            df.set_index(primary_keys, inplace=True)

            aux_data_dict[abbrev] = df

        return aux_data_dict, err

    def auxiliary_fields(self):
        """Return set of [file_abbrev,field] pairs, one for each
        field in <self>.cdf_elements.fields referring to auxilliary files"""
        pat = re.compile("([^\\[]+)\\[([^\\[\\]]+)\\]")
        all_set = set().union(*list(self.cdf_elements.fields))
        aux_field_list = [re.findall(pat, x)[0] for x in all_set if re.findall(pat, x)]
        return aux_field_list

    def __init__(
        self,
        dir_path,
        aux_data_dir=None,
    ):
        """<dir_path> is the directory for the munger. If munger deals with auxiliary data files,
        <aux_data_dir> is the directory holding those files."""
        self.name = os.path.basename(dir_path)  # e.g., 'nc_general'
        self.path_to_munger_dir = dir_path
        # TODO make handling of these directories consistent

        [
            self.cdf_elements,
            self.file_type,
            self.encoding,
            self.thousands_separator,
            self.aux_meta,
            self.options,
        ] = read_munger_info_from_files(self.path_to_munger_dir)

        if aux_data_dir:
            self.aux_data = self.get_aux_data(aux_data_dir)
        else:
            self.aux_data = {}
        self.aux_data_dir = aux_data_dir

        # used repeatedly, so calculated once for convenience
        self.field_list = set()
        for t, r in self.cdf_elements.iterrows():
            self.field_list = self.field_list.union(r["fields"])


def check_and_init_munger(munger_path: str, aux_data_dir: str = None) -> (Munger, dict):
    err = check_munger_files(munger_path)
    if ui.fatal_error(err):
        munger = None
    else:
        munger = Munger(munger_path, aux_data_dir=aux_data_dir)
    return munger, err


def read_munger_info_from_files(dir_path):
    """<aux_data_dir> is required if there are auxiliary data files"""
    # create auxiliary dataframe
    if "aux_meta.txt" in os.listdir(dir_path):
        # if some elements are reported in separate files per auxilliary.txt file, read from file
        aux_meta = pd.read_csv(
            os.path.join(dir_path, "aux_meta.txt"),
            sep="\t",
            index_col="abbreviated_file_name",
        )
    else:
        # set auxiliary dataframe to empty
        aux_meta = pd.DataFrame([[]])

    # read cdf_element info
    cdf_elements = pd.read_csv(
        os.path.join(dir_path, "cdf_elements.txt"),
        sep="\t",
        index_col="name",
        encoding="iso-8859-1",
        quoting=csv.QUOTE_MINIMAL,
    ).fillna("")

    # add column for list of fields used in formulas
    cdf_elements["fields"] = [[]] * cdf_elements.shape[0]
    for i, r in cdf_elements.iterrows():
        text_field_list, last_text = m.text_fragments_and_fields(
            cdf_elements.loc[i, "raw_identifier_formula"]
        )
        cdf_elements.loc[i, "fields"] = [f for t, f in text_field_list]

    # read formatting info
    required_keys = munger_pars_req
    optional_keys = list(munger_pars_opt.keys())
    options, missing_required_params = ui.get_runtime_parameters(
        required_keys=required_keys,
        param_file=os.path.join(dir_path, "format.config"),
        header="format",
        optional_keys=optional_keys,
    )
    options = recast_options(options, munger_pars_opt)

    file_type = options["file_type"]
    if "encoding" in options.keys():
        encoding = options["encoding"]
    else:
        encoding = "iso-8859-1"
    if "thousands_separator" in options.keys() and options[
        "thousands_separator"
    ] not in ["", "None"]:
        thousands_separator = options["thousands_separator"]
    else:
        thousands_separator = None

    # TODO have options hold all optional parameters (and maybe even all parameters)
    #  and remove explicit attributes entirely?
    return [cdf_elements, file_type, encoding, thousands_separator, aux_meta, options]


# TODO combine ensure_jurisdiction_dir with ensure_juris_files
def ensure_jurisdiction_dir(juris_path, ignore_empty=False) -> dict:
    # create directory if it doesn't exist
    try:
        Path(juris_path).mkdir(parents=True)
    except FileExistsError:
        print(f"Directory already exists: {juris_path} ")
    else:
        print(f"Directory created: {juris_path}")

    # ensure the contents of the jurisdiction directory are correct
    err = ensure_juris_files(juris_path, ignore_empty=ignore_empty)
    return err


def ensure_juris_files(juris_path, ignore_empty=False) -> dict:
    """Check that the jurisdiction files are complete and consistent with one another.
    Check for extraneous files in Jurisdiction directory.
    Assumes Jurisdiction directory exists. Assumes dictionary.txt is in the template file"""

    # package possible errors from this function into a dictionary and return them
    err = None
    juris_name = Path(juris_path).name

    project_root = Path(__file__).parents[1].absolute()
    templates_dir = os.path.join(project_root, "templates", "jurisdiction_templates")
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
                encoding="iso-8859-1",
            )
        except pd.errors.EmptyDataError:
            if not ignore_empty:
                err = ui.add_new_error(
                    err,
                    "system",
                    "juris_and_munger.ensure_juris_files",
                    "Template file {" + juris_file + "}.txt has no contents",
                )
            temp = pd.DataFrame()
        # if file does not exist
        if not os.path.isfile(cf_path):
            # create the file
            temp.to_csv(cf_path, sep="\t", index=False)
            created = True

        # if file exists, check format against template
        if not created:
            cf_df = pd.read_csv(
                os.path.join(juris_path, f"{juris_file}.txt"),
                sep="\t",
                encoding="iso=8859-1",
                quoting=csv.QUOTE_MINIMAL,
            )
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
                dedupe(cf_path)
            else:
                # dedupe the file
                dedupe(cf_path)
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
    return err


def check_munger_files(munger_path: str) -> dict:
    """Check that the munger files are complete and consistent with one another.
    Assumes munger directory exists. Assumes dictionary.txt is in the template file.
    <munger_path> is the path to the directory of the particular munger
    """

    err = None
    project_root = Path(__file__).parents[1].absolute()
    munger_name = Path(munger_path).name

    # check whether directory exists
    if not os.path.isdir(munger_path):
        err = ui.add_new_error(
            err, "munger", munger_name, f"Directory does not exist: {munger_path}"
        )
        return err

    # check whether all files exist
    templates = os.path.join(project_root, "templates", "munger_templates")
    template_with_extension_list = os.listdir(templates)
    for munger_file in template_with_extension_list:
        # TODO create optional template for auxiliary.txt
        cf_path = os.path.join(munger_path, munger_file)
        # if file does not already exist in munger dir, throw error
        file_exists = os.path.isfile(cf_path)

        # if file exists, check format against template and then contents
        if file_exists:
            err = check_munger_file_format(munger_path, munger_file, templates, err)

            # if no errors found so far, check contents
            if not ui.fatal_error(
                err, error_type_list=["munger"], name_key_list=[munger_file]
            ):
                err = check_munger_file_contents(munger_path, munger_file, err)
        else:
            err = ui.add_new_error(err, "munger", munger_name, "File does not exist")
    return err


def check_munger_file_format(
    munger_path: str, munger_file: str, templates: str, err: dict
) -> dict:

    problems = list()
    if munger_file[-4:] == ".txt":
        cf_df = pd.read_csv(
            os.path.join(munger_path, munger_file), sep="\t", encoding="iso-8859-1"
        )
        temp = pd.read_csv(
            os.path.join(templates, munger_file), sep="\t", encoding="iso-8859-1"
        )

        # check column names are correct
        if set(cf_df.columns) != set(temp.columns):
            err = ui.add_new_error(
                err,
                "munger",
                munger_path,
                f"Columns in {munger_file} do not match template.:\n"
                f"Columns of {munger_file}: {cf_df.columns}\n"
                f"Columns of template: {temp.columns}",
            )

    elif munger_file == "format.config":
        d, err = ui.get_runtime_parameters(
            required_keys=munger_pars_req,
            param_file=os.path.join(munger_path, munger_file),
            header="format",
            err=err,
            optional_keys=list(munger_pars_opt.keys()),
        )
    else:
        err = ui.add_new_error(
            err,
            "munger",
            munger_path,
            f"Unrecognized file in munger: {munger_file}",
        )
    return err


def check_munger_file_contents(munger_path, munger_file, err):
    """check whether munger files are internally consistent"""
    munger_name = Path(munger_path).name
    if munger_file == "cdf_elements.txt":
        # read cdf_elements and format from files
        cdf_elements = pd.read_csv(
            os.path.join(munger_path, "cdf_elements.txt"),
            sep="\t",
            encoding="iso-8859-1",
        ).fillna("")

        # every source in cdf_elements is either row, column or other
        bad_source = [x for x in cdf_elements.source if x not in ["row", "column"]]
        if bad_source:
            err = ui.add_new_error(
                err,
                "warn-munger",
                munger_name,
                f"Source(s) in cdf_elements.txt not recognized: {bad_source}",
            )

        # formulas have good syntax
        bad_formula = [
            x
            for x in cdf_elements.raw_identifier_formula.unique()
            if not m.good_syntax(x)
        ]
        if bad_formula:
            f_str = ",".join(bad_formula)
            err = ui.add_new_error(
                err,
                "warn-munger",
                munger_name,
                f"At least one formula in cdf_elements.txt has bad syntax: {f_str}",
            )

        # for each column-source record in cdf_element, contents of bracket are numbers in the header_rows
        p_not_just_digits = re.compile(r"<.*\D.*>")
        p_catch_digits = re.compile(r"<(\d+)>")
        bad_column_formula = set()

        # TODO check: can this error out now?
        for i, r in cdf_elements[cdf_elements.source == "column"].iterrows():
            if p_not_just_digits.search(r["raw_identifier_formula"]):
                bad_column_formula.add(r["raw_identifier_formula"])
            else:
                integer_list = [
                    int(x) for x in p_catch_digits.findall(r["raw_identifier_formula"])
                ]
                bad_integer_list = [
                    x
                    for x in integer_list
                    if (x > int(format_d["header_row_count"]) - 1 or x < 0)
                ]
                if bad_integer_list:
                    bad_column_formula.add(r["raw_identifier_formula"])
        if bad_column_formula:
            err = ui.add_new_error(
                err,
                "munger",
                munger_name,
                f"At least one column-source formula in cdf_elements.txt has bad syntax: {bad_column_formula}",
            )

    elif munger_file == "format.config":
        format_d, err = ui.get_runtime_parameters(
            required_keys=munger_pars_req,
            param_file=os.path.join(munger_path, "format.config"),
            header="format",
            err=err,
            optional_keys=list(munger_pars_opt.keys()),
        )

        # warn if encoding missing or is not recognized
        if "encoding" not in format_d.keys():
            err = ui.add_new_error(
                err,
                "warn-munger",
                munger_name,
                f"No encoding specified; iso-8859-1 will be used",
            )
        elif not format_d["encoding"] in ui.recognized_encodings:
            err = ui.add_new_error(
                err,
                "warn-munger",
                munger_name,
                (
                    f"Encoding {format_d['encoding']} in format file is not recognized;"
                    f"iso-8859-1 will be used"
                ),
            )

        # check all parameters for flat files
        if format_d["file_type"] in ["txt", "csv", "xls"]:
            # Either field_name_row is a number, or field_names_if_no_field_name_row is a non-empty list
            if (
                (not format_d["field_name_row"])
                or (not format_d["field_name_row"].isnumeric())
                and (
                    (not format_d["field_names_if_no_field_name_row"])
                    or len(format_d["field_names_if_no_field_name_row"]) == 0
                )
            ):
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    (
                        f"field_name_row is not an integer, "
                        f"but no field names are give in field_names_if_no_field_name_row."
                    ),
                )

            # other entries in format.config are of correct type
            try:
                int(format_d["header_row_count"])
            except (TypeError, ValueError):
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f'header_row_count is not an integer:  {format_d["header_row_count"]}',
                )

        # check all parameters for concatenated blocks (e.g., Georgia ExpressVote output)
        elif format_d["file_type"] in ["concatenated-blocks"]:
            for key in [
                "count_of_top_lines_to_skip",
                "last_header_column_count",
                "column_width",
            ]:
                try:
                    int(format_d[key])
                except (ValueError, TypeError):
                    err = ui.add_new_error(
                        err,
                        "munger",
                        munger_name,
                        f"{key} is not an integer:  {format_d[key]}",
                    )
    else:
        err = ui.add_new_error(
            err,
            "system",
            "juris_and_munger.check_munger_file_contents",
            f"Munger template file not recognized: {munger_file}",
        )

    return err


def dedupe(f_path):
    # TODO allow specification of unique constraints
    df = pd.read_csv(f_path, sep="\t", encoding="iso-8859-1", quoting=csv.QUOTE_MINIMAL)
    dupe = ""
    dupes_df, df = ui.find_dupes(df)
    if not dupes_df.empty:
        df.to_csv(f_path, sep="\t", index=False)
    return


def check_nulls(element, f_path, project_root):
    # TODO write description
    # TODO automatically drop null rows
    nn_path = os.path.join(
        project_root,
        "CDF_schema_def_info","elements",
        element,
        "not_null_fields.txt",
    )
    not_nulls = pd.read_csv(nn_path, sep="\t", encoding="iso-8859-1")
    df = pd.read_csv(f_path, sep="\t", encoding="iso-8859-1", quoting=csv.QUOTE_MINIMAL)

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
            encoding="iso-8859-1",
            quoting=csv.QUOTE_MINIMAL,
        )
    except FileNotFoundError:
        err = ui.add_new_error(
            err,
            "system",
            "juris_and_munger.check_dependencies",
            f"file doesn't exist: {f_path}",
        )

    # Find all dependent columns
    dependent = [c for c in element_df if c in d.keys()]
    changed_elements = set()
    for c in dependent:
        target = d[c]
        ed = (
            pd.read_csv(
                os.path.join(juris_dir, f"{element}.txt"),
                sep="\t",
                header=0,
                encoding="iso-8859-1",
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
                encoding="iso-8859-1",
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


# TODO before processing jurisdiction files into db, alert user to any duplicate names.
#  Enforce name change? Or just suggest?
def load_juris_dframe_into_cdf(session, element, juris_path, error) -> dict:
    """TODO"""
    project_root = Path(__file__).parents[1].absolute()
    cdf_schema_def_dir = os.path.join(
        project_root,
        "CDF_schema_def_info",
    )
    element_fpath = os.path.join(juris_path, f"{element}.txt")
    if not os.path.exists(element_fpath):
        error = ui.add_new_error(
            error,
            "jurisdiction",
            Path(juris_path).name,
            f"File {element}.txt not found"
        )
        return error
    df = pd.read_csv(
        element_fpath, sep="\t", encoding="iso-8859-1", quoting=csv.QUOTE_MINIMAL
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
            f"Duplicates were found in {element}.txt"
        )

    # replace plain text enumerations from file system with id/othertext from db
    enum_file = os.path.join(
        cdf_schema_def_dir, "elements", element, "enumerations.txt"
    )
    if os.path.isfile(enum_file):  # (if not, there are no enums for this element)
        enums = pd.read_csv(enum_file, sep="\t")
        # get all relevant enumeration tables
        for e in enums["enumeration"]:  # e.g., e = "ReportingUnitType"
            cdf_e = pd.read_sql_table(e, session.bind)
            # for every instance of the enumeration in the current table, add id and othertype columns to the dataframe
            if e in df.columns:
                df = m.enum_col_to_id_othertext(df, e, cdf_e)

    # get Ids for any foreign key (or similar) in the table, e.g., Party_Id, etc.
    fk_file_path = os.path.join(
        cdf_schema_def_dir, "elements", element, "foreign_keys.txt"
    )
    if os.path.isfile(fk_file_path):
        foreign_keys = pd.read_csv(fk_file_path, sep="\t", index_col="fieldname")

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
            "juris_and_munger.load_juris_dframe_into_cdf",
            f"Error loading {element} to database: {e}"
        )
    return error


def get_ids_for_foreign_keys(
    session, df1, element, foreign_key, refs, load_refs, error
):
    """ TODO <fn> is foreign key"""
    df = df1.copy()
    # append the Id corresponding to <fn> from the db
    foreign_elt = f"{foreign_key[:-3]}"
    interim = f"{foreign_elt}_Name"

    target_list = []
    for r in refs:
        ref_name_field = db.get_name_field(r)

        r_target = pd.read_sql_table(r, session.bind)[["Id", ref_name_field]]
        r_target.rename(
            columns={"Id": foreign_key, ref_name_field: interim}, inplace=True
        )

        target_list.append(r_target)

    target = pd.concat(target_list)

    df = df.merge(target, how="left", left_on=foreign_elt, right_on=interim)

    # TODO might have to check for '' or 0 as well as nulls
    missing = df[(df[foreign_elt].notnull()) & (df[interim].isnull())]
    if missing.empty:
        df.drop([interim], axis=1)
    else:
        if load_refs:
            # Always try to handle/fill in the missing IDs
            raise ForeignKeyException(
                f"For some {element} records, {foreign_elt} was not found"
            )
        else:
            if not element in error:
                error = ui.add_new_error(
                    error,
                    "system",
                    "juris_and_munger.get_ids_for_foreign_keys",
                    f"For some {element} records, {foreign_elt} was not found",
                )
    return df


def check_results_munger_compatibility(
    mu: Munger, df: pd.DataFrame, file_name, error: dict
) -> dict:
    # check that count columns exist
    missing = [i for i in mu.options["count_columns"] if i >= df.shape[1]]
    if missing:
        error = ui.add_new_error(
            error,
            "munger",
            mu.name,
            f"Only {df.shape[1]} columns read from results file {file_name}. Check file_type in format.config",
        )
    else:
        # check that count cols are numeric
        for i in mu.options["count_columns"]:
            if not is_numeric_dtype(df.iloc[:, i]):
                try:
                    df.iloc[:, i] = df.iloc[:, i].astype(int)
                except ValueError as ve:
                    error = ui.add_new_error(
                        error,
                        "munger",
                        mu.name,
                        f"Column {i} ({df.columns[i]}) cannot be parsed as an integer.\n{ve}",
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


class ForeignKeyException(Exception):
    pass


if __name__ == "__main__":
    print("Done (juris_and_munger)!")
