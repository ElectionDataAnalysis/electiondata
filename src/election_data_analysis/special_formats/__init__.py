import pandas as pd
import io
import xml.etree.ElementTree as et
from pathlib import Path
from typing import Optional, Dict, List
from election_data_analysis import munge as m
from election_data_analysis import juris_and_munger as jm
from election_data_analysis import user_interface as ui


def disambiguate(li: list) -> (list, dict):
    """returns new list, with numbers added to any repeat entries
    (e.g., ['foo','foo','bar'] yields ['foo','foo 1','bar'])
    and a dictionary for the alternatives (e.g., alts = {'foo 1':'foo'})"""
    c = dict()
    alts = dict()
    new_li = []
    for x in li:
        if x in c.keys():
            new = f"{x} {c[x]}"
            new_li.append(new)
            alts[new] = x
            c[x] += 1
        else:
            new_li.append(x)
            c[x] = 1
    return new_li, alts


def strip_empties(li: list) -> list:
    # get rid of leading empty strings
    first_useful = next(idx for idx in range(len(li)) if li[idx] != "")
    li = li[first_useful:]

    # get rid of trailing empty strings
    li.reverse()
    first_useful = next(idx for idx in range(len(li)) if li[idx] != "")
    li = li[first_useful:]
    li.reverse()

    return li


def remove_by_index(main_list: list, idx_list: list):
    """creates new list by removing from <new_list> indices indicated in <idx_list>.
    Indices in <idx_list> can be negative or positive. Positive indices are
    removed first."""
    # TODO error checking for overlapping neg & pos indices
    new_list = main_list.copy()
    not_neg = [idx for idx in idx_list if idx >= 0]
    not_neg.sort()
    not_neg.reverse()
    for idx in not_neg:
        new_list.pop(idx)
    neg = [idx for idx in idx_list if idx < 0]
    neg.sort()
    for idx in neg:
        new_list.pop(idx)
    return new_list


def extract_items(line: str, w: int) -> list:
    """assume line ends in \n.
    drops any trailing empty strings from list"""
    item_list = [
        line[idx * w : (idx + 1) * w].strip() for idx in range(int((len(line) - 1) / w))
    ]
    item_list = strip_empties(item_list)
    return item_list


def read_alternate_munger(
        file_type: str,
        f_path: str,
        munger: jm.Munger,
        err: dict
) -> (pd.DataFrame, dict):
    if file_type in ["concatenated-blocks"]:
        raw_results, err = read_concatenated_blocks(f_path, munger, err)
    elif file_type in ["xls-multi"]:
        raw_results, err = read_multi_sheet_excel(f_path, munger, err)
    else:
        err = ui.add_new_error(
            err,
            "munger",
            munger.name,
            f"file type not recognized: {file_type}"
        )
        raw_results = pd.DataFrame()
    return raw_results, err


def read_concatenated_blocks(
    f_path: str, munger: jm.Munger, err: dict
) -> (pd.DataFrame, dict):
    """Assumes first column of each block is ReportingUnit, last column is contest total"""
    try:
        with open(f_path, "r") as f:
            data = f.readlines()
    except Exception as exc:
        err = ui.add_new_error(err, "file", f_path, f"Datafile not read:\n{exc}\n")
        return pd.DataFrame(), err

    # get  munger parameters
    w = munger.options["column_width"]
    tlts = munger.options["count_of_top_lines_to_skip"]
    v_t_cc = munger.options["last_header_column_count"]
    skip_cols = munger.options["columns_to_skip"]

    df = dict()

    # skip lines at top
    data = data[tlts:]

    try:
        while len(data) > 3:
            # TODO allow number & interps of headers to vary?
            # get rid of blank lines
            while data[0] == "\n":
                data.pop(0)

            # get the header lines
            header_0 = data.pop(0).strip()
            header_1 = data.pop(0)
            header_line = data.pop(0)

            # get info from header line
            field_list = extract_items(header_line, w)

            # remove first column header and headers of any columns to be skipped
            last_header = remove_by_index(field_list, [0] + skip_cols)

            # check that the size of the side-to-side repeated block is consistent
            if len(last_header) % v_t_cc != 0:
                e = (
                    f"Count of last header (per munger) ({v_t_cc}) "
                    f"does not evenly divide the number of count columns in the results file "
                    f"({len(last_header)})"
                )
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger.name,
                    e,
                )
                return pd.DataFrame(), err

            # get list from next header row and disambiguate
            # TODO tech debt: disambiguation assumes Candidate formula is <header_1>
            header_1_list, alts = disambiguate(extract_items(header_1, w * v_t_cc))

            #  add disambiguated entries to munger's dictionary of alternatives
            if "Candidate" in munger.alt.keys():
                munger.alt["Candidate"].update(alts)
            else:
                munger.alt["Candidate"] = alts

            # create df from next batch of lines, with that multi-index
            # find idx of next empty line (or end of data)
            try:
                next_empty = next(idx for idx in range(len(data)) if data[idx] == "\n")
            except StopIteration:
                next_empty = len(data)
            # create io
            vote_count_block = io.StringIO()
            vote_count_block.write("".join(data[:next_empty]))
            vote_count_block.seek(0)

            df[header_0] = pd.read_fwf(
                vote_count_block, colspecs="infer", index=False, header=None
            )

            # Drop extraneous columns (per munger). Negative numbers count from right side
            df[header_0].drop(df[header_0].columns[skip_cols], axis=1, inplace=True)

            # make first column into an index
            df[header_0].set_index(keys=[0], inplace=True)

            # add multi-index with header_1 and header_2 info
            index_array = [
                [y for z in [[cand] * v_t_cc for cand in header_1_list] for y in z],
                last_header,
            ]
            df[header_0].columns = pd.MultiIndex.from_arrays(index_array)

            # move header_1 & header_2 info to columns
            df[header_0] = pd.melt(
                df[header_0],
                ignore_index=False,
                value_vars=df[header_0].columns.tolist(),
                value_name="count",
                var_name=["header_1", "header_2"],
            )

            # Add columns for header_0
            df[header_0] = m.add_constant_column(df[header_0], "header_0", header_0)

            # remove processed lines from data
            data = data[next_empty:]
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "warn-munger",
            munger.name,
            f"unparsed lines at bottom of file ({Path(f_path).name}):\n{data}\n",
        )

    # consolidate all into one dataframe
    try:
        raw_results = pd.concat(list(df.values()))
    except ValueError as e:
        err = ui.add_new_error(
            err,
            "munger",
            munger.name,
            f"Error concatenating data from blocks: {e}",
        )
        return pd.DataFrame, err

    # Make row index (from first column of blocks) into a column called 'first_column'
    raw_results.reset_index(inplace=True)
    raw_results.rename(columns={0: "first_column"}, inplace=True)

    return raw_results, err


def read_multi_sheet_excel(
    f_path: str,
    munger: jm.Munger,
    err: dict,
) -> (pd.DataFrame, dict):
    # get munger parameters
    sheets_to_skip = munger.options["sheets_to_skip"]
    count_of_top_lines_to_skip = munger.options["count_of_top_lines_to_skip"]
    constant_line_count = munger.options["constant_line_count"]
    header_row_count = munger.options["header_row_count"]
    columns_to_skip = munger.options["columns_to_skip"]

    try:
        df = pd.read_excel(f_path,sheet_name=None,header=None)
    except Exception as e:
        new_err = ui.add_new_error(
            err,
            "file",
            Path(f_path).name,
            f"Error reading file: {e}"
        )
        if new_err:
            err = ui.consolidate_errors([err,new_err])
            if ui.fatal_error(new_err):
                return pd.DataFrame(), err

    sheets_to_read = [k for k in df.keys() if k not in sheets_to_skip]

    raw_results = pd.DataFrame()
    for sh in sheets_to_read:
        try:
            data = df[sh].copy()

            # remove lines designated ignorable
            data = data.iloc[count_of_top_lines_to_skip:]

            # remove any all-null rows
            data.dropna(how="all",inplace=True)

            # read constant info from first non-null entries of constant-header rows
            # then drop those rows
            constants = data.iloc[:constant_line_count].fillna(method="bfill", axis=1).iloc[:,0]
            data = data.iloc[constant_line_count:]

            # reset column headers to generic index (moving column names to rows
            # TODO remove data = data.transpose().reset_index().transpose()

            # add multi-index for actual header rows
            header_variable_names = [f"header_{j}" for j in range(header_row_count)]

            col_multi_index = pd.MultiIndex.from_frame(
                data.iloc[range(header_row_count),:].transpose().fillna(method="ffill"),
                names=header_variable_names,
            )
            data.columns = col_multi_index

            # remove header rows from data
            data = data.iloc[header_row_count:]

            # Drop extraneous columns per munger, and columns without data
            data.drop(data.columns[columns_to_skip], axis=1, inplace=True)
            data.dropna(axis=1, how="all", inplace=True)

            # make first column into an index
            data.set_index(keys=data.columns[0], inplace=True)

            # move header info to columns
            data = pd.melt(
                data,
                ignore_index=False,
                value_name="count",
                var_name=header_variable_names,
            )

            # add column(s) for constant info
            for j in range(constant_line_count):
                data = m.add_constant_column(data,f"constant_{j}",constants.iloc[j])

            # Make row index (from first column of blocks) into a column called 'first_column'
            data.reset_index(inplace=True)
            data.rename(columns={data.columns[0]: "first_column"}, inplace=True)

            raw_results = pd.concat([raw_results,data])
        except Exception as e:
            err = ui.add_new_error(
                err,
                "system",
                "special_formats.read_multi_sheet_excel",
                f"Unexpected exception while processing sheet {sh}: {e}"
            )
    return raw_results, err


def add_info(node: et.Element, info: dict, vc_field: dict) -> dict:
    for k in vc_field[node.tag].keys():
        info[k] = node.attrib[vc_field[node.tag][k]]
    return info


def read_xml(fpath: str, err: Optional[Dict]) -> (pd.DataFrame, Optional[Dict]):
    vc_field = {
        'jurisdiction': {'ReportingUnit': 'name'},
        'voteType': {'CountItemType': 'voteTypeName', 'Count': 'votes'},
        'contest': {'CandidateContest': 'contestLongName'},
        'choice': {'Candidate': 'choiceName'},
    }
    db_elements = {k2 for k, v in vc_field.items() for k2 in v.keys()}

    try:
        tree = et.parse(fpath)
    except FileNotFoundError:
        err = ui.add_new_error(
            err,
            "file",
            Path(fpath).name,
            "File not found"
        )
        return pd.DataFrame(), err

    try:
        vc_record_list = []
        info = dict()
        for node in tree.iter():  # depth-first search, what we want!
            if node.tag in vc_field.keys():
                if set(info.keys()) == db_elements:
                    # record should be complete: add it to the list
                    print(info)
                    vc_record_list.append(info)
                info = add_info(node, info, vc_field)
                raw_results = pd.DataFrame(vc_record_list)
                return raw_results, err
    except Exception as e:
        err = ui.add_new_error(
            err,
            "munger",
            f"Error munging xml: {e}"
        )
        return pd.DataFrame(), err
