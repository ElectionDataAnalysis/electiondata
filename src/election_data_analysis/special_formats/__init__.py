import json
import pandas as pd
import traceback
import xml.etree.ElementTree as et
from copy import deepcopy
from pathlib import Path
from typing import Optional, Dict, List, Any
from election_data_analysis import munge as m
from election_data_analysis import user_interface as ui


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


def read_xml(
    f_path: str,
    p: Dict[str, Any],
    munger_name: str,
    err: Optional[Dict],
) -> (pd.DataFrame, Optional[Dict]):
    """Create dataframe from the xml file, with column names matching the fields in the raw_identifier formulas.
    Skip nodes whose tags are unrecognized"""

    namespace = None  # for syntax checker
    # read data from file
    try:
        tree = et.parse(f_path)
    except FileNotFoundError:
        err = ui.add_new_error(err, "file", Path(f_path).name, "File not found")
        return pd.DataFrame(), err

    # identify tags with counts or other raw data (the info we want)
    # and list data to be pulled from each tag
    fields = set(p["count_fields_by_name"]).union(p["munge_fields"]["in_field_values"])
    tags = {f.split(".")[0] for f in fields}
    tags.update(p["nesting_tags"])
    attributes = {
        t: [x.split(".")[1] for x in fields if x.split(".")[0] == t] for t in tags
    }

    try:
        root = tree.getroot()
        if munger_name == "nist_xml.munger":
            namespace = nist_namespace(f_path, "")
            tags, attributes = nist_tags(tags, attributes, namespace)
        results_list = results_below(root, tags, attributes)
        raw_results = pd.DataFrame(results_list)
        if raw_results.empty:
            err = ui.add_new_error(
                err, "file", Path(f_path).name, "No results read from file"
            )
        if munger_name == "nist_xml.munger":
            raw_results = clean_nist_columns(raw_results, namespace)
            raw_results = replace_id_values(raw_results, f_path)
        # TODO tech debt is the for loop necessary before clean_count_cols?
        for c in p["count_fields_by_name"]:
            raw_results[c] = pd.to_numeric(raw_results[c], errors="coerce")
        raw_results, err_df = m.clean_count_cols(
            raw_results,
            p["count_fields_by_name"],
        )
        if not err_df.empty:
            err = ui.add_err_df(err, err_df, munger_name, f_path)
    except Exception as exc:
        err = ui.add_new_error(err, "munger", munger_name, f"Error reading xml: {exc}")
        raw_results = pd.DataFrame()
    return raw_results, err


def nist_tags(good_tags, good_pairs, namespace):
    """ NIST file requires some finessing of values for the parsing """
    good_tags.add("Election")
    good_tags.add("ContestCollection")
    good_tags.add("BallotSelection")
    good_tags.add("VoteCountsCollection")
    good_tags.add("BallotName")

    new_tags = set()
    new_pairs = {}
    for tag in good_tags:
        new_tags.add(f"{{{namespace}}}{tag}")

    for key in good_pairs:
        new_pairs[f"{{{namespace}}}{key}"] = good_pairs[key]

    return new_tags, new_pairs


def results_below(node: et.Element, good_tags: set, good_pairs: dict) -> list:
    """appends all (possibly incomplete) results records that can be
    read from nodes below to the list self.results"""
    r_below = []

    if list(node) == list():
        r_below = {}
        for k in good_pairs[node.tag]:
            if k == "text":
                r_below[f"{node.tag}.{k}"] = node.text
            else:
                r_below[f"{node.tag}.{k}"] = node.attrib.get(k, "")
        r_below = [r_below]
    else:
        for child in node:
            r_below_child = []
            if child.tag in good_tags:
                # get list of all (possibly incomplete) results records from below the child
                r_below_child = r_below_child + results_below(
                    child, good_tags, good_pairs
                )
                for result in r_below_child:
                    if node.tag in good_pairs.keys():
                        result.update(
                            {
                                f"{node.tag}.{k}": node.attrib.get(k, "")
                                for k in good_pairs[node.tag]
                            }
                        )
                    # handle special cases for NIST here
                    if len(result.keys()) == 1:
                        for r in r_below:
                            if list(result.keys())[0] not in list(r.keys()):
                                r.update(result)
                    else:
                        r_below.append(result)
    return r_below


def read_nested_json(
    f_path: str, p: Dict[str, Any], munger_name: str, err: Optional[Dict]
) -> (pd.DataFrame, Optional[Dict]):
    """
    Create dataframe from a nested json file, by traversing the json dictionary
    recursively, similar to the case of xml.
    """

    # read data from file
    try:
        with open(f_path, "r") as f:
            j = json.load(f)
    except FileNotFoundError:
        traceback.print_exc()
        err = ui.add_new_error(err, "file", Path(f_path).name, "File not found")
        return pd.DataFrame(), err

    # Identify keys for counts and other raw data (attributes) we want

    # The last value is the final key
    count_keys = {k.split(".")[-1] for k in p["count_fields_by_name"]}
    attribute_keys = {k.split(".")[-1] for k in p["munge_fields"]["in_field_values"]}

    # Any prior values are nested keys
    nested_keys = set()
    for k in set(p["count_fields_by_name"]) | set(p["munge_fields"]["in_field_values"]):
        nested_keys |= set(k.split(".")[:-1])

    try:
        current_values = {}
        current_nested_keys = []
        results_list = json_results_below(
            j,
            count_keys,
            attribute_keys,
            nested_keys,
            current_values,
            current_nested_keys,
        )
        raw_results = pd.DataFrame(results_list)

        # Only keep columns that we want, so the other ones don't cause trouble later.
        cols_we_want = list(p["count_fields_by_name"]) + list(
            p["munge_fields"]["in_field_values"]
        )
        raw_results = raw_results[cols_we_want]

        # Perform standard cleaning
        # TODO tech debt: for loop probably unnecessary before clean_count_cols
        for c in p["count_fields_by_name"]:
            raw_results[c] = pd.to_numeric(raw_results[c], errors="coerce")
        raw_results, err_df = m.clean_count_cols(
            raw_results,
            p["count_fields_by_name"],
        )
        if not err_df.empty:
            err = ui.add_err_df(err, err_df, munger_name, f_path)
    except Exception as e:
        traceback.print_exc()
        err = ui.add_new_error(err, "munger", munger_name, f"Error reading xml: {e}")
        raw_results = pd.DataFrame()
    return raw_results, err


def json_results_below(
    j: dict or list,
    count_keys: set,
    attribute_keys: set,
    nested_keys: set,
    current_values: dict,
    current_nested_keys: list,
) -> list:
    """
    Traverse entire json, keeping info for attribute_key's, and returning
    rows when a count_key is reached.
    """

    # The json can be either a dict or a list.
    if isinstance(j, list):
        results = []
        for v in j:
            results += json_results_below(
                v,
                count_keys,
                attribute_keys,
                nested_keys,
                current_values,
                current_nested_keys,
            )
        return results

    else:  # json is dict

        # Update values at current level
        for k, v in j.items():
            if k in attribute_keys | count_keys:
                current_values[".".join(current_nested_keys + [k])] = v

        # Recursively update values
        results = []
        for k, v in j.items():
            if k in nested_keys:
                child_nested_keys = current_nested_keys + [k]
            else:
                child_nested_keys = current_nested_keys[:]
            if isinstance(v, dict) or isinstance(v, list):
                results += json_results_below(
                    v,
                    count_keys,
                    attribute_keys,
                    nested_keys,
                    current_values,
                    child_nested_keys,
                )

        # Return current_values if we've reached the counts,
        # since each count value can only occur in one row.
        for k in j.keys():
            if k in count_keys:
                return [deepcopy(current_values)]

        # Otherwise, return the results below current node
        return results


def nist_lookup(f_path: str) -> (pd.DataFrame, pd.DataFrame):
    """The NIST format stores data about each entity separately from where
    they may be referenced. For example, a ReportingUnit ID and Name may be
    defined in one section, and then the ID will be referenced in the VoteCounts.
    In order to match the ReportingUnit to particular VoteCounts, we parse the
    XML and build dataframes tha we can reference."""

    # when this function is called, it's already been accessed. So we omit
    # the normal error checking here because we can assume it exists.
    tree = et.parse(f_path)
    root = tree.getroot()

    # this namespace for reportingUnit info
    namespace_xsi = nist_namespace(f_path, "xsi")
    # this namespace for others
    namespace_blank = nist_namespace(f_path, "")

    candidates = []
    reporting_units = []
    parties = []

    # loop through the XML
    for child in root.iter():
        # get candidate info
        if child.tag == f"{{{namespace_blank}}}CandidateCollection":
            for grandchild in child.iter():
                if grandchild.tag == f"{{{namespace_blank}}}Candidate":
                    candidate = {"ObjectId": grandchild.attrib.get("ObjectId")}
                    for g in grandchild:
                        if g.tag == f"{{{namespace_blank}}}BallotName":
                            for h in g:
                                candidate["BallotName"] = h.text
                        elif g.tag == f"{{{namespace_blank}}}PartyId":
                            candidate["PartyId"] = g.text
                    candidates.append(candidate)
        # get reportingUnit info
        if (
            child.tag == f"{{{namespace_blank}}}GpUnit"
            and "ReportingUnit" == child.attrib.get(f"{{{namespace_xsi}}}type")
        ):
            reporting_units.append(
                {
                    "ObjectId": child.attrib.get("ObjectId"),
                    "Name": child.attrib.get("Name"),
                }
            )
        # get party info
        if child.tag == f"{{{namespace_blank}}}Party":
            parties.append(
                {
                    "ObjectId": child.attrib.get("ObjectId"),
                    "PartyName": child.attrib.get("Abbreviation"),
                }
            )

    # prep dataframes
    reporting_unit_df = pd.DataFrame(reporting_units)
    reporting_unit_df = reporting_unit_df[reporting_unit_df["Name"] != "Statewide"]

    candidate_df = pd.DataFrame(candidates)
    party_df = pd.DataFrame(parties)
    candidate_df = candidate_df.merge(
        party_df, left_on="PartyId", right_on="ObjectId", suffixes=(None, "_y")
    )[["ObjectId", "BallotName", "PartyName"]]

    return candidate_df, reporting_unit_df


def nist_namespace(f_path, key) -> Optional[dict]:
    """get the namespaces in the XML and return error if the one we're expecting
    is not found"""
    namespaces = dict([node for _, node in et.iterparse(f_path, events=["start-ns"])])
    try:
        namespace = namespaces[key]
        return namespace
    except Exception:
        return None


def clean_nist_columns(df, namespace):
    col = df.columns
    new_headers = []
    for i in range(len(col)):
        new_headers.append(col[i].replace(f"{{{namespace}}}", ""))
    df.columns = new_headers
    return df


def replace_id_values(df, f_path):
    candidate_df, reporting_unit_df = nist_lookup(f_path)

    # Add reporting unit info
    df = df.merge(reporting_unit_df, left_on="GpUnitId.text", right_on="ObjectId")
    df = df[
        [
            "VoteCounts.Type",
            "VoteCounts.Count",
            "CandidateId.text",
            "Contest.Name",
            "Name",
        ]
    ].rename(columns={"Name": "GpUnitId.text"})

    # add candidate, party info
    df = df.merge(candidate_df, left_on="CandidateId.text", right_on="ObjectId")
    df = df[
        [
            "VoteCounts.Type",
            "VoteCounts.Count",
            "BallotName",
            "Contest.Name",
            "PartyName",
            "GpUnitId.text",
        ]
    ].rename(
        columns={
            "BallotName": "CandidateId.text",
            "PartyName": "PartyId.text",
        }
    )
    return df
