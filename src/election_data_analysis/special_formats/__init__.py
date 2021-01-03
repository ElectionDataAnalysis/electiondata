import io
import json
import pandas as pd
import traceback
import xml.etree.ElementTree as et
from copy import deepcopy
from pathlib import Path
from typing import Optional, Dict, List, Any
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


def read_xml(
    f_path: str,
    p: Dict[str, Any],
    munger_name: str,
    err: Optional[Dict],
) -> (pd.DataFrame, Optional[Dict]):
    """Create dataframe from the xml file, with column names matching the fields in the raw_identifier formulas.
    Skip nodes whose tags are unrecognized"""

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
    attributes = {t: [x.split(".")[1] for x in fields if x.split(".")[0] == t] for t in tags}

    try:
        root = tree.getroot()
        results_list = results_below(root, tags, attributes)
        raw_results = pd.DataFrame(results_list)
        if raw_results.empty:
            err = ui.add_new_error(err, "file", Path(f_path).name, "No results read from file")
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


def results_below(node: et.Element, good_tags: set, good_pairs: dict) -> list:
    """appends all (possibly incomplete) results records that can be
    read from nodes below to the list self.results"""
    r_below = []

    if node.getchildren() == list():
        r_below = [{f"{node.tag}.{k}": node.attrib.get(k, "") for k in good_pairs[node.tag]}]
    else:
        for child in node:
            if child.tag in good_tags:
                # get list of all (possibly incomplete) results records from below the child
                r_below_child = results_below(child, good_tags, good_pairs)
                # add info from the current node to each record and append result to self.results
                for result in r_below_child:
                    if node.tag in good_tags:
                        result.update(
                            {f"{node.tag}.{k}": node.attrib.get(k, "") for k in good_pairs[node.tag]}
                        )
                    r_below.append(result)
    return r_below


def read_nested_json(f_path: str,
                     p: Dict[str, Any],
                     munger_name: str,
                     err: Optional[Dict]) -> (pd.DataFrame, Optional[Dict]):
    """
    Create dataframe from a nested json file, by traversing the json dictionary
    recursively, similar to the case of xml.
    """

    # read data from file
    try:
        with open(f_path, 'r') as f:
            j = json.load(f)
    except FileNotFoundError:
        traceback.print_exc()
        err = ui.add_new_error(err, "file", Path(f_path).name, "File not found")
        return pd.DataFrame(), err

    # Identify keys for counts and other raw data (attributes) we want

    # The last value is the final key
    count_keys = {k.split('.')[-1] for k in p["count_fields_by_name"]}
    attribute_keys = {k.split('.')[-1] for k in p["munge_fields"]["in_field_values"]}

    # Any prior values are nested keys
    nested_keys = set()
    for k in set(p["count_fields_by_name"]) | set(p["munge_fields"]["in_field_values"]):
        nested_keys |= set(k.split('.')[:-1])

    try:
        current_values = {}
        current_nested_keys = []
        results_list = json_results_below(j,
                                          count_keys,
                                          attribute_keys,
                                          nested_keys,
                                          current_values,
                                          current_nested_keys)
        raw_results = pd.DataFrame(results_list)

        # Only keep columns that we want, so the other ones don't cause trouble later.
        cols_we_want = list(p["count_fields_by_name"]) + list(p["munge_fields"]["in_field_values"])
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


def json_results_below(j: dict or list,
                       count_keys: set,
                       attribute_keys: set,
                       nested_keys: set,
                       current_values: dict,
                       current_nested_keys: list) -> list:
    """
    Traverse entire json, keeping info for attribute_key's, and returning
    rows when a count_key is reached.
    """

    # The json can be either a dict or a list.
    if isinstance(j, list):
        results = []
        for v in j:
            results += json_results_below(v,
                                          count_keys,
                                          attribute_keys,
                                          nested_keys,
                                          current_values,
                                          current_nested_keys)
        return results

    else: # json is dict

        # Update values at current level
        for k, v in j.items():
            if k in attribute_keys | count_keys:
                current_values['.'.join(current_nested_keys + [k])] = v

        # Recursively update values
        results = []
        for k, v in j.items():
            if k in nested_keys:
                child_nested_keys = current_nested_keys + [k]
            else:
                child_nested_keys = current_nested_keys[:]
            if isinstance(v, dict) or isinstance(v, list):
                results += json_results_below(v,
                                              count_keys,
                                              attribute_keys,
                                              nested_keys,
                                              current_values,
                                              child_nested_keys)

        # Return current_values if we've reached the counts,
        # since each count value can only occur in one row.
        for k in j.keys():
            if k in count_keys:
                return [deepcopy(current_values)]

        # Otherwise, return the results below current node
        return results
