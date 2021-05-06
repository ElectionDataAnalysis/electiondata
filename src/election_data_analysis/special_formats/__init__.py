from typing import Optional, Dict, List, Any

import lxml.etree as lxml_et
import pandas as pd

from election_data_analysis import user_interface as ui

# constants
# NB: if nist schema were out of sync with internal db schema, this would be non-trivial
cit_list = [
    "absentee",
    "absentee-fwab",
    "absentee-in-person",
    "absentee-mail",
    "early",
    "election-day",
    "provisional",
    "seats",
    "total",
    "uocava",
    "write-in",
]
cit_from_raw_nist_df = pd.DataFrame(
    [["CountItemType", x, x] for x in cit_list],
    columns=["cdf_element", "cdf_internal_name", "raw_identifier_value"],
)


def tree_parse_info(xpath: str, namespace: Optional[str]) -> Dict[str, Any]:
    """extracts xml path-parsing info from xpath with
    optional .* attribute. E.g., Election/Contest.name
    gets parsed to path Election/Contest and attribute
    name. If namespace (e.g., 'html') is given, then namespase is
    prepended as appropriate in path (e.g. {html}Election/{html}Contest)"""
    if "." in xpath:
        [no_ns_path, attrib] = xpath.split(".")
    else:
        no_ns_path = xpath
        attrib = None
    components = no_ns_path.split("/")
    if namespace:
        ns = f"{{{namespace}}}"
    else:
        ns = ""
    ns_components = [f"{ns}{s}" for s in components]
    path = "/".join(ns_components)
    tag = ns_components[-1]
    head = ns_components[0]
    tail = "/".join(ns_components[1:])
    local_root_tag = ns_components[0]

    d = {
        "path": path,
        "tag": tag,
        "attrib": attrib,
        "local_root_tag": local_root_tag,
        "head": head,
        "tail": tail,
    }
    return d


def xml_count_parse_info(
    p: Dict[str, Any], ignore_namespace: bool = False
) -> Dict[str, Any]:
    """Extracts parsing info from munger parameters into dictionary
    {'count_path': ..., 'count_attrib': ...}"""
    if ignore_namespace:
        namespace = None
    else:
        namespace = p["namespace"]
    d = tree_parse_info(p["count_location"], namespace)
    parse_info = {"main_path": d["path"], "main_attrib": d["attrib"]}
    return parse_info


def xml_string_path_info(
    munge_fields: List[str],
    namespace: Optional[str],
) -> Dict[str, Dict[str, Dict[str, str]]]:
    """For each munge string, extracts info for traversing tree"""
    info_dict = dict()
    for field in munge_fields:
        info_dict[field] = tree_parse_info(field, namespace)
    return info_dict


def df_from_tree(
    tree: lxml_et.ElementTree,
    main_path: str,
    main_attrib: Optional[str],
    xml_path_info: Dict[str, Dict[str, Dict[str, str]]],
    file_name: str,
    namespace: Optional[str],
    lookup_id: str = None,
) -> (pd.DataFrame, Optional[dict]):
    """Reads all counts (or lookup_ids, if given), along with info from munge string paths ((tag, attr) for each element), into a dataframe.
    If main_attrib is None, reads from text value of element; otherwise from attribute."""
    # create parent lookup
    parent = {c: p for p in tree.iter() for c in p}
    if namespace:
        ns = f"{{{namespace}}}"
    else:
        ns = ""
    with_ns = [f"{ns}{s}" for s in main_path.split("/")]
    head = with_ns[0]
    tail = "/".join(with_ns[1:])
    root = tree.getroot()
    if root.tag == head:
        err = None
    else:
        err = ui.add_new_error(
            None,
            "file",
            file_name,
            f"Root element of file is not {head}, as expected per munger",
        )
        return (pd.DataFrame, err)
    df_rows = list()
    for driver in root.findall(tail):
        if lookup_id:
            if main_attrib:
                row = {lookup_id: driver.attrib[main_attrib]}
            else:
                row = {lookup_id: driver.text}
        else:
            if main_attrib:
                row = {"Count": int(driver.attrib[main_attrib])}
            else:
                row = {"Count": int(driver.text)}

        ancestor = driver
        while ancestor is not None:
            for field in xml_path_info.keys():
                if xml_path_info[field]["local_root_tag"] == ancestor.tag:
                    if xml_path_info[field]["attrib"]:
                        try:
                            row[field] = ancestor.attrib[xml_path_info[field]["attrib"]]
                        except KeyError:
                            pass
                    else:
                        row[field] = ancestor.find(xml_path_info[field]["tail"]).text
            if ancestor in parent.keys():
                ancestor = parent[ancestor]
            else:
                ancestor = None
        df_rows.append(row)
    df = pd.DataFrame(df_rows)
    return df, err


def nist_namespace(f_path, key) -> Optional[dict]:
    """get the namespaces in the XML and return error if the one we're expecting
    is not found"""
    namespaces = dict(
        [node for _, node in lxml_et.iterparse(f_path, events=["start-ns"])]
    )
    try:
        namespace = namespaces[key]
        return namespace
    except Exception:
        return None
