import xml.etree.ElementTree as ET
from typing import Optional,Dict,Any,List,Union,Pattern
from urllib import request

import pandas as pd
from lxml import etree as lxml_et
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from elections import (
    database as db,
    userinterface as ui,
    constants,
)


def nist_v2_xml_export_tree(
    session: Session,
    election: str,
    jurisdiction: str,
    rollup: bool = False,
    major_subdivision: Optional[str] = None,
    sub_div_type_file: Optional[str] = None,
    issuer: str = constants.default_issuer,
    issuer_abbreviation: str = constants.default_issuer_abbreviation,
    status: str = constants.default_status,
    vendor_application_id: str = constants.default_vendor_application_id,
) -> (ET.ElementTree, Dict[str, Any]):
    """Creates a tree in the NIST common data format (V2) containing the results
    from the given election and jurisdiction. Note that all available results will
    be exported. I.e., if database has precinct-level results, the tree will
    contain precinct-level results.
    Major subdivision for rollup is <major_subdivision> if that's given;
    otherwise major subdivision is read from <sub_div_type_file> if given;
    otherwise pulled from db.
    """
    err = None
    # set up
    election_id = db.name_to_id(session, "Election", election)
    jurisdiction_id = db.name_to_id(session, "ReportingUnit", jurisdiction)
    if not election_id or not jurisdiction_id:
        err = ui.add_new_error(
            err,
            "database",
            session.bind.url.database,
            f"One or more of election {election} or jurisdiction {jurisdiction} not found in database"
        )
        tree = ET.ElementTree()
        return tree, err

    # include jurisdiction id in gp unit ids
    gpu_idxs = {jurisdiction_id}

    if rollup:
        # get major subdivision type if not provided
        if not major_subdivision:
            major_subdivision = db.get_major_subdiv_type(
                session, jurisdiction, file_path=sub_div_type_file
            )

    # get vote count data
    results_df = db.read_vote_count_nist(
        session, election_id, jurisdiction_id, rollup_ru_type=major_subdivision
    )

    # collect ids for gp units that have vote counts, gp units that are election districts
    gpu_idxs.update(results_df.ReportingUnit_Id.unique())
    gpu_idxs.update(results_df.ElectionDistrict_Id.unique())

    # ElectionReport (root)
    attr = {
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:schemaLocation":constants.nist_schema_location,
        "xmlns":constants.nist_namespace,
    }
    root = ET.Element("ElectionReport",attr)

    # add election sub-element of ElectionReport
    e_elt = ET.SubElement(root,"Election")

    # other sub-elements of ElectionReport
    ET.SubElement(root,"Format").text = "summary-contest"  # NB NIST restricts choices
    ET.SubElement(root,"GeneratedDate").text = datetime.now(tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    # get name, ru-type and composing info for all gpus
    rus = pd.read_sql("ReportingUnit", session.bind, index_col="Id")
    ru_types = pd.read_sql("ReportingUnitType", session.bind, index_col="Id")
    cruj = pd.read_sql("ComposingReportingUnitJoin", session.bind, index_col="Id")

    # add each gpu
    for idx in gpu_idxs:
        name = rus.loc[idx]["Name"]

        children = [
            f"oid{x}"
            for x in cruj[cruj["ParentReportingUnit_Id"] == idx][
                "ChildReportingUnit_Id"
            ].unique()
            if x in gpu_idxs and x != idx
        ]
        attr = {
            "ObjectId": f"oid{idx}",
            "xsi:type": "ReportingUnit",
        }
        gpu_elt = ET.SubElement(root,"GpUnit",attr)
        if children:
            children_elt = ET.SubElement(gpu_elt,"ComposingGpUnitIds")
            children_elt.text = " ".join(children)
        gpu_name = ET.SubElement(gpu_elt,"Name")
        ET.SubElement(gpu_name,"Text",{"Language":"en"}).text = name
        ET.SubElement(gpu_elt,"Type").text = ru_types.loc[
            rus.loc[idx]["ReportingUnitType_Id"]
        ]["Txt"]
        if ru_types.loc[rus.loc[idx]["ReportingUnitType_Id"]]["Txt"] == "other":
            ET.SubElement(gpu_elt,"OtherType").text = rus.loc[idx][
                "OtherReportingUnitType"
            ]

    # other sub-elements of ElectionReport
    ET.SubElement(root,"Issuer").text = issuer
    ET.SubElement(root,"IssuerAbbreviation").text = issuer_abbreviation

    # add each party
    party_df = results_df[["Party_Id", "PartyName"]].drop_duplicates()
    for i, p in party_df.iterrows():
        p_elt = ET.SubElement(
            root,
            "Party",
            {
                "ObjectId": f'oid{p["Party_Id"]}',
            },
        )
        p_name_elt = ET.SubElement(p_elt,"Name")
        ET.SubElement(p_name_elt,"Text",{"Language":"en"}).text = p["PartyName"]

    # still more sub-elements of ElectionReport
    ET.SubElement(root,"SequenceStart").text = "1"  # TODO placeholder
    ET.SubElement(root,"SequenceEnd").text = "1"  # TODO placeholder
    ET.SubElement(root,"Status").text = status
    ET.SubElement(root,"VendorApplicationId").text = vendor_application_id

    # add each candidate (as sub-element of Election)
    candidate_df = results_df[
        ["Candidate_Id", "BallotName", "Party_Id"]
    ].drop_duplicates()
    for i, can in candidate_df.iterrows():
        can_elt = ET.SubElement(
            e_elt, "Candidate", {"ObjectId": f'oid{can["Candidate_Id"]}'}
        )
        bn_elt = ET.SubElement(can_elt,"BallotName")
        ET.SubElement(bn_elt,"Text",{"Language":"en"}).text = can["BallotName"]
        party_id_elt = ET.SubElement(can_elt,"PartyId")
        party_id_elt.text = f'oid{can["Party_Id"]}'

    # add each contest (as sub-element of Election)
    contest_df = results_df[
        ["Contest_Id", "ContestName", "ContestType", "ElectionDistrict_Id"]
    ].drop_duplicates()
    for i, con in contest_df.iterrows():
        # create element for the contest
        attr = {
            "ObjectId": f'oid{con["Contest_Id"]}',
            "xsi:type": f'{con["ContestType"]}Contest',
        }
        con_elt = ET.SubElement(e_elt,"Contest",attr)

        # create ballot selection sub-elements
        # TODO (remove assumption that it's a  CandidateContest)
        selection_idxs = results_df[results_df.Contest_Id == con["Contest_Id"]][
            "Selection_Id"
        ].unique()
        for s_idx in selection_idxs:
            attr = {
                "ObjectId": f"oid{s_idx}",
                "xsi:type": "CandidateSelection",
            }
            cs_elt = ET.SubElement(con_elt,"ContestSelection",attr)
            vc_df = results_df[
                (results_df.Contest_Id == con["Contest_Id"])
                & (results_df.Selection_Id == s_idx)
            ][
                [
                    "ReportingUnit_Id",
                    "Candidate_Id",
                    "CountItemType",
                    "OtherCountItemType",
                    "Count",
                ]
            ].drop_duplicates()
            for idx, vc in vc_df.iterrows():
                vote_counts_elt = ET.SubElement(cs_elt,"VoteCounts")
                # create GpUnitId sub-element
                ET.SubElement(
                    vote_counts_elt, "GpUnitId"
                ).text = f'oid{vc["ReportingUnit_Id"]}'
                # create Type sub-elements (for CountItemType)
                ET.SubElement(vote_counts_elt,"Type").text = vc["CountItemType"]
                if vc["CountItemType"] == "other":
                    ET.SubElement(vote_counts_elt,"OtherType").text = vc[
                        "OtherCountItemType"
                    ]
                # create Count sub-element
                ET.SubElement(vote_counts_elt,"Count").text = str(vc["Count"])

            candidate_ids = " ".join([f"oid{x}" for x in vc_df.Candidate_Id.unique()])
            ET.SubElement(cs_elt,"CandidateIds").text = candidate_ids

        # create ElectionDistrictId sub-element
        ET.SubElement(
            con_elt, "ElectionDistrictId"
        ).text = f'oid{con["ElectionDistrict_Id"]}'

        # create Name sub-element
        ET.SubElement(con_elt,"Name").text = con["ContestName"]

        # create VotesAllowed sub-element
        ET.SubElement(con_elt,"VotesAllowed").text = "1"
        # TODO tech debt allow arbitrary "votes allowed

    # election scope (geographic unit for whole election)
    ET.SubElement(e_elt,"ElectionScopeId").text = f"oid{jurisdiction_id}"

    # add properties of particular election
    # NB we assume only one election!
    election_df = pd.read_sql_table("Election", session.bind, index_col="Id")
    election_type_df = pd.read_sql_table("ElectionType", session.bind, index_col="Id")
    election_name_elt = ET.SubElement(e_elt,"Name")
    ET.SubElement(election_name_elt,"Text",{"Language":"en"}).text = election_df.loc[
        election_id
    ]["Name"]
    ET.SubElement(e_elt,"StartDate").text = "1900-01-01"  # placeholder
    ET.SubElement(e_elt,"EndDate").text = "1900-01-01"  # placeholder

    # election type
    e_type = election_type_df.loc[election_df.loc[election_id]["ElectionType_Id"]][
        "Txt"
    ]
    ET.SubElement(e_elt,"Type").text = e_type
    if e_type == "other":
        ET.SubElement(e_elt,"OtherType").text = election_df.loc[election_id][
            "OtherElectionType"
        ]

    tree = ET.ElementTree(root)
    return tree, err


# constants
# NB: if nist schema were out of sync with internal db schema, this would be non-trivial


def tree_parse_info(xpath: str,ns: Optional[str]) -> Dict[str,Any]:
    """extracts xml path-parsing info from xpath with
    optional .* attribute. E.g., Election/Contest.name
    gets parsed to path Election/Contest and attribute
    name. If namespace (e.g., 'html') is given, then namespace is
    prepended as appropriate in path (e.g. {html}Election/{html}Contest)"""
    if "." in xpath:
        [no_ns_path, attrib] = xpath.split(".")
    else:
        no_ns_path = xpath
        attrib = None
    components = no_ns_path.split("/")
    if ns:
        ns = f"{{{ns}}}"
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
        ns = None
    else:
        ns = p["namespace"]
    d = tree_parse_info(p["count_location"], ns)
    parse_info = {"main_path": d["path"], "main_attrib": d["attrib"]}
    return parse_info


def xml_string_path_info(
    munge_fields: List[str],
    ns: Optional[str],
) -> Dict[str, Dict[str, Dict[str, str]]]:
    """For each munge string, extracts info for traversing tree"""
    info_dict = dict()
    for field in munge_fields:
        info_dict[field] = tree_parse_info(field,ns)
    return info_dict


def df_from_tree(
    tree: lxml_et.ElementTree,
    main_path: str,
    main_attrib: Optional[str],
    xml_path_info: Dict[str, Dict[str, Dict[str, str]]],
    file_name: str,
    ns: Optional[str],
    lookup_id: str = None,
) -> (pd.DataFrame, Optional[dict]):
    """Reads all counts (or lookup_ids, if given), along with info from munge string paths
    ((tag, attr) for each element), into a dataframe.
    If main_attrib is None, reads from text value of element; otherwise from attribute."""
    # create parent lookup
    parent = {c: p for p in tree.iter() for c in p}
    if ns:
        ns = f"{{{ns}}}"
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
        return pd.DataFrame, err
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


def check_nist_namespace(f_path,key) -> Optional[dict]:
    """get the namespaces in the XML and return error if the one we're expecting
    is not found"""
    namespaces = dict(
        [node for _, node in lxml_et.iterparse(f_path, events=["start-ns"])]
    )
    try:
        ns = namespaces[key]
        return ns
    except KeyError:
        return None


def read_data_from_url(
    url: str,
    local_html_file: str,
    local_excel_file: str,
    match: Union[str, Pattern] = "",
):
    """Copies html to a local file.  Pulls all tables from html into
    separate sheets of an excel file. If match is given,
    pulls into Excel only the tables containing that string/pattern"""
    page = request.urlopen(url).read()
    with open(local_html_file, "wb") as f:
        f.write(page)

    df_list = pd.read_html(local_html_file, match=match)

    # write to Excel
    with pd.ExcelWriter(local_excel_file) as ew:
        num = 1
        for df in df_list:
            df.to_excel(ew, sheet_name=f"Sheet{num}", index=None)
            num += 1
    return
