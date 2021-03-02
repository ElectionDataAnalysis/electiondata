import xml.etree.ElementTree as et
import pandas as pd
from datetime import datetime
from election_data_analysis import (
    analyze as an,
    database as db,
)

def nist_xml_export(session, election, jurisdiction):
    election_id = db.name_to_id(session, "Election", election)
    jurisdiction_id = db.name_to_id(session, "ReportingUnit", jurisdiction)

    # ElectionReport (root)
    attr = {
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:schemaLocation": "https://github.com/usnistgov/ElectionResultsReporting/raw/version2/NIST_V2_election_results_reporting.xsd",
        "xmlns": "http://itl.nist.gov/ns/voting/1500-100/v2",
    }
    root = et.Element("ElectionReport", attr)

    # election
    elections = an.nist_election(session, election_id, jurisdiction_id)
    ## there's only one election, but it comes back as a single element of a list
    for e in elections:
        attr = dict()
        e_elt = et.SubElement(root, "Election", attr)

        # offices
        offices = an.nist_office(session, election_id, jurisdiction_id)
        # track election districts for each contest (will need to list each as a gp unit)
        gpu_ids = set()
        for off in offices:
            gpu_ids.add(off["ElectoralDistrictId"])

    # other sub-elements of ElectionReport
    et.SubElement(root, "Format").text = "summary-contest"
    et.SubElement(root, "GeneratedDate").text = "1900-01-01T00:00:00Z"  # TODO placeholder, needs to be fixed

    # contests
    contests = an.nist_candidate_contest(session, election_id, jurisdiction_id)
    for con in contests:
        # create element for the contest
        attr = {
            "ObjectId": f'oid{con["Id"]}',
            "xsi:type": con["Type"],
        }
        con_elt = et.SubElement(e_elt, "Contest", attr)
        # create ballot title sub-element
        bt_elt = et.SubElement(con_elt, "BallotTitle")
        attr = {"Language": "en"}
        et.SubElement(bt_elt, "Text", attr).text = con["ContestName"]
        # create ballot selection sub-elements
        for s_dict in con["BallotSelection"]:
            attr = {
                "ObjectId": f'oid{s_dict["Id"]}',
                "xsi:type": "CandidateSelection",
            }
            cs_elt = et.SubElement(con_elt, "ContestSelection", attr)
            c_elt = et.SubElement(cs_elt, "VoteCounts")
            for vc_dict in s_dict["VoteCounts"]:
                vc_elt = et.SubElement(c_elt, "Count")
                et.SubElement(vc_elt, "CountItemType").text = vc_dict["CountItemType"]
                et.SubElement(vc_elt, "GpUnitId").text = f'oid{vc_dict["GpUnitId"]}'
                et.SubElement(vc_elt, "Count").text = str(vc_dict["Count"])

            et.SubElement(cs_elt, "CandidateIds").text = f'oid{s_dict["CandidateId"]}'
            # TODO tech debt ^^ assumes single candidate


        # create ElectionDistrictId sub-element
        et.SubElement(con_elt, "ElectionDistrictId").text = f'oid{con["ElectionDistrictId"]}'


    # get ids for gpunits that have vote counts
    vc_gpus = an.nist_reporting_unit(session, election_id, jurisdiction_id)
    gpu_ids.update({gpu["Id"] for gpu in vc_gpus})

    # get name, ru-type and composing info for all gpus
    rus = pd.read_sql("ReportingUnit", session.bind, index_col="Id")
    ru_types = pd.read_sql("ReportingUnitType", session.bind, index_col="Id")
    cruj = pd.read_sql("ComposingReportingUnitJoin", session.bind, index_col="Id")

    # relevant = rus.index.isin(gpu_ids)
    for idx in gpu_ids:
        name = rus.loc[idx]["Name"]
        rut = rus.loc[idx]["OtherReportingUnitType"]
        if rut == "":   # if it's a standard type
            rut = ru_types.loc[rus.loc[idx]["ReportingUnitType_Id"]]["Txt"]
        assert rut != "other", f"ReportingUnit with index {idx} has type other"
        children = [
            f'oid{x}' for x in
            cruj[cruj["ParentReportingUnit_Id"] == idx]["ChildReportingUnit_Id"].unique()
            if x in gpu_ids and x != idx
        ]
        attr = {
            "ObjectId": f'oid{idx}',
            "Name": name,
            "Type": rut,
        }
        gpu_elt = et.SubElement(e_elt, "GpUnit", attr)
        if children:
            children_elt = et.SubElement(gpu_elt, "ComposingGpUnitIds")
            children_elt.text = " ".join(children)

    # parties
    parties = an.nist_party(session, election_id, jurisdiction_id)
    for p in parties:
        attr = {
            "ObjectId": f'oid{p["Id"]}',
            "Name": p["Name"],
        }
        p_elt = et.SubElement(e_elt, "Party", attr)

    # candidates
    candidates = an.nist_candidate(session, election_id, jurisdiction_id)
    for can in candidates:
        attr = {
            "ObjectId": f'oid{can["Id"]}',
            "BallotName": can["BallotName"],
        }
        can_elt = et.SubElement(e_elt, "Candidate", attr)
        party_id_elt = et.SubElement(can_elt, "PartyId")
        party_id_elt.text = f'oid{can["PartyId"]}'

    tree = et.ElementTree(root)
    return tree

