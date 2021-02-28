import xml.etree.ElementTree as et
import pandas as pd
from election_data_analysis import (
    analyze as an,
    database as db,
)

def nist_xml_export(session, election, jurisdiction):
    election_id = db.name_to_id(session, "Election", election)
    jurisdiction_id = db.name_to_id(session, "ReportingUnit", jurisdiction)

    root = et.Element("ElectionReport")
    tree = et.ElementTree(root)

    # election
    elections = an.nist_election(session, election_id, jurisdiction_id)
    for e in elections:
        attr = {
            "objectId": str(e["Id"]),
            "Date": "1900-01-01",    # TODO placeholder, needs to be fixed
            "Name": e["Name"],
            "Type": e["Type"],
        }
        e_elt = et.SubElement(root, "Election", attr)

    # offices
    offices = an.nist_office(session, election_id, jurisdiction_id)
    # track election districts for each contest (will need to list each as a gp unit)
    gpu_ids = set()
    for off in offices:
        gpu_ids.add(off["ElectoralDistrictId"])
        attr = {
            "objectId": str(off["Id"]),
            "Name": off["Name"],
            # "ElectoralDistrictId": str(off["ElectoralDistrictId"]),
        }
        off_elt = et.SubElement(root, "Office", attr)
        ed_id = et.SubElement(off_elt, "ElectoralDistrictId")
        ed_id.text = str(off["ElectoralDistrictId"])

    # contests
    contests = an.nist_candidate_contest(session, election_id, jurisdiction_id)
    for con in contests:

        attr = {
            "objectId": str(con['Id']),
            "Type": con["Type"],
            "ContestName": con["ContestName"],
        }
        con_elt = et.SubElement(root, "Contest", attr)
        off_id_elt = et.SubElement(con_elt, "OfficeId")
        off_id_elt.text = str(con["OfficeId"])
        for s_dict in con["BallotSelection"]:
            attr = {
                "objectId": str(s_dict["Id"]),
                "Type": "CandidateSelection",
            }
            s_elt = et.SubElement(con_elt, "BallotSelection", attr)
            can_id_elt = et.SubElement(s_elt, "CandidateId")
            can_id_elt.text = str(s_dict["CandidateId"])
            vcs_elt = et.SubElement(s_elt, "VoteCountsCollection", dict())
            for vc_dict in s_dict["VoteCounts"]:
                vc_elt = et.SubElement(vcs_elt, "VoteCounts")
                gpu_id_elt = et.SubElement(vc_elt, "CountItemType")
                gpu_id_elt.text = vc_dict["CountItemType"]
                gpu_id_elt = et.SubElement(vc_elt, "GpUnitId")
                gpu_id_elt.text = str(vc_dict["GpUnitId"])
                gpu_id_elt = et.SubElement(vc_elt, "Count")
                gpu_id_elt.text = str(vc_dict["Count"])

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
            str(x) for x in
            cruj[cruj["ParentReportingUnit_Id"] == idx]["ChildReportingUnit_Id"].unique()
            if x in gpu_ids and x != idx
        ]
        attr = {
            "objectId": str(idx),
            "Name": name,
            "Type": rut,
        }
        gpu_elt = et.SubElement(root, "GpUnit", attr)
        if children:
            children_elt = et.SubElement(gpu_elt, "ComposingGpUnitIds")
            children_elt.text = " ".join(children)

    # parties
    parties = an.nist_party(session, election_id, jurisdiction_id)
    for p in parties:
        attr = {
            "objectId": str(p["Id"]),
            "Name": p["Name"],
        }
        p_elt = et.SubElement(root, "Party", attr)

    # candidates
    candidates = an.nist_candidate(session, election_id, jurisdiction_id)
    for can in candidates:
        attr = {
            "objectId": str(can["Id"]),
            "BallotName": can["BallotName"],
        }
        can_elt = et.SubElement(root, "Candidate", attr)
        party_id_elt = et.SubElement(can_elt, "PartyId")
        party_id_elt.text = str(can["PartyId"])

    return tree

