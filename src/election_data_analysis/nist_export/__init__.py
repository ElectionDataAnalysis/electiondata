import xml.etree.ElementTree as et
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from election_data_analysis import (
    analyze as an,
    database as db,
)

# constants
default_issuer = "unspecified user of code base at github.com/ElectionDataAnalysis/election_data_analysis"
default_issuer_abbreviation = "unspecified"
default_status = "unofficial-partial"  # choices are limited by xsd schema
default_vendor_application_id = "open source software at github.com/ElectionDataAnalysis/election_data_analysis"


def nist_xml_export(
        session: Session,
        election: str,
        jurisdiction: str,
        issuer: str = default_issuer,
        issuer_abbreviation: str = default_issuer_abbreviation,
        status: str = default_status,
        vendor_application_id: str = default_vendor_application_id
):
    election_id = db.name_to_id(session, "Election", election)
    jurisdiction_id = db.name_to_id(session, "ReportingUnit", jurisdiction)
    # include jurisdiction id in gp unit ids
    gpu_idxs = {jurisdiction_id}

    # collect ids for gp units that have vote counts (NB will add to gpu_ids later)
    vc_gpus = an.nist_reporting_unit(session, election_id, jurisdiction_id)
    gpu_idxs.update({gpu["Id"] for gpu in vc_gpus})

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
    election = elections[0]
    attr = dict()
    e_elt = et.SubElement(root, "Election", attr)

    # offices
    offices = an.nist_office(session, election_id, jurisdiction_id)
    # track election districts for each contest (will need to list each as a gp unit)
    for off in offices:
        gpu_idxs.add(off["ElectoralDistrictId"])

    # other sub-elements of ElectionReport
    et.SubElement(root, "Format").text = "summary-contest"
    et.SubElement(root, "GeneratedDate").text = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    et.SubElement(root, "Issuer").text = issuer
    et.SubElement(root, "IssuerAbbreviation").text = issuer_abbreviation
    et.SubElement(root, "SequenceStart").text = "1"  # TODO placeholder
    et.SubElement(root, "SequenceEnd").text = "1"  # TODO placeholder
    et.SubElement(root, "Status").text = status
    et.SubElement(root, "VendorApplicationId").text = vendor_application_id

    # candidates (sub-elements of Election)
    candidates = an.nist_candidate(session, election_id, jurisdiction_id)
    for can in candidates:
        attr = {
            "ObjectId": f'oid{can["Id"]}',
        }
        can_elt = et.SubElement(e_elt, "Candidate", attr)
        bn_elt = et.SubElement(can_elt, "BallotName")
        et.SubElement(bn_elt, "Text").text = can["BallotName"]
        party_id_elt = et.SubElement(can_elt, "PartyId")
        party_id_elt.text = f'oid{can["PartyId"]}'

    # contests (sub-elements of Election)
    contests = an.nist_candidate_contest(session, election_id, jurisdiction_id)
    for con in contests:
        # create element for the contest
        attr = {
            "ObjectId": f'oid{con["Id"]}',
            "xsi:type": con["Type"],
        }
        con_elt = et.SubElement(e_elt, "Contest", attr)

        # create ballot selection sub-elements
        for s_dict in con["BallotSelection"]:
            attr = {
                "ObjectId": f'oid{s_dict["Id"]}',
                "xsi:type": "CandidateSelection",
            }
            cs_elt = et.SubElement(con_elt, "ContestSelection", attr)
            for vc_dict in s_dict["VoteCounts"]:
                vote_counts_elt = et.SubElement(cs_elt, "VoteCounts")
                # create GpUnitId sub-element
                et.SubElement(vote_counts_elt, "GpUnitId").text = f'oid{vc_dict["GpUnitId"]}'
                # create Type sub-elements (for CountItemType)
                et.SubElement(vote_counts_elt, "Type").text = vc_dict["CountItemType"]
                # if Type is 'other' need OtherType sub-element
                # create Count sub-element
                et.SubElement(vote_counts_elt, "Count").text = str(vc_dict["Count"])

            et.SubElement(cs_elt, "CandidateIds").text = f'oid{s_dict["CandidateId"]}'
            # TODO tech debt ^^ assumes single candidate

        # create ElectionDistrictId sub-element
        et.SubElement(con_elt, "ElectionDistrictId").text = f'oid{con["ElectionDistrictId"]}'

        # create Name sub-element
        et.SubElement(con_elt, "Name", {"Language": "en"}).text = con["ContestName"]

        # create VotesAllowed sub-element
        et.SubElement(con_elt, "VotesAllowed").text = "1"
        # TODO tech debt allow arbitrary "votes allowed

    # election scope (geographic unit for whole election)
    et.SubElement(e_elt, "ElectionScopeId").text = f"oid{jurisdiction_id}"

    # election name
    election_name_elt = et.SubElement(e_elt, "Name")
    et.SubElement(election_name_elt, "Text", {"Language": "en"}).text = election["Name"]

    # election start and end date
    et.SubElement(e_elt, "StartDate").text = "1900-01-01"  # placeholder
    et.SubElement(e_elt, "EndDate").text = "1900-01-01"  # placeholder

    # election type
    et.SubElement(e_elt, "Type").text = election["Type"]

    # get name, ru-type and composing info for all gpus
    rus = pd.read_sql("ReportingUnit", session.bind, index_col="Id")
    ru_types = pd.read_sql("ReportingUnitType", session.bind, index_col="Id")
    cruj = pd.read_sql("ComposingReportingUnitJoin", session.bind, index_col="Id")

    # relevant = rus.index.isin(gpu_ids)
    for idx in gpu_idxs:
        name = rus.loc[idx]["Name"]
        rut = rus.loc[idx]["OtherReportingUnitType"]
        if rut == "":   # if it's a standard type
            rut = ru_types.loc[rus.loc[idx]["ReportingUnitType_Id"]]["Txt"]
        assert rut != "other", f"ReportingUnit with index {idx} has type other"
        children = [
            f'oid{x}' for x in
            cruj[cruj["ParentReportingUnit_Id"] == idx]["ChildReportingUnit_Id"].unique()
            if x in gpu_idxs and x != idx
        ]
        attr = {
            "ObjectId": f'oid{idx}',
            "Name": name,
            "Type": rut,
        }
        """gpu_elt = et.SubElement(e_elt, "GpUnit", attr)
        if children:
            children_elt = et.SubElement(gpu_elt, "ComposingGpUnitIds")
            children_elt.text = " ".join(children)"""

    """# parties
    parties = an.nist_party(session, election_id, jurisdiction_id)
    for p in parties:
        attr = {
            "ObjectId": f'oid{p["Id"]}',
            "Name": p["Name"],
        }
        p_elt = et.SubElement(e_elt, "Party", attr)"""

    tree = et.ElementTree(root)
    return tree

