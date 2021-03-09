import xml.etree.ElementTree as et
from typing import Optional
import pandas as pd
from sqlalchemy.orm import Session
from psycopg2 import sql
from datetime import datetime, timezone
from election_data_analysis import (
    database as db,
    analyze as an,
)

# constants set by issuer
default_issuer = "unspecified user of code base at github.com/ElectionDataAnalysis/election_data_analysis"
default_issuer_abbreviation = "unspecified"
default_status = "unofficial-partial"  # choices are limited by xsd schema
default_vendor_application_id = "open source software at github.com/ElectionDataAnalysis/election_data_analysis"

# constants dictated by NIST
schema_location = "https://github.com/usnistgov/ElectionResultsReporting/raw/version2/NIST_V2_election_results_reporting.xsd"
namespace = "http://itl.nist.gov/ns/voting/1500-100/v2"


def nist_v2_xml_export_tree(
        session: Session,
        election: str,
        jurisdiction: str,
        issuer: str = default_issuer,
        issuer_abbreviation: str = default_issuer_abbreviation,
        status: str = default_status,
        vendor_application_id: str = default_vendor_application_id
) -> et.ElementTree:
    """Creates a tree in the NIST common data format (V2) containing the results
    from the given election and jurisdiction. Note that all available results will
    be exported. I.e., if database has precinct-level results, the tree will
    contain precinct-level results. See"""
    # set up
    election_id = db.name_to_id(session, "Election", election)
    jurisdiction_id = db.name_to_id(session, "ReportingUnit", jurisdiction)

    # include jurisdiction id in gp unit ids
    gpu_idxs = {jurisdiction_id}

    # get vote count data
    results_df = read_vote_count_nist(session, election_id, jurisdiction_id)

    # collect ids for gp units that have vote counts, gp units that are election districts
    gpu_idxs.update(results_df.ReportingUnit_Id.unique())
    gpu_idxs.update(results_df.ElectionDistrict_Id.unique())

    # ElectionReport (root)
    attr = {
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:schemaLocation": schema_location,
        "xmlns": namespace,
    }
    root = et.Element("ElectionReport", attr)

    # add election sub-element of ElectionReport
    e_elt = et.SubElement(root, "Election")

    # other sub-elements of ElectionReport
    et.SubElement(root, "Format").text = "summary-contest"  # NB NIST restricts choices
    et.SubElement(root, "GeneratedDate").text = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # get name, ru-type and composing info for all gpus
    rus = pd.read_sql("ReportingUnit", session.bind, index_col="Id")
    ru_types = pd.read_sql("ReportingUnitType", session.bind, index_col="Id")
    cruj = pd.read_sql("ComposingReportingUnitJoin", session.bind, index_col="Id")

    # add each gpu
    for idx in gpu_idxs:
        name = rus.loc[idx]["Name"]

        children = [
            f'oid{x}' for x in
            cruj[cruj["ParentReportingUnit_Id"] == idx]["ChildReportingUnit_Id"].unique()
            if x in gpu_idxs and x != idx
        ]
        attr = {
            "ObjectId": f'oid{idx}',
            "xsi:type": "ReportingUnit",
        }
        gpu_elt = et.SubElement(root, "GpUnit", attr)
        if children:
            children_elt = et.SubElement(gpu_elt, "ComposingGpUnitIds")
            children_elt.text = " ".join(children)
        gpu_name = et.SubElement(gpu_elt, "Name")
        et.SubElement(gpu_name, "Text", {"Language": "en"}).text = name
        et.SubElement(gpu_elt, "Type").text = ru_types.loc[rus.loc[idx]["ReportingUnitType_Id"]]["Txt"]
        if ru_types.loc[rus.loc[idx]["ReportingUnitType_Id"]]["Txt"] == "other":
            et.SubElement(gpu_elt, "OtherType").text = rus.loc[idx]["OtherReportingUnitType"]

    # other sub-elements of ElectionReport
    et.SubElement(root, "Issuer").text = issuer
    et.SubElement(root, "IssuerAbbreviation").text = issuer_abbreviation

    # add each party
    party_df = results_df[["Party_Id", "PartyName"]].drop_duplicates()
    for i, p in party_df.iterrows():
        p_elt = et.SubElement(root, "Party", {"ObjectId": f'oid{p["Party_Id"]}',})
        p_name_elt = et.SubElement(p_elt, "Name")
        et.SubElement(p_name_elt, "Text", {"Language": "en"}).text = p["PartyName"]

    # still more sub-elements of ElectionReport
    et.SubElement(root, "SequenceStart").text = "1"  # TODO placeholder
    et.SubElement(root, "SequenceEnd").text = "1"  # TODO placeholder
    et.SubElement(root, "Status").text = status
    et.SubElement(root, "VendorApplicationId").text = vendor_application_id

    # add each candidate (as sub-element of Election)
    candidate_df = results_df[["Candidate_Id", "BallotName", "Party_Id"]].drop_duplicates()
    for i, can in candidate_df.iterrows():
        can_elt = et.SubElement(e_elt, "Candidate", {"ObjectId": f'oid{can["Candidate_Id"]}'})
        bn_elt = et.SubElement(can_elt, "BallotName")
        et.SubElement(bn_elt, "Text", {"Language": "en"}).text = can["BallotName"]
        party_id_elt = et.SubElement(can_elt, "PartyId")
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
        con_elt = et.SubElement(e_elt, "Contest", attr)

        # create ballot selection sub-elements
        # TODO (remove assumption that it's a  CandidateContest)
        selection_idxs = results_df[results_df.Contest_Id == con["Contest_Id"]][
            "Selection_Id"
        ].unique()
        for s_idx in selection_idxs:
            attr = {
                "ObjectId": f'oid{s_idx}',
                "xsi:type": "CandidateSelection",
            }
            cs_elt = et.SubElement(con_elt, "ContestSelection", attr)
            vc_df = results_df[
                (results_df.Contest_Id == con["Contest_Id"]) & (results_df.Selection_Id == s_idx)
            ][["ReportingUnit_Id", "Candidate_Id", "CountItemType", "OtherCountItemType", "Count"]].drop_duplicates()
            for i, vc in vc_df.iterrows():
                vote_counts_elt = et.SubElement(cs_elt, "VoteCounts")
                # create GpUnitId sub-element
                et.SubElement(vote_counts_elt, "GpUnitId").text = f'oid{vc["ReportingUnit_Id"]}'
                # create Type sub-elements (for CountItemType)
                et.SubElement(vote_counts_elt, "Type").text = vc["CountItemType"]
                if vc["CountItemType"] == "other":
                    et.SubElement(vote_counts_elt, "OtherType").text = vc["OtherCountItemType"]
                # create Count sub-element
                et.SubElement(vote_counts_elt, "Count").text = str(vc["Count"])

            candidate_ids = " ".join([f'oid{x}' for x in vc_df.Candidate_Id.unique()])
            et.SubElement(cs_elt, "CandidateIds").text = candidate_ids

        # create ElectionDistrictId sub-element
        et.SubElement(con_elt, "ElectionDistrictId").text = f'oid{con["ElectionDistrict_Id"]}'

        # create Name sub-element
        et.SubElement(con_elt, "Name").text = con["ContestName"]

        # create VotesAllowed sub-element
        et.SubElement(con_elt, "VotesAllowed").text = "1"
        # TODO tech debt allow arbitrary "votes allowed

    # election scope (geographic unit for whole election)
    et.SubElement(e_elt, "ElectionScopeId").text = f"oid{jurisdiction_id}"

    # add properties of particular election
    # NB we assume only one election!
    election_df = pd.read_sql_table("Election", session.bind, index_col="Id")
    election_type_df = pd.read_sql_table("ElectionType", session.bind, index_col="Id")
    election_name_elt = et.SubElement(e_elt, "Name")
    et.SubElement(election_name_elt, "Text", {"Language": "en"}).text = election_df.loc[election_id]["Name"]
    et.SubElement(e_elt, "StartDate").text = "1900-01-01"  # placeholder
    et.SubElement(e_elt, "EndDate").text = "1900-01-01"  # placeholder

    # election type
    e_type = election_type_df.loc[
        election_df.loc[election_id]["ElectionType_Id"]
    ]["Txt"]
    et.SubElement(e_elt, "Type").text = e_type
    if e_type == "other":
        et.SubElement(e_elt, "OtherType").text = election_df.loc[election_id]["OtherElectionType"]

    tree = et.ElementTree(root)
    return tree


def read_vote_count_nist(
    session: Session,
    election_id: int,
    reporting_unit_id: int,
    rollup_ru_type: Optional[str] = None
) ->  pd.DataFrame:
    """The VoteCount table is the only place that maps contests to a specific
    election. But this table is the largest one, so we don't want to use pandas methods
    to read into a DF and then filter"""

    fields = ["ReportingUnitType_Id",
                "Party_Id", "PartyName", "Candidate_Id", "BallotName",
                "Contest_Id", "ContestType", "ElectionDistrict_Id", "ContestName",
                "Selection_Id", "ReportingUnit_Id", "CountItemType", "OtherCountItemType",
              "Count"
    ]
    q = sql.SQL(
        """
        SELECT  DISTINCT {fields}
        FROM    (
                    SELECT  "Id" as "VoteCount_Id", "Contest_Id", "Selection_Id",
                            "ReportingUnit_Id", "Election_Id", "CountItemType_Id", 
                            "OtherCountItemType", "Count"
                    FROM    "VoteCount"
                ) vc
                JOIN (SELECT "Id", "Name" as "ContestName" , contest_type as "ContestType" FROM "Contest") con on vc."Contest_Id" = con."Id"
                JOIN "ComposingReportingUnitJoin" cruj ON vc."ReportingUnit_Id" = cruj."ChildReportingUnit_Id"
                JOIN "CandidateSelection" cs ON vc."Selection_Id" = cs."Id"
                JOIN "Candidate" c on cs."Candidate_Id" = c."Id"
                JOIN (SELECT "Id", "Name" AS "PartyName" FROM "Party") p ON cs."Party_Id" = p."Id"
                JOIN "CandidateContest" cc ON con."Id" = cc."Id"
                JOIN (SELECT "Id", "Name" as "OfficeName", "ElectionDistrict_Id" FROM "Office") o on cc."Office_Id" = o."Id"
                -- this reporting unit info refers to the districts (state house, state senate, etc)
                JOIN (SELECT "Id", "Name" AS "ReportingUnitName", "ReportingUnitType_Id" FROM "ReportingUnit") ru on o."ElectionDistrict_Id" = ru."Id"
                JOIN (SELECT "Id", "Txt" AS unit_type FROM "ReportingUnitType") rut on ru."ReportingUnitType_Id" = rut."Id"
                -- this reporting unit info refers to the geopolitical divisions (county, state, etc)
                JOIN (SELECT "Id" as "GP_Id", "Name" AS "GPReportingUnitName", "ReportingUnitType_Id" AS "GPReportingUnitType_Id" FROM "ReportingUnit") gpru on vc."ReportingUnit_Id" = gpru."GP_Id"
                JOIN (SELECT "Id", "Txt" AS "GPType" FROM "ReportingUnitType") gprut on gpru."GPReportingUnitType_Id" = gprut."Id"
                JOIN (SELECT "Id", "Name" as "ElectionName", "ElectionType_Id", "OtherElectionType" FROM "Election") e on vc."Election_Id" = e."Id"
                JOIN (SELECT "Id", "Txt" as "ElectionType" FROM "ElectionType") et on e."ElectionType_Id" = et."Id"
                JOIN (SELECT "Id", "Txt" as "CountItemType" FROM "CountItemType") cit on vc."CountItemType_Id" = cit."Id"
        WHERE   "Election_Id" = %s
                AND "ParentReportingUnit_Id" = %s
        """
    ).format(
        fields=sql.SQL(",").join(sql.Identifier(field) for field in fields),
    )
    connection = session.bind.raw_connection()
    cursor = connection.cursor()
    cursor.execute(q, [election_id, reporting_unit_id])
    results = cursor.fetchall()
    unrolled_df = pd.DataFrame(results, columns=fields)

    if rollup_ru_type:
        results_df, err_str = an.rollup(
            session,
            unrolled_df,
            "Count",
            "ReportingUnit_Id",
            "ReportingUnit_Id",
            rollup_rut=rollup_ru_type,
            ignore=["ReportingUnitType_Id"]
        )
        # TODO add ReportingUnitType_Id column
        #  What happened to OtherReportingUnitType? Might need it.
        #  NB: the ReportingUnitType_Id from read_vote_count is the Election District type
    else:
        results_df = unrolled_df
    return results_df


