import xml.etree.ElementTree as et
from election_data_analysis import (
    analyze as an,
    database as db,
)

def nist_xml_export(session, election, jurisdiction):
    election_id = db.name_to_id(session, "Election", election)
    jurisdiction_id = db.name_to_id(session, "ReportingUnit", jurisdiction)

    root = et.Element("ElectionReport")

    contests = an.nist_candidate_contest(session, election_id, jurisdiction_id)
    for k in contests.keys():
        c_dict = contests[k]
        attr = {
            "objectID": c_dict['Id']",
            "Type": c_dict["Type"],
            "ContestName": c_dict["ContestName"],
        }
        c_elt = et.SubElement(root, "Contest", attr)
        for s_dict in c_dict["BallotSelection"]:
            attr = {
                "objectID": s_dict["Id"],
                "Type": "CandidateSelection",
                "CandidateId": s_dict["CandidateId"],
            }
            s_elt = et.SubElement(c_elt, "BallotSelection", attr)
            for vc_dict in s_dict["VoteCounts"]:
                attr = {
                    "GpUnitId": vc_dict["Id"],
                    "CountItemType": vc_dict["CountItemType"]
                    "Count": vc_dict["Count"]
                }
                vc_elt = et.SubElement(s_elt,"VoteCount", attr)


