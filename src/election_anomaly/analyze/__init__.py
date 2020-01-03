#!usr/bin/python3

import db_routines as dbr

def roll_up (Election_Id,ReportingUnit_Id,ReportingUnitType_Id,CountItemType_Id):
    # TODO

    q = """SELECT ecj."Contest_Id",ccsj."CandidateSelection_Id", vote_sum
    FROM 
        "ElectionContestJoin" ecj 
        LEFT JOIN "CandidateContestSelectionJoin" ccsj ON ccsj."CandidateContest_Id" = ecj."Contest_Id"
        LEFT JOIN "SelectionReportingUnitVoteCountJoin" sruvcj ON 
            sruvcj."Selection_Id" = ccsj."Selection_Id"
            AND sruvcj."Election_Id" = ecj."Election_Id"
            AND sruvcj."Contest_Id" = ecj."Contest_Id"
        LEFT JOIN "VoteCount" vc ON 
            
    GROUP BY ecj."Contest_Id", ccsj."CandidateSelection_Id"
    ORDER BY ecj."Contest_Id", ccsj."CandidateSelection_Id"
    WHERE 
        ecj."Election_Id" = %s 
        AND sruvcj."ReportingUnit_Id" = %s
        AND vc."CountItemType_Id" = %s
    """ # TODO omits ballot questions
    sql_ids = []
    strs = [Election_Id,ReportingUnit_Id,CountItemType_Id]
    return # TODO return list of data rows: contest, choice, list of vote sums by vote_count_type