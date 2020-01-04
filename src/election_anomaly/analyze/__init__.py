#!usr/bin/python3

import db_routines as dbr

def roll_up (con,cur,schema,Election_Id,ReportingUnit_Id,ReportingUnitType_Id,CountItemType_Id):
    # TODO may fail if ReportingTypeUnit is other*

    q = """SELECT
       sum(vc."Count"), ecj."Contest_Id",ccsj."CandidateSelection_Id"
    FROM
        {0}."ComposingReportingUnitJoin" cruj
        LEFT JOIN {0}."ReportingUnit" ru ON ru."Id" = cruj."ChildReportingUnit_Id"
        LEFT JOIN {0}."VoteCount" vc ON vc."ReportingUnit_Id" = cruj."ChildReportingUnit_Id"
        LEFT JOIN {0}."SelectionElectionContestVoteCountJoin" secvcj ON
            secvcj."VoteCount_Id" = vc."Id"
        LEFT JOIN {0}."ElectionContestJoin" ecj ON
            secvcj."Election_Id" = ecj."Election_Id"
        LEFT JOIN {0}."CandidateContestSelectionJoin" ccsj ON
            ccsj."CandidateContest_Id" = ecj."Contest_Id"
            AND secvcj."Selection_Id" = ccsj."CandidateSelection_Id"
            AND secvcj."Contest_Id" = ecj."Contest_Id"
        -- diagnostic
        LEFT JOIN {0}."CandidateSelection" cs ON cs."Id" = ccsj."CandidateSelection_Id"
        LEFT JOIN {0}."Candidate" c ON c."Id" = cs."Candidate_Id"
        LEFT JOIN {0}."CandidateContest" cc ON cc."Id" = ccsj."CandidateContest_Id"
    WHERE
        ecj."Election_Id" = %s
        AND cruj."ParentReportingUnit_Id" = %s
        AND vc."CountItemType_Id" = %s
        AND ru."ReportingUnitType_Id" = %s
        AND cc."Id" is not null
    GROUP BY ecj."Contest_Id", ccsj."CandidateSelection_Id"
    ORDER BY ecj."Contest_Id", ccsj."CandidateSelection_Id"

""" # TODO omits ballot questions
    sql_ids = [schema]
    strs = [Election_Id,ReportingUnit_Id,CountItemType_Id,ReportingUnitType_Id]
    a = dbr.query(q,sql_ids,strs,con,cur)
    b = [list(x) for  x in list(a)]
    return b # TODO return list of data rows: contest, choice, list of vote sums by vote_count_type


if __name__ == '__main__':
    con = dbr.establish_connection(paramfile='../../local_data/database.ini')
    cur = con.cursor()

    a = roll_up(con,cur,'cdf_xx',223,61,25,53)

    print (a)
