#!usr/bin/python3

import numpy as np
import analyze as an
import pandas as pd
import db_routines as dbr

def counts_from_selection_list(con, cur, schema, selection_list,
                                ReportingUnit_Id,Election_Id, Contest_Id, CountItem_Type_Id):
    """Given a list of Selection_Ids -- think list of candidates -- and a ReportingUnit_Id,
    along with election, contest, countitem type,
    return ordered list (tuple?) of corresponding counts rolled up from precincts"""
    # TODO this seems inefficient.

    ct_list = []
    for si in selection_list:
         q = """SELECT
       COALESCE(sum(vc."Count"),0)
    FROM
        {0}."SelectionElectionContestVoteCountJoin" secvcj 
        LEFT JOIN {0}."VoteCount" vc ON secvcj."VoteCount_Id" = vc."Id"
        LEFT JOIN {0}."ComposingReportingUnitJoin" cruj ON vc."ReportingUnit_Id" = cruj."ChildReportingUnit_Id"
        LEFT JOIN {0}."ReportingUnit" ru ON ru."Id" = cruj."ChildReportingUnit_Id"            
    WHERE
        secvcj."Election_Id" = %s
        AND secvcj."Selection_Id" = %s
        AND secvcj."Contest_Id" = %s
        AND cruj."ParentReportingUnit_Id" = %s
        AND vc."CountItemType_Id" = %s
        AND ru."ReportingUnitType_Id" = 25
    """  # TODO correct hard-coding of precinct ReportingUnitType_Id (25)
         sql_ids = [schema]
         strs = (Election_Id,si,Contest_Id,ReportingUnit_Id,CountItemType_Id)
            # TODO correct hard-coding of precinct childReportingUnitType (25)
         ct_list.append(dbr.query(q,sql_ids,strs,con,cur)[0][0])

    return ct_list

def zscore(con,cur, schema,Election_Id,CandidateContest_Id,ReportingUnit_Id,childReportingUnitType_Id,CountItem_Type_Id):
    """Given an election, contest, (larger) reporting unit,
        type of reporting unit for subdivision, and count type,
    return list of reporting units with a z-score (of the count vector) for each reporting unit """
    # TODO z-score seems an unnatural measure for our case, as the distribution of sums of distances is presumably not gaussian.
    candidate_selection_id_list, cts_by_ru_d = count_tuples(con,cur, schema,Election_Id,CandidateContest_Id,ReportingUnit_Id,childReportingUnitType_Id,CountItem_Type_Id)
    zscore_li = an.euclidean_zscore([ct[1] for ct in cts_by_ru_d])
    ru_list = [x[0] for x in cts_by_ru_d] # list of reporting units
    return  ru_list ,zscore_li

def count_tuples(con,cur, schema,Election_Id,CandidateContest_Id,ReportingUnit_Id,childReportingUnitType_Id,CountItem_Type_Id):
    """Given an election, contest, (larger) reporting unit,
        type of reporting unit for subdivision, and count type,
    return list of reportingunit - count tuple pairs
    and a single list of CandidateSelection_Ids corresponding to order of entries the count tuples """

    # get CandidateSelection_Ids
    q = """
    SELECT DISTINCT "CandidateSelection_Id" FROM {0}."CandidateContestSelectionJoin"
    WHERE "Election_Id"= %s AND "CandidateContest_Id" = %s 
    """
    sql_ids = [schema]
    strs = (Election_Id,CandidateContest_Id)
    csi_list = [x[0] for x in dbr.query(q,sql_ids,strs,con,cur)]

    q = """SELECT DISTINCT cruj."ChildReportingUnit_Id" 
    FROM {0}."ComposingReportingUnitJoin" AS cruj
        LEFT JOIN {0}."ReportingUnit" AS ru ON ru."Id" = cruj."ChildReportingUnit_Id"
    WHERE cruj."ParentReportingUnit_Id" = %s AND ru."ReportingUnitType_Id" = %s
    """
    sql_ids = [schema]
    strs = [ReportingUnit_Id,childReportingUnitType_Id]
    sub_ru_list = [x[0] for x in dbr.query(q,sql_ids,strs,con,cur)]

    li = []
    for ru in sub_ru_list:
        # put entry into d -- ru: count tuple for ru
        li.append([ru, counts_from_selection_list(con,cur,schema,csi_list, ru,Election_Id,CandidateContest_Id,CountItem_Type_Id)])
    # TODO
    return csi_list, li

def candidate_contest_roll_up (con,cur,schema,Election_Id,ReportingUnit_Id,ReportingUnitType_Id,CountItemType_Id):
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
    """

    sql_ids = [schema]
    strs = [Election_Id,ReportingUnit_Id,CountItemType_Id,ReportingUnitType_Id]
    return dbr.query(q,sql_ids,strs,con,cur)

def get_outlier_ru(con,cur,schema,
                Election_Id,CandidateContest_Id,ReportingUnit_Id,childReportingUnitType_Id,
                     CountItemType_Id):
    ru_list, zscore_list = zscore(con,cur,schema,
                Election_Id,CandidateContest_Id,ReportingUnit_Id,childReportingUnitType_Id,
                     CountItemType_Id)
    max_index = zscore_list.index(max(zscore_list))
    outlier_reporting_unit = dbr.read_field_value(con,cur,schema,('ReportingUnit',ru_list[max_index],'Name'))
    return outlier_reporting_unit


if __name__ == '__main__':
    con = dbr.establish_connection(paramfile='../../local_data/database.ini')
    cur = con.cursor()

    scenario = input('Enter xx or nc\n')
    if scenario == 'xx':
        schema = 'cdf_xx'
        Election_Id = 262
        ReportingUnit_Id = 62
        childReportingUnitType_Id = 25
        CountItemType_Id = 52
        CandidateContest_Id = 922
    elif scenario == 'nc':
        schema = 'cdf_nc'
        Election_Id = 12681
        ReportingUnit_Id = 59
        childReportingUnitType_Id = 19  # county
        CountItemType_Id = 50   # absentee-mail
        CandidateContest_Id = 13257

    a = candidate_contest_roll_up(con,cur,schema,
                Election_Id,ReportingUnit_Id,childReportingUnitType_Id,CountItemType_Id)
    print (a)

    csi_list,d = count_tuples(con,cur,schema,
                Election_Id,CandidateContest_Id,ReportingUnit_Id,childReportingUnitType_Id,
                     CountItemType_Id)
    print('csi_list:')
    print (csi_list)
    print ('d')
    print(d)

    #df = pd.DataFrame(data=d,index=csi_list)
    #print (df)

    print(get_outlier_ru(con,cur,schema,
                Election_Id,CandidateContest_Id,ReportingUnit_Id,childReportingUnitType_Id,
                     CountItemType_Id))


