#!usr/bin/python3

import numpy as np
import matplotlib.pyplot as plt

np.set_printoptions(precision=1)
import analyze as an
import pandas as pd
import db_routines as dbr


def counts_from_selection_list(con, cur, schema, selection_list,
                                ReportingUnit_Id,Election_Id, Contest_Id, CountItemType_Id):
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

def zscore(con,cur, schema,Election_Id,CandidateContest_Id,ReportingUnit_Id,childReportingUnitType_Id,CountItem_Type_Id,mode = 'pct'):
    """Given an election, contest, (larger) reporting unit,
        type of reporting unit for subdivision, and count type,
        and mode ('pct' for percentages; 'votes' for raw totals of vote numbers
    return list of reporting units with a z-score (of the count vector) for each reporting unit """
    # TODO z-score seems an unnatural measure for our case, as the distribution of sums of distances is presumably not gaussian.
    candidate_selection_id_list, cts_by_ru_d = count_tuples(con,cur, schema,Election_Id,CandidateContest_Id,ReportingUnit_Id,childReportingUnitType_Id,CountItem_Type_Id)

    ru_list = [ ct[0] for ct in cts_by_ru_d if sum (ct[1]) != 0] # list of reporting units
    if mode == 'votes':
        vector_list = [ct[1] for ct in cts_by_ru_d]
    elif mode == 'pct':
        vector_list = [ [y / sum(ct[1]) for y in ct[1]] for ct in cts_by_ru_d  if sum(ct[1]) != 0]

    print('zscore(): vector_list is: ')
    print (np.array(vector_list))
    zscore_li = an.euclidean_zscore(vector_list)
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
                     CountItemType_Id,mode = 'pct'):
    ru_list, zscore_list = zscore(con,cur,schema,
                Election_Id,CandidateContest_Id,ReportingUnit_Id,childReportingUnitType_Id,
                     CountItemType_Id,'pct')
    max_zscore = max(zscore_list)
    max_index = zscore_list.index(max_zscore)
    outlier_reporting_unit = dbr.read_field_value(con,cur,schema,('ReportingUnit',ru_list[max_index],'Name'))
    print('Zscore list is: \n' + str(np.array(zscore_list)))
    print('Outlier is: '+outlier_reporting_unit)
    print('Zscore of outlier is: ')
    print(max_zscore)
    return outlier_reporting_unit, max_zscore

def bar_chart_by_county(con,cur,schema,Election_Id,Contest_Id,CountItemType_Id,mode = 'pct'):
    # TODO
    #%% create dataframe indexed by county, column for votes or pcts (depending on mode)
    data =
    return

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
        Election_Id = 15834
        ReportingUnit_Id = 59
        childReportingUnitType_Id = 19  # county
        CountItemType_Id = 50   # absentee-mail
        CandidateContest_Id = 16410

    a = candidate_contest_roll_up(con,cur,schema,
                Election_Id,ReportingUnit_Id,childReportingUnitType_Id,CountItemType_Id)
    print (a)

    csi_list,d = count_tuples(con,cur,schema,
                Election_Id,CandidateContest_Id,ReportingUnit_Id,childReportingUnitType_Id,
                   CountItemType_Id)
#    print('csi_list:')
#    print (csi_list)
    print ('nonzero in d')

    a1 = np.array([ y for y in d if y[1] != [0,0,0]])
    print(a1)

    a = np.array( [( y[0],dbr.read_field_value(con,cur,schema,('ReportingUnit',y[0],'Name'))) for y in d if y[1] != [0,0,0]])

    print(a)



    print(get_outlier_ru(con,cur,schema,
                Election_Id,CandidateContest_Id,ReportingUnit_Id,childReportingUnitType_Id,
                     CountItemType_Id))


    #%% try bar chart

    # data to plot
    n_groups = 4
    means_frank = (5, 55, 40, 65)
    means_guido = (85, 62, 54, 20)

    # create plot
    fig, ax = plt.subplots()
    index = np.arange(n_groups)
    bar_width = 0.35
    opacity = 0.8

    rects1 = plt.bar(index, means_frank, bar_width,
                     alpha=opacity,
                     color='b',
                     label='Frank')

    rects2 = plt.bar(index + bar_width, means_guido, bar_width,
                     alpha=opacity,
                     color='g',
                     label='Guido')

    plt.xlabel('Person')
    plt.ylabel('Scores')
    plt.title('Scores by person')
    plt.xticks(index + bar_width, ('A', 'B', 'C', 'D'))
    plt.legend()

    plt.tight_layout()
    plt.show()