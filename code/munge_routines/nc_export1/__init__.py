#!/usr/bin/python3
# under construction
#/munge_routines/nc_export1/__init__.py

import re
from munge_routines import format_type_for_insert, id_and_name_from_external, upsert
import psycopg2
from psycopg2 import sql


## create external identifier / nc_export1 pairs of offices dictionary

def office_key(state_name,contest_name,term_string):
    
    p_congressional = re.compile('^US HOUSE OF REPRESENTATIVES DISTRICT (?P<district>[\d])+$')
    m = p_congressional.search(contest_name)
    if m:
        return(state_name+';US Congress;House of Representatives;' + term_string + ';District ' + str(eval(m.group('district'))))
    
    else:
        return('no match')
    

## given key, add external identifier to dictionary for certain offices

def add_ei(office_d):
    parse_d = {'General Assembly;House of Representatives':{'name':'NC HOUSE OF REPRESENTATIVES DISTRICT ','district_number_parser':re.compile(';District (?P<district_number>\d+)'),'digits':3},
            'General Assembly;Senate':{'name':'NC STATE SENATE DISTRICT ','district_number_parser':re.compile(';District (?P<district_number>\d+)'),'digits':2},
             'US Congress;House of Representatives':{'name':'US HOUSE OF REPRESENTATIVES DISTRICT ','district_number_parser':re.compile(';District (?P<district_number>\d+)'),'digits':2}}
    for k in office_d.keys():
        office_d[k].pop('ExternalIdentifier')
        found = 0
        for m in parse_d.keys():
            if re.findall(m,k):
                number = parse_d[m]['district_number_parser'].search(k).group('district_number').zfill(parse_d[m]['digits'])
                ext_id = parse_d[m]['name'] + number
                found = 1
        if found:
            if 'ExternalIdentifiers' not in office_d[k].keys():
                office_d[k]['ExternalIdentifiers'] = {}
            office_d[k]['ExternalIdentifiers']['nc_export1'] = ext_id
    return(office_d)
            
def raw_records_to_cdf(df,cdf_schema,con,cur,state_id = 0,id_type_other_id = 0):
    """ attempt to create munger-agnostic raw-to-cdf script; for now, nc_export1 stuff is hard-coded *** """

    # get BallotMeasureSelection dict (Selection:Id) from cdf schema
    q = 'SELECT "Selection", "Id" FROM {0}."BallotMeasureSelection"'
    cur.execute(sql.SQL(q).format(sql.Identifier(cdf_schema)))
    a = cur.fetchall()
    ballot_measure_selections = dict(a)

    with open('CDF_schema_def_info/tables.txt', 'r') as f:
        table_ds = eval(f.read())
    tables_d = {}
    for ddd in table_ds:
        tables_d[ddd.pop('tablename')] = ddd

        ## upsert election, get id
        [electiontype_id, otherelectiontype] = format_type_for_insert(cdf_schema, 'ElectionType',
                                                                      df.state.context_dictionary['Election'][
                                                                          df.election]['ElectionType'], con, cur)
    value_d = {'Name': df.election, 'EndDate': df.state.context_dictionary['Election'][df.election]['EndDate'],
               'StartDate': df.state.context_dictionary['Election'][df.election]['StartDate'],
               'OtherElectionType': otherelectiontype, 'ElectionType_Id': electiontype_id}
    election_id = upsert(cdf_schema, 'Election', tables_d['Election'], value_d, con, cur)[0]

    ## upsert state, get id (default Reporting Unit for ballot questions)
    if state_id == 0:
        t = 'ReportingUnit'
        [reportingunittype_id, otherreportingunittype] = format_type_for_insert(cdf_schema, 'ReportingUnitType',
                                                                                'state', con, cur)
    value_d = {'Name': df.state.name, 'ReportingUnitType_Id': reportingunittype_id,
               'OtherReportingUnitType': otherreportingunittype}
    state_id = upsert(cdf_schema, t, tables_d[t], value_d, con, cur)[0]

    ids_d = {'state': state_id, 'Election_Id': election_id}  # to hold ids of found items for later reference

    ###### get id for IdentifierType 'other'
    if id_type_other_id == 0:
        q = 'SELECT "Id" FROM {}."IdentifierType" WHERE txt = \'other\' '
    cur.execute(sql.SQL(q).format(sql.Identifier(cdf_schema)))
    a = cur.fetchall()
    if a:
        id_type_other_id = a[0][0]
    else:
        bbb = 1 / 0  # ***
    ###########################


    # get rows from raw table
    nc_export1_raw_cols = [['county','TEXT'], ['election_date','DATE'], ['precinct','TEXT'], ['contest_type','TEXT'],['contest_name','TEXT'],
                ['vote_for','INT'], ['choice','TEXT'], ['choice_party','TEXT'], ['vote_for','INT'],
                ['election_day','INT'], ['one_stop','INT'], ['absentee_by_mail','INT'], ['provisional','INT'],
                ['total_votes','INT'], ['real_precinct','TEXT']]  # *** depends on munger
    raw_col_slots = ['{' + str(i + 2) + '}' for i in range(len(nc_export1_raw_cols))]
    q = 'SELECT DISTINCT ' + ','.join(raw_col_slots) + ' FROM {0}.{1}'
    sql_ids = [df.state.schema_name, df.table_name] + [x[0] for x in nc_export1_raw_cols]
    format_args = [sql.Identifier(x) for x in sql_ids]
    cur.execute(sql.SQL(q).format(*format_args))
    rows = cur.fetchall()

    # create dictionaries for processing data from rows. Not all CDF elements are included. E.g., 'Election' element is not filled from df rows, but from df.election

    nc_export1_counts_d = {'election_day': {'CountItemType': 'election-day'}, 'one_stop': {'CountItemType': 'early'},
              'absentee_by_mail': {'CountItemType': 'absentee-mail'}, 'provisional': {'CountItemType': 'provisional'},
              'total_votes': {'CountItemType': 'total'}}
    # look up id,type pairs for each kind of count, add info to counts dictionary
    for ct,dic in nc_export1_counts_d.items():
        text = dic['CountItemType']
        [dic['CountItemType_Id'], dic['OtherCountItemType']] = format_type_for_insert(cdf_schema, 'CountItemType',
                                                                  text, con, cur)



    nc_export1_d = {'Office':[
        {'ExternalIdentifier':'contest_name',
        'InternalNameField':'Name',
        'Enumerations':{},
         'OtherFields':{},
        'Condition':'True'}
    ],
    'ReportingUnit': [        ## note: conditions should be mutually exclusive
        {'ExternalIdentifier': 'county + \';\' + precinct',
         'InternalNameField': 'Name',
         'Enumerations':{'ReportingUnitType': 'precinct'},
         'Condition': 'real_precinct == \'Y\''},
        {'ExternalIdentifier': 'county + \';\' + precinct',
         'InternalNameField': 'Name',
         'Enumerations':{'ReportingUnitType': 'other;unknown'},
         'OtherFields':{},
         'Condition': 'real_precinct != \'Y\''}
    ],
    'Party':[
        {'ExternalIdentifier':'choice_party',
        'InternalNameField':'Name',
        'Enumerations':{},
         'OtherFields': {},
         'Condition':'True'}
    ],
        'Candidate': [
            {'ExternalIdentifier': 'choice',
             'InternalNameField': 'Name',
             'Enumerations': {},
             'OtherFields': { 'Party_Id': 'ids_d["Party_Id"]'},
             # don't include fields defined from external context, such as the ElectionDistrict_Id
             'Condition': 'choice not in ballot_measure_selections.keys()'}
        ],

        'CandidateContest':[
        {'ExternalIdentifier':'contest_name',
        'InternalNameField':'Name',
        'Enumerations':{},
         'OtherFields':{'VotesAllowed':'vote_for','Office_Id':'ids_d["Office_Id"]'},    # don't include fields defined from external context, such as the ElectionDistrict_Id
        'Condition':'choice not in ballot_measure_selections.keys()'}
    ],
    'BallotMeasureContest':[
        {'ExternalIdentifier':'contest_name',
         'InternalNameField': 'Name',
         'Enumerations':{},
         'OtherFields':{'ElectionDistrict_Id':'ids_d["state"]'},
        'Condition':'choice in ballot_measure_selections.keys()'}    # Because towns are split between counties, default all ballot measure contests to state
    ],
    'BallotMeasureSelection':[
        {'ExternalIdentifier':'choice',
        'InternalNameField':'Selection',
        'Enumerations':{},
        'OtherFields':{},
         'Condition':'choice in [\'Yes\',\'No\',\'For\',\'Against\']'}
    ]
    }     # munger-dependent ***

    for row in rows:
        row_d = {}
        for i in range(len(nc_export1_raw_cols)):
            if row[i]!= 0 and not row[i]:   # if db query returned None     # awkward ***
                exec(nc_export1_raw_cols[i][0] + ' = None')
            elif nc_export1_raw_cols[i][1] == 'INT':
                exec(nc_export1_raw_cols[i][0] + ' = ' + str(row[i]) )
            else:   # *** DATE and TEXT handled identically
                exec(nc_export1_raw_cols[i][0] + ' = "'+ row[i] +'"')


        # Process all straight-forward elements into cdf
        for t in ['ReportingUnit', 'Party']:  # *** list is munger-dependent
            for item in nc_export1_d[t]:  # for each case (most elts have only one, but ReportingUnit has more) e.g. item = {'ExternalIdentifier': county,
                # 'Enumerations':{'ReportingUnitType': 'county'}}
                if eval(item['ExternalIdentifier']) and eval(item['Condition']):
                    # get internal db name and id from the info in the df row
                    [cdf_id, cdf_name] = id_and_name_from_external(cdf_schema, t,
                                                                   eval(item['ExternalIdentifier']),
                                                                   id_type_other_id, 'nc_export1', con, cur,
                                                                   item[
                                                                       'InternalNameField'])  # cdf_name may be unnecessary ***
                    if [cdf_id, cdf_name] == [None, None]:  # if no such is found in db, insert it!
                        cdf_name = eval(item['ExternalIdentifier'])
                        value_d = {item['InternalNameField']: cdf_name}  # usually 'Name' but not always
                        for e in item['Enumerations'].keys():  # e.g. e = 'ReportingUnitType'
                            [value_d[e + 'Id'], value_d['Other' + e]] = format_type_for_insert(cdf_schema, e,
                                                                                               item[
                                                                                                   'Enumerations'][
                                                                                                   e], con, cur)
                        for f in item['OtherFields'].keys():
                            value_d[f] = eval(item['OtherFields'][f])
                        if t == 'CandidateContest' or t == 'BallotMeasureContest':  # need to get ElectionDistrict_Id from contextual knowledge
                            value_d['ElectionDistrict_Id'] = ids_d['contest_reporting_unit_id']
                        cdf_id = upsert(cdf_schema, t, tables_d[t], value_d, con, cur)[0]
                ids_d[t + '_Id'] = cdf_id

        # process Ballot Measures and Candidate Contests into CDF (depends on values in row):
        selection = eval(nc_export1_d['BallotMeasureSelection'][0]['ExternalIdentifier'])
        # *** cond = nc_export1_d['BallotMeasureSelection']['ExternalIdentifier'] +' in ballot_measure_selections.keys()'
        if selection in ballot_measure_selections.keys() :     # if selection is a Ballot Measure selection, assume contest is a Ballot Measure
            ids_d['selection_id'] = ballot_measure_selections[selection]
            # fill BallotMeasureContest
            value_d = {'Name':eval(nc_export1_d['BallotMeasureContest'][0]['ExternalIdentifier']),'ElectionDistrict_Id':state_id}  # all ballot measures are assumed to be state-level ***
            ids_d['contest_id'] = upsert(cdf_schema, 'BallotMeasureContest', tables_d['BallotMeasureContest'], value_d, con, cur)[0]
            # fill ElectionContestJoin
            value_d = {'Election_Id':election_id,'Contest_Id':ids_d['contest_id']}
            upsert(cdf_schema,'ElectionContestJoin', tables_d['ElectionContestJoin'], value_d, con, cur)
            # fill BallotMeasureContestSelectionJoin ***
            value_d = {'BallotMeasureContest_Id':ids_d['contest_id'],'BallotMeasureSelection_Id':ids_d['selection_id']}
            upsert(cdf_schema, 'BallotMeasureContestSelectionJoin', tables_d['BallotMeasureContestSelectionJoin'], value_d, con, cur)

        else:       # if not a Ballot Measure (i.e., if a Candidate Contest)
            office_name = eval(nc_export1_d['Office'][0]['ExternalIdentifier'])
            q = 'SELECT f."Id", f."Name" FROM {0}."ExternalIdentifier" AS e LEFT JOIN {0}."Office" AS f ON e."ForeignId" = f."Id" WHERE e."IdentifierType_Id" = %s AND e."Value" =  %s AND e."OtherIdentifierType" = \'nc_export1\';'
            cur.execute(sql.SQL(q).format(sql.Identifier(cdf_schema)), [id_type_other_id, office_name])
            a = cur.fetchall()
            if not a: # if Office is not already associated to the munger in the db (from state's context_dictionary, for example), skip this row
               continue
            ids_d['Office_Id'] = a[0][0]

            # Find Id for ReportingUnit for contest via context_dictionary['Office']
            # find reporting unit associated to contest (not reporting unit associated to df row)
            election_district_name = df.state.context_dictionary['Office'][a[0][1]]['ElectionDistrict']
            q = 'SELECT "Id" FROM {0}."ReportingUnit" WHERE "Name" = %s'
            cur.execute(sql.SQL(q).format(sql.Identifier(cdf_schema)),[election_district_name,])
            b = cur.fetchall()
            # ids_d['contest_reporting_unit_id'] = b[0][0]  ***
            election_district_id = b[0][0]

            # insert into CandidateContest table
            votes_allowed = eval(nc_export1_d['CandidateContest'][0]['OtherFields']['VotesAllowed']) # *** munger-dependent, misses other fields e.g. NumberElected
            value_d = {'Name':election_district_name,'ElectionDistrict_Id':election_district_id,'Office_Id':ids_d['Office_Id'],'VotesAllowed':votes_allowed}
            ids_d['contest_id'] = upsert(cdf_schema,'CandidateContest',tables_d['CandidateContest'],value_d,con,cur)[0]

            # insert into Candidate table
            ballot_name = eval(nc_export1_d['Candidate'][0]['ExternalIdentifier'])
            value_d = {'BallotName':ballot_name,'Election_Id':election_id,'Party_Id':ids_d['Party_Id']}
            ids_d['Candidate_Id'] = upsert(cdf_schema,'Candidate',tables_d['Candidate'],value_d,con,cur)[0]

            # insert into CandidateSelection
            value_d = {'Candidate_Id':ids_d['Candidate_Id']}
            ids_d['selection_id'] = upsert(cdf_schema,'CandidateSelection',tables_d['CandidateSelection'],value_d,con,cur)[0]

            # create record in CandidateContestSelectionJoin
            value_d = {'CandidateContest_Id':ids_d['contest_id'],'CandidateSelection_Id': ids_d['selection_id']}
            upsert(cdf_schema, 'CandidateContestSelectionJoin', tables_d['CandidateContestSelectionJoin'], value_d, con, cur)


        ## *** Enter vote counts
        #      nc_export1_counts = {'election_day': {'CountItemType': 'election-day'}, 'one_stop': {'CountItemType': 'early'},
        #               'absentee_by_mail': {'CountItemType': 'absentee-mail'}, 'provisional': {'CountItemType': 'provisional'},
        #               'total_votes': {'CountItemType': 'total'}}

        for ct,dic in nc_export1_counts_d.items():
            value_d = {'Count':eval(ct),'ReportingUnit_Id':ids_d['ReportingUnit_Id'],'CountItemType_Id': dic['CountItemType_Id'],'OtherCountItemType':dic['OtherCountItemType']}
            # *** dupes are a problem only when contest & reporting unit are specified.
            ids_d['VoteCount_Id']=upsert(cdf_schema, 'VoteCount', tables_d['VoteCount'], value_d, con, cur,'dupes_ok')[0]

            # fill SelectionReportingUnitVoteCountJoin
            value_d = {'Selection_Id':ids_d['selection_id'],'Election_Id':ids_d['Election_Id'],'ReportingUnit_Id':ids_d['ReportingUnit_Id'],'VoteCount_Id':ids_d['VoteCount_Id']}

        con.commit()
        print(row)
    return str(ids_d)

def element_to_cdf (cdf_schema,t,munger_d,ids_d,id_type_other_id,con,cur):
    """Under construction. Need to figure out how to efficiently pass the vector of raw values ***"""
    for item in munger_d[t]:  # for each case (most elts have only one, but ReportingUnit has more) e.g. item = {'ExternalIdentifier': county,
        # 'Enumerations':{'ReportingUnitType': 'county'}}
        if eval(item['Condition']):    # if the condition holds
            # get internal db name and id from the info in the df row
            [cdf_id, cdf_name] = id_and_name_from_external(cdf_schema, t,
                                                           eval(item['ExternalIdentifier']),
                                                           id_type_other_id, 'nc_export1', con, cur,
                                                           item[
                                                               'InternalNameField'])  # cdf_name may be unnecessary ***
            if [cdf_id, cdf_name] == [None, None]:  # if no such is found in db, insert it!
                cdf_name = eval(item['ExternalIdentifier'])
                value_d = {item['InternalNameField']: cdf_name}  # usually 'Name' but not always
                for e in item['Enumerations'].keys():  # e.g. e = 'ReportingUnitType'
                    [value_d[e + 'Id'], value_d['Other' + e]] = format_type_for_insert(cdf_schema, e,
                                                                                       item[
                                                                                           'Enumerations'][
                                                                                           e], con, cur)
                for f in item['OtherFields'].keys():
                    value_d[f] = eval(item['OtherFields'][f])
                if t == 'CandidateContest' or t == 'BallotMeasureContest':  # need to get ElectionDistrict_Id from contextual knowledge
                    value_d['ElectionDistrict_Id'] = ids_d['contest_reporting_unit_id']
                cdf_id = upsert(cdf_schema, t, tables_d[t], value_d, con, cur)[0]
        ids_d[t + '_Id'] = cdf_id
        return(ids_d)


    


