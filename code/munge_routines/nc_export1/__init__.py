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
            
def rtcdf(df,cdf_schema,con,cur,state_id = 0,id_type_other_id = 0):
    """ attempt to create munger-agnostic raw-to-cdf script; for now, nc_export1 stuff is hard-coded *** """
    rs = ['raw_to_cdf:']

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

    ###### get rows from raw table
    raw_cols = [['county','TEXT'], ['election_date','DATE'], ['precinct','TEXT'], ['contest_name','TEXT'],
                ['vote_for','INT'], ['choice','TEXT'], ['choice_party','TEXT'], ['vote_for','INT'],
                ['election_day','INT'], ['one_stop','INT'], ['absentee_by_mail','INT'], ['provisional','INT'],
                ['total_votes','INT'], ['real_precinct','TEXT']]  # *** depends on munger

    raw_col_slots = ['{' + str(i + 2) + '}' for i in range(len(raw_cols))]

    q = 'SELECT DISTINCT ' + ','.join(raw_col_slots) + ' FROM {0}.{1}'
    sql_ids = [df.state.schema_name, df.table_name] + [x[0] for x in raw_cols]
    format_args = [sql.Identifier(x) for x in sql_ids]
    cur.execute(sql.SQL(q).format(*format_args))
    rows = cur.fetchall()

    # create dictionaries for processing

    nc_export1_d = {'ReportingUnit': [        ## note: conditions should be mutually exclusive
        {'ExternalIdentifier': 'county + \';\' + precinct',
         'Enumerations':{'ReportingUnitType': 'precinct'},
         'Condition': 'real_precinct == \'Y\''},
        {'ExternalIdentifier': 'county + \';\' + precinct',
         'Enumerations':{'ReportingUnitType': 'other;unknown'},
         'Condition': 'real_precinct != \'Y\''}
    ],
    'Party':[
        {'ExternalIdentifier':'choice_party',
        'Enumerations':{},
        'Condition':'True'}
    ],
    'Election':[
        {'ExternalIdentifier':'election_date',
         'Enumerations':{}, # only list enumerations that require knowledge outside the file. E.g., omit 'ElectionType':'general'
        'Condition':'1'}
    ],
    'Office':[
        {'ExternalIdentifier':'contest_name',
        'Enumerations':{},
        'Condition':'1'}
    ]
    }     # munger-dependent ***
    for row in rows:
        raw_values_d = {}
        for i in range(len(raw_cols)):
            if not row[i]:   # if db query returned None
                exec(raw_cols[i][0] + ' = None')
            elif raw_cols[i][1] == 'INT':
                exec(raw_cols[i][0] + ' = ' + str(row[i]) )
            else:   # *** DATE and TEXT handled identically
                exec( raw_cols[i][0] + ' = "'+ row[i] +'"')
        ids_d = {'state':state_id,'Election_Id':election_id}  # to hold ids of found items for later reference
        for t in nc_export1_d.keys():       # e.g., t = 'ReportingUnit'
            for item in nc_export1_d[t]:    # e.g. item = {'ExternalIdentifier': county,
                                            # 'Enumerations':{'ReportingUnitType': 'county'},'Conditions': []}
                if eval(item['Condition']):
                    # get internal db id
                    [cdf_id,cdf_name] = id_and_name_from_external(cdf_schema, t, eval(item['ExternalIdentifier']), id_type_other_id, 'nc_export1', con, cur)     # cdf_name may be unnecessary ***
                    if [cdf_id,cdf_name] == [None,None]:    # if no such is found in db, insert it!
                        cdf_name = eval(item['ExternalIdentifier'])
                        value_d = {'Name': cdf_name}    # *** some tables (e.g., BallotMeasureSelection) don't have Names ***
                        for e in item['Enumerations'].keys():  # e.g. e = 'ReportingUnitType'
                            [value_d[e+'Id'],value_d['Other'+e]] = format_type_for_insert(cdf_schema,e, item['Enumerations'][e],con,cur)
                        # *** 'other_element_refs': [{'fieldname': 'ElectionDistrict_Id', 'refers_to': 'ReportingUnit'}]
                        cdf_id = upsert(cdf_schema,t,tables_d[t],value_d,con,cur)[0]
                ids_d[t+'_Id'] = cdf_id
    return str(ids_d)


def raw_to_cdf(df,cdf_schema,con,cur,state_id = 0,id_type_other_id = 0):        #e.g., d = {'ReportingUnit':{'North Carolina':59, 'North Carolina;Alamance County':61} ... }
    """ load data from df to cdf; this should eventually be written in a munger-agnostic way, and also extended to all appropriate tables ***
    optional arguments:
            state_id and id_type_other_id: 0 values flag that none was supplied
    """
    rs = ['raw_to_cdf:']
    
    with open('CDF_schema_def_info/tables.txt','r') as f:
        table_ds = eval(f.read())
    tables_d = {}
    for ddd in table_ds:
        tables_d[ddd.pop('tablename')] = ddd

    
    ## upsert election, get id
    [electiontype_id,otherelectiontype] = format_type_for_insert(cdf_schema,'ElectionType',df.state.context_dictionary['Election'][df.election]['ElectionType'],con,cur)
    value_d = {'Name':df.election,'EndDate':df.state.context_dictionary['Election'][df.election]['EndDate'], 'StartDate':df.state.context_dictionary['Election'][df.election]['StartDate'], 'OtherElectionType':otherelectiontype,'ElectionType_Id':electiontype_id}
    election_id = upsert(cdf_schema,'Election',tables_d['Election'],value_d,con,cur)[0]
    
    ## upsert state, get id (default Reporting Unit for ballot questions)
    if state_id == 0:
        t = 'ReportingUnit'
        [reportingunittype_id,otherreportingunittype] = format_type_for_insert(cdf_schema,'ReportingUnitType','state',con,cur)
        value_d = {'Name':df.state.name,'ReportingUnitType_Id':reportingunittype_id,'OtherReportingUnitType':otherreportingunittype}
        state_id = upsert(cdf_schema,t,tables_d[t],value_d,con,cur)[0]

    ###### get id for IdentifierType 'other'
    if id_type_other_id == 0:
        q = 'SELECT "Id" FROM {}."IdentifierType" WHERE txt = \'other\' '
        cur.execute(sql.SQL(q).format( sql.Identifier(cdf_schema)))
        a = cur.fetchall()
        if a:
            id_type_other_id = a[0][0]
        else:
            bbb = 1/0 # ***
    ###########################
    
    ###### get rows from raw table
    raw_cols = ['county', 'election_date','precinct', 'contest_name', 'vote_for', 'choice', 'choice_party', 'vote_for','election_day', 'one_stop', 'absentee_by_mail', 'provisional', 'total_votes','real_precinct']    # *** depends on munger
    
    raw_col_slots = ['{'+ str(i+2)+'}' for i in range(len(raw_cols))]
    
    q = 'SELECT DISTINCT '+ ','.join(raw_col_slots) + ' FROM {0}.{1}'
    sql_ids = [df.state.schema_name,df.table_name] + raw_cols
    format_args = [sql.Identifier(x) for x in sql_ids]
    cur.execute(sql.SQL(q).format( *format_args))
    rows = cur.fetchall()
    
    ### create dictionaries for more efficient processing
    name_d = {}
    choice_d = {}
    name_choice_d = {}
    
    
    for row in rows:
        exec ('['+ ','.join(raw_cols) +'] = row' )  # load data into variables named per raw_cols

        name_d[n] = vf
        choice_d[ch]=ch_p
        if n in name_choice_d.keys():
            name_choice_d[n].append(ch)
        else:
            name_choice_d[n] = [ch]
    ########
    
    for name in name_d.keys():     # loop over contests
        ## if contest is a ballot question (all choices are ballot measure selections)
        if all(x in ['Against','For','Yes','No'] for x in name_choice_d[name]):       ## *** store list of ballot measure selections with other enumerations? *** handle error if different yes/no text used?
            t = 'BallotMeasureContest'
            value_d = {'Name':name}
            contest_id = upsert(cdf_schema,t,tables_d[t],value_d,con,cur)[0]
            rs.append('Inserted Ballot Measure Contest '+name)
            
            ### insert BallotMeasureSelections and Join into cdf
            for ch in name_choice_d[name]:
                t = 'BallotMeasureSelection'
                value_d = {'Selection':ch}
                ballotmeasureselection_id = upsert(cdf_schema,t,tables_d[t],value_d,con,cur)[0]
                
                t = 'BallotMeasureContestSelectionJoin'
                value_d = {'BallotMeasureContest_Id':contest_id,'BallotMeasureSelection_Id':ballotmeasureselection_id}
                upsert(cdf_schema,t,tables_d[t],value_d,con,cur)[0]
        
            
        else:
            [office_id,office_name] = id_and_name_from_external (cdf_schema,'Office',name,id_type_other_id,'nc_export1',con,cur)
            ## find the corresponding office (internal db name)
            q = 'SELECT f."Id", f."Name" FROM {0}."ExternalIdentifier" AS e LEFT JOIN {0}."Office" AS f ON e."ForeignId" = f."Id" WHERE e."IdentifierType_Id" = %s AND e."Value" =  %s AND e."OtherIdentifierType" = \'nc_export1\';'
            cur.execute(sql.SQL(q).format(sql.Identifier(cdf_schema)),[id_type_other_id,name])
            a = cur.fetchall()
            rs.append(str(a))
            if not a:
                rs.append('No office in context dictionary for '+df.state.name+' corresponding to office '+ name+'of type '+ str(id_type_other_id))
            #if office_name is not None -- this will entirely skip offices not listed in the state's context_dictionary!
            else:
                [office_id,office_name] = a[0]
                rs.append(str(office_name))
                
                t = 'ReportingUnit'
                [id,txt]= format_type_for_insert (cdf_schema,'ReportingUnitType',df.state.context_dictionary['Office'][office_name]['ElectionDistrictType'] ,con,cur)
                value_d = {'Name':office_name,'ReportingUnitType_Id':id,'OtherReportingUnitType':txt}
                election_district_id = upsert(cdf_schema,t,tables_d[t],value_d,con,cur)[0]
                
                t = 'CandidateContest'
                value_d = {'Name':name,'VotesAllowed':name_d[name],'Office_Id':office_id,'ElectionDistrict_Id':election_district_id}
                contest_id = upsert(cdf_schema,t,tables_d[t],value_d,con,cur)[0]
                rs.append('Inserted candidate contest ' + name)
                
                ### insert CandidateSelections and Join into cdf
                for ch in name_choice_d[name]:
                    t = 'Candidate'
                    value_d = {'BallotName':ch,'Election_Id':election_id,'Party_Id':6465}  # *** need to get actual Party_Id
                    cand_id = upsert(cdf_schema,t,tables_d[t],value_d,con,cur)[0]
                    
                    t = 'CandidateSelection'
                    value_d = {'Candidate_Id':cand_id}
                    selection_id = upsert(cdf_schema,t,tables_d[t],value_d,con,cur)[0]
                    
                    t ='CandidateContestSelectionJoin'
                    value_d = {'CandidateContest_Id':contest_id,'CandidateSelection_Id':selection_id}
                    upsert(cdf_schema,t,tables_d[t],value_d,con,cur)[0]

        
    return('</p><p>'.join(rs))

    


