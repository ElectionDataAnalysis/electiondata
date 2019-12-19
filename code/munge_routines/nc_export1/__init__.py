#!/usr/bin/python3
# under construction
#/munge_routines/nc_export1/__init__.py

import re
from munge_routines import get_upsert_id, format_type_for_insert, id_and_name_from_external, upsert

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
            
        

### load data from df to cdf; this should eventually be written in a munger-agnostic way, and also extended to all appropriate tables ***
import psycopg2
from psycopg2 import sql

def raw_to_cdf(df,cdf_schema,con,cur,d):        #e.g., d = {'ReportingUnit':{'North Carolina':59, 'North Carolina;Alamance County':61} ... }
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

    ###### get id for IdentifierType 'other'    *** inefficiency: no need to repeat for each datafile
    q = 'SELECT "Id" FROM {}."IdentifierType" WHERE txt = \'other\' '
    cur.execute(   sql.SQL(q).format( sql.Identifier(cdf_schema)))
    a = cur.fetchall()
    if a:
        id_type_other_id = a[0][0]
    else:
        return( 'No type \'other\' in table '+cdf_schema+'.IdentifierType')
    ###########################

    q = 'SELECT DISTINCT contest_name, vote_for, choice, choice_party FROM {}.{}'
    cur.execute(sql.SQL(q).format(sql.Identifier(df.state.schema_name),sql.Identifier(df.table_name)))
    contest_choice_pairs = cur.fetchall()
    
    ### create dictionaries for more efficient processing
    name_d = {}
    choice_d = {}
    name_choice_d = {}
    for [n,vf,ch,ch_p] in contest_choice_pairs:
        name_d[n]=vf
        choice_d[ch]=ch_p
        if n in name_choice_d.keys():
            name_choice_d[n].append(ch)
        else:
            name_choice_d[n] = [ch]
    ########
    
    for name in name_d.keys():     # loop over contests
        ## if contest is a ballot question (all choices are ballot measure selections)
        if all(x in ['Against','For','Yes','No'] for x in name_choice_d[name]):       ## *** store list of ballot measure selections with other enumerations? *** handle error if different yes/no text used?
            conflict_ds = [{'fieldname':'Name', 'datatype':'TEXT','value':name}]    ## *** munger info: value of conflict fields for each table
            other_ds = []           ## *** munger info: value of other fields for each table
            contest_id = get_upsert_id(cdf_schema, 'BallotMeasureContest',conflict_ds,other_ds,con,cur)[0]
            rs.append('Inserted Ballot Measure Contest '+name)
            
            ### insert BallotMeasureSelections and Join into cdf
            for ch in name_choice_d[name]:
                conflict_ds = [{'fieldname':'Selection', 'datatype':'TEXT','value':ch}]
                other_ds =  []
                selection_id = get_upsert_id(cdf_schema, 'BallotMeasureSelection',conflict_ds,other_ds,con,cur)[0]
                
                conflict_ds = [{'fieldname':'BallotMeasureContest_Id','datatype':'INT','value':contest_id},{'fieldname':'BallotMeasureSelection_Id','datatype':'INT','value':selection_id}]
                other_ds = []
                get_upsert_id(cdf_schema, 'BallotMeasureContestSelectionJoin',conflict_ds,other_ds,con,cur)[0]
        
            
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
                req_ds = [{'fieldname':'Name', 'datatype':'TEXT','value':office_name}]     # name the ReportingUnit for the election district after the office_name # *** fix hard-coding of 30 -- how to find ReportingUnitType_Id from the name of the office?
                ### *** try:   df.state.context_dictionary['Office'][office_name]['ElectionDistrictType'] to get ReportingUnitType; then find (id,txt) pair
                [id,txt]= format_type_for_insert(cdf_schema,'ReportingUnitType',df.state.context_dictionary['Office'][office_name]['ElectionDistrictType'] ,con,cur)
                other_ds = [{'fieldname':'ReportingUnitType_Id', 'datatype':'INTEGER','value':id},{'fieldname':'OtherReportingUnitType', 'datatype':'TEXT','value':txt}]
                election_district_id = get_upsert_id(cdf_schema,'ReportingUnit',req_ds,other_ds,con,cur)[0]
                req_ds = [{'fieldname':'Name', 'datatype':'TEXT','value':name}]
                other_ds = [{'fieldname':'VotesAllowed', 'datatype':'INTEGER','value':name_d[name]},{'fieldname':'Office_Id', 'datatype':'INTEGER','value':office_id},{'fieldname':'ElectionDistrict_Id', 'datatype':'INTEGER','value':election_district_id}]
                contest_id = get_upsert_id(cdf_schema, 'CandidateContest',req_ds,other_ds,con,cur)[0]
                rs.append('Inserted candidate contest ' + name)
                
                ### insert CandidateSelections and Join into cdf
                for ch in name_choice_d[name]:
                    
                    conflict_ds = [{'fieldname':'BallotName', 'datatype':'TEXT','value':ch},{'fieldname':'Election_Id', 'datatype':'INT','value':election_id},{'fieldname':'Party_Id', 'datatype':'INT','value':6465}]  # *** need to get actual Party_Id
                    other_ds = []
                    cand_id = get_upsert_id(cdf_schema,'Candidate',conflict_ds,other_ds,con,cur)[0]
                    conflict_ds = [{'fieldname':'Candidate_Id', 'datatype':'INT','value':cand_id}]
                    other_ds =  []
                    selection_id = get_upsert_id(cdf_schema, 'CandidateSelection',conflict_ds,other_ds,con,cur)[0]
                    
                    conflict_ds = [{'fieldname':'CandidateContest_Id','datatype':'INT','value':contest_id},{'fieldname':'CandidateSelection_Id','datatype':'INT','value':selection_id}]
                    other_ds = []
                    get_upsert_id(cdf_schema, 'CandidateContestSelectionJoin',conflict_ds,other_ds,con,cur)

        
    return('</p><p>'.join(rs))

    


