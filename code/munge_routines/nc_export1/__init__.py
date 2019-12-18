#!/usr/bin/python3
# under construction
#/munge_routines/nc_export1/__init__.py

import re
from munge_routines import get_upsert_id

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
            
        

### load CandidateContest data from df to cdf; this should eventually be written in a munger-agnostic way, and also extended to all appropriate tables ***
import psycopg2
from psycopg2 import sql

def raw_to_cdf(df,cdf_schema,con,cur,d):        #e.g., d = {'ReportingUnit':{'North Carolina':59, 'North Carolina;Alamance County':61} ... }
    rs = ['raw_to_cdf:']
    
    ## get id for IdentifierType 'other'    *** inefficient to do this repeatedly
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
    
    ### create dictionaries for more efficient parsing
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
            req_d = {'fieldname':'Name', 'datatype':'TEXT','value':name}    ## *** munger info: value of required fields for each table
            other_ds = []           ## *** munger info: value of other fields for each table
            contest_id = get_upsert_id(cdf_schema, 'BallotMeasureContest',req_d,other_ds,con,cur)
            rs.append('Inserted Ballot Measure Contest '+name)
            ### insert BallotMeasureSelections and Join into cdf
            for ch in name_choice_d[name]:
                req_d = {'fieldname':'Selection', 'datatype':'TEXT','value':ch}
                other_ds =  {}
                selection_id = get_upsert_id(cdf_schema, 'BallotMeasureSelection',req_d,other_ds,con,cur)
                
                ### *** TO DO: insert join into BallotMeasureContestSelectionJoin table.
        
            
        else:
            ## find the corresponding office (internal db name)
            q = 'SELECT f."Id", f."Name" FROM {0}."ExternalIdentifier" AS e LEFT JOIN {0}."Office" AS f ON e."ForeignId" = f."Id" WHERE e."IdentifierType_Id" = %s AND e."Value" =  %s AND e."OtherIdentifierType" = \'nc_export1\';'
            cur.execute(sql.SQL(q).format(sql.Identifier(cdf_schema)),[id_type_other_id,name])
            a = cur.fetchall()
            rs.append(str(a))
            if a:
                [office_id,office_name] = a[0]
                rs.append(str(office_name))
            #if office_name is not None -- this will entirely skip offices not listed in the state's context_dictionary!
                req_d = {'fieldname':'Name', 'datatype':'TEXT','value':office_name}     # name the ReportingUnit for the election district after the office_name
                other_ds = [{'fieldname':'ReportingUnitType_Id', 'datatype':'INTEGER','value':30}] # *** fix hard-coding of 30
                election_district_id = get_upsert_id(cdf_schema,'ReportingUnit',req_d,[],con,cur)[0]
                req_d = {'fieldname':'Name', 'datatype':'TEXT','value':name}
                other_ds = [{'fieldname':'VotesAllowed', 'datatype':'INTEGER','value':vote_for},{'fieldname':'Office_Id', 'datatype':'INTEGER','value':office_id},{'fieldname':'ElectionDistrict_Id', 'datatype':'INTEGER','value':election_district_id}]
                contest_id = get_upsert_id(cdf_schema, 'CandidateContest',req_d,other_ds,con,cur)
                rs.append('Inserted candidate contest ' + name)
        
        
    return('</p><p>'.join(rs))

    


