#!/usr/bin/python3
# under construction
#/munge_routines/nc_export1/__init__.py

import re

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

def raw_to_cdf(df,schema,con,cur,d):        #e.g., d = {'ReportingUnit':{'North Carolina':59, 'North Carolina;Alamance County':61} ... }
    q = 'SELECT DISTINCT contest_name, vote_for FROM {}.{}'
    cur.execute(sql.SQL(q).format(sql.Identifier(schema),sql.Identifier(df.table_name)))
    contests = cur.fetchall()
    for c in contests:
        contest_name = c[0]
        if contest_name in d['Office'].keys():
            vote_for = c[1]
            office_id = d['Office'][contest_name]
            election_district = s.context_dictionary['Office']['ElectionDistrict']
            ## should be able to upsert contest now....
            b = 1/0 # ***
        
    return('Contest name is '+contest_name+'; vote for '+str(vote_for))

    


