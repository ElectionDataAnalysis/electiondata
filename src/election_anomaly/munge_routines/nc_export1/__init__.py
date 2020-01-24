#!/usr/bin/python3
# under construction
#/munge_routines/nc_export1/__init__.py

import re
import db_routines as dbr
from munge_routines import format_type_for_insert, id_and_name_from_external, id_from_select_or_insert,composing_from_reporting_unit_name

def office_key(state_name,contest_name,term_string):
    """create external identifier / nc_export1 pairs of offices dictionary"""
    p_congressional = re.compile('^US HOUSE OF REPRESENTATIVES DISTRICT (?P<district>[\d])+$')
    m = p_congressional.search(contest_name)
    if m:
        return(state_name+';US Congress;House of Representatives;' + term_string + ';District ' + str(eval(m.group('district'))))
    
    else:
        return('no match')

def add_ei(office_d):
    """given key, add external identifier to dictionary for certain offices"""
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
                    [value_d[e + 'Id'], value_d['Other' + e]] = format_type_for_insert(session, e,
                                                                                       item[
                                                                                           'Enumerations'][
                                                                                           e])
                for f in item['OtherFields'].keys():
                    value_d[f] = eval(item['OtherFields'][f])
                if t == 'CandidateContest' or t == 'BallotMeasureContest':  # need to get ElectionDistrict_Id from contextual knowledge
                    value_d['ElectionDistrict_Id'] = ids_d['contest_reporting_unit_id']
                cdf_id = id_from_select_or_insert(session,meta.tables[cdf_schema + '.' + t],  value_d)
        ids_d[t + '_Id'] = cdf_id
        return(ids_d)


    


