#!/usr/bin/python3
# munge_routines/__init__.py
# under construction

import db_routines as dbr
import sqlalchemy as db
from sqlalchemy import select, Integer, String, Date
from sqlalchemy.sql import column
import pandas as pd

def report_error(error_string):
    print('Munge error: '+error_string)

def spellcheck(session,value_d):
    corrected_value_d = {}
    spell_meta = db.MetaData(bind=session.bind, reflect=True, schema='misspellings')
    for key,value in value_d.items():
        cor = spell_meta.tables['misspellings.corrections']
        q = session.query(cor.c.good).filter(cor.c.bad == value)
        rp = session.execute(q)
        corrected_value_d[key] = rp.fetchone()[0]
    return corrected_value_d

def id_and_name_from_external(session,cdf_schema,meta,t,external_name,identifiertype_id,otheridentifiertype,internal_name_field='Name'):
    ## find the internal db name and id from external identifier

    ei_t = meta.tables[cdf_schema + '.ExternalIdentifier']
    j = ei_t.join(t,ei_t.c.ForeignId == t.c.Id)
    q = select([t.c.Id,t.c[internal_name_field]]).select_from(j).where(db.and_(ei_t.c.Value == external_name,ei_t.c.IdentifierType_Id == identifiertype_id,db.or_(ei_t.c.OtherIdentifierType == otheridentifiertype,ei_t.c.OtherIdentifierType == None)))
    rp = session.execute(q)
    if rp.rowcount > 0:
        return rp.fetchone()
    else:
        return(None,None)
            

def id_query_components(table_d,value_d):
    f_nt = [[dd['fieldname'], dd['datatype'] + ' %s'] for dd in table_d['fields']] + [[e + '_Id', 'INT %s'] for e in
                                                                                      table_d['enumerations']] + [
               ['Other' + e, 'TEXT %s'] for e in table_d['enumerations']] + [[dd['fieldname'], 'INT %s'] for dd in
                                                                             table_d[
                                                                                 'other_element_refs']]  # name-type pairs for each field
    # remove any fields missing from the value_d parameter
    good_f_nt = [x for x in f_nt if x[0] in value_d.keys()]

    f_names = [good_f_nt[x][0] for x in range(len(good_f_nt))]
    f_val_slot_list = [good_f_nt[x][1] for x in range(len(good_f_nt))]
    f_vals = [value_d[n] for n in f_names]

    cf_names = list(set().union(*table_d['unique_constraints']))
    f_id_slot_list = ['{' + str(i + 2) + '}' for i in range(len(f_names))]
    f_id_slots = ','.join(f_id_slot_list)
    if cf_names:
        cf_id_slots = ','.join(['{' + str(i + 2 + len(f_names)) + '}' for i in range(len(cf_names))])
        cf_query_string = ' ON CONFLICT (' + cf_id_slots + ') DO NOTHING '
    else:
        cf_query_string = ''
    f_val_slots = ','.join(f_val_slot_list)
    f_val_slots = f_val_slots.replace('INTEGER', '').replace('INT',
                                                             '')  # TODO kludge: postgres needs us to omit datatype for INTEGER, INT, not sure why. ***

    val_return_list = ['c.' + i for i in f_id_slot_list]
    return [f_id_slots,f_val_slots,cf_query_string,val_return_list,f_names,cf_names,f_vals]

def id_from_select_only(session,t,value_d, mode='no_dupes',check_spelling = True):
    """Returns the Id of the record in table with values given in the dictionary value_d.
    On error (nothing found, or more than one found) returns 0"""

    q = session.query(t).filter_by(**value_d)
    rp = session.execute(q)

    if rp.rowcount == 0: # if nothing returned
        # check misspellings.corrections table and try again. Set check_spelling = False to avoid needless repeats
        if check_spelling:
            try:
                corrected_value_d = spellcheck(session,value_d)
                return id_from_select_only(session,t, corrected_value_d, mode, check_spelling=False)
            except:
                report_error('No record found for this query:\n\t' + str(q) +'\n\t'+str(value_d))
                return 0
        else:
            report_error('No record found for this query:\n\t' + str(q) +'\n\t'+str(value_d))
            return 0
    elif rp.rowcount >1 and mode == 'no_dupes':
        report_error('More than one record found for this query:\n\t'+dbr.query_as_string(a,sql_ids,strs,con,cur))
        return 0
    else:
        return rp.fetchone()[0]

def id_from_select_or_insert(session,t, value_d, mode='no_dupes'):
    """ t is a table from the metadata; value_d gives the values for the fields in the table
    (.e.g., value_d['Name'] = 'North Carolina;Alamance County'); return the upserted record.
    E.g., tables_d[table] = {'tablename':'ReportingUnit', 'fields':[{'fieldname':'Name','datatype':'TEXT'}],
    'enumerations':['ReportingUnitType','CountItemStatus'],'other_element_refs':[], 'unique_constraints':[['Name']],
    'not_null_fields':['ReportingUnitType_Id']
    modes with consequences: 'dupes_ok'
       } """

    s = session.query(t).filter_by(**value_d)
    rp = session.execute(s)
    if rp.rowcount == 0:
        ins = t.insert().values(**value_d)
        rp2 = session.execute(ins)
        id = rp2.inserted_primary_key[0]
        session.commit()
    else:
        id = rp.fetchone()[0]
    return(id)

def composing_from_reporting_unit_name(session,meta,cdf_schema,name,id=0):
    # insert all ComposingReportingUnit joins that can be deduced from the internal db name of the ReportingUnit
    # Use the ; convention to identify all parents

    ru_d = {'fields':[{'fieldname':'Name','datatype':'TEXT'}],
                     'enumerations':['ReportingUnitType','CountItemStatus'],
                     'other_element_refs':[],
                     'unique_constraints':[['Name']],
		     'not_null_fields':['ReportingUnitType_Id']} # TODO avoid hard-coding this in various places
    cruj_d = {'fields':[],
                      'enumerations':[],
                      'other_element_refs':[{'fieldname':'ParentReportingUnit_Id', 'refers_to':'ReportingUnit'}, {'fieldname':'ChildReportingUnit_Id', 'refers_to':'ReportingUnit'}],
                      'unique_constraints':[['ParentReportingUnit_Id','ChildReportingUnit_Id']],
		     'not_null_fields':['ParentReportingUnit_Id','ChildReportingUnit_Id']} # TODO avoid hard-coding this in various places
    # if no id is passed, find id corresponding to name
    if id == 0:
        id = id_from_select_or_insert(session,meta,meta.tables[cdf_schema + '.ReportingUnit'], {'Name': name})
    chain = name.split(';')
    for i in range(1,len(chain)+1):
        parent = ';'.join(chain[0:i])
        parent_id = id_from_select_only(session, meta.tables[cdf_schema+ '.ReportingUnit'], {'Name': parent})
        id_from_select_or_insert(session,meta.tables[cdf_schema + '.ComposingReportingUnitJoin'],
                                 {'ParentReportingUnit_Id': parent_id, 'ChildReportingUnit_Id': id})
    return

def format_type_for_insert(session,e_table,txt):
    """This is designed for enumeration tables. e_table is a metadata object, and must have an "Id" field and a "Txt" field.
    This function returns a (type_id, othertype_text) pair; for types in the enumeration, returns (type_id for the given txt, ""),
    while for other types returns (type_id for "other",txt) """
    s = select([e_table.c.Id]).where(e_table.c.Txt == txt)
    ResultProxy = session.execute(s)
    assert ResultProxy.rowcount < 2,'Cannot recover from duplicate value of '+ txt + ' in table ' + str(e_table)
    if ResultProxy.rowcount == 1:   # TODO prevent multiple fillings of *Type tables, which yield rowcounts > 1
        return([ResultProxy.fetchone()[0],''])
    elif ResultProxy.rowcount == 0:
        other_s = select([e_table.c.Id]).where(e_table.c.Txt == 'other')
        OtherResultProxy = session.execute(other_s)
        assert OtherResultProxy.rowcount == 1, 'The Common Data Format  does not allow the option ' + txt + ' for table ' + e_table.fullname
        return[OtherResultProxy.fetchone()[0],txt]

def raw_records_to_cdf(session,meta,df,mu,cdf_schema,state_id = 0,id_type_other_id = 0):
    """ munger-agnostic raw-to-cdf script; ***
    df is datafile, mu is munger """
    #%% get id for IdentifierType 'other' if it was not passed as parameter
    if id_type_other_id == 0:
        a = dbr.read_id_from_enum(session,meta,cdf_schema,'IdentifierType','other')
        if a:
            id_type_other_id = a
        else:
            raise Exception('No Id found for IdentifierType \'other\'; fix IdentifierType table and rerun.')
    with open('CDF_schema_def_info/tables.txt', 'r') as f:
        table_def_list = eval(f.read())
    tables_d = {}
    for table_def in table_def_list:
        tables_d[table_def[0]] = table_def[1]

    #%% get id for  election
    [electiontype_id, otherelectiontype] = format_type_for_insert(session,meta.tables[meta.schema + '.ElectionType'], df.state.context_dictionary['Election'][df.election]['ElectionType'])
    value_d = {'Name': df.election, 'EndDate': df.state.context_dictionary['Election'][df.election]['EndDate'],
               'StartDate': df.state.context_dictionary['Election'][df.election]['StartDate'],
               'OtherElectionType': otherelectiontype, 'ElectionType_Id': electiontype_id}
    election_id = id_from_select_or_insert(session,meta.tables[cdf_schema + '.Election'], value_d)

    # if state_id is not passed as parameter, select-or-insert state, get id (default Reporting Unit for ballot questions)
    if state_id == 0:
        t = 'ReportingUnit'
        [reportingunittype_id, otherreportingunittype] = format_type_for_insert(session, meta.tables[meta.schema + '.ReportingUnitType'],
                                                                                'state')
        value_d = {'Name': df.state.name, 'ReportingUnitType_Id': reportingunittype_id,
                   'OtherReportingUnitType': otherreportingunittype}
        state_id = id_from_select_or_insert(session, meta.tables[cdf_schema + '.' + t], value_d)

    # store state_id and election_id for later use
    ids_d = {'state': state_id, 'Election_Id': election_id}  # to hold ids of found items for later reference

    # get BallotMeasureSelection dict {Selection:Id} from cdf schema
    bms_dframe = pd.read_sql_table('BallotMeasureSelection',session.bind,schema=cdf_schema,index_col='Selection')
    ballot_measure_selections = bms_dframe.T.to_dict('records')[0]

    munger_raw_cols = mu.content_dictionary['raw_cols'] # TODO is this used?
    # create dictionaries for processing data from rows. Not all CDF elements are included. E.g., 'Election' element is not filled from df rows, but from df.election

    munger_counts_d = mu.content_dictionary['counts_dictionary'] # TODO is this used?
    # look up id,type pairs for each kind of count, add info to counts dictionary
    for ct,dic in munger_counts_d.items():
        text = dic['CountItemType']
        [dic['CountItemType_Id'], dic['OtherCountItemType']] = format_type_for_insert(session,meta.tables[meta.schema + '.CountItemType'],text)
    munger_fields_d = mu.content_dictionary['fields_dictionary']
    # TODO is the above used?

    raw_rows = pd.read_sql_table(df.table_name,session.bind,schema=df.state.schema_name)
    for index,row in raw_rows.iterrows():   # loop over content rows of dataframe
        # track progress
        if index % 5000 == 0:  # for every five-thousandth row
            print('\t\tProcessing item number ' + str(index) + ': ' + str(row))

        for i in range(len(munger_raw_cols)):
            var_name_str = munger_raw_cols[i][0].replace(' ','_')
            if row[i]!= 0 and not row[i]:   # if db query returned None     # awkward ***
                exec(var_name_str + ' = "" ')
            elif munger_raw_cols[i][1] == Integer:
                exec(var_name_str + ' = ' + str(row[i]) )
            else:   # *** DATE and TEXT handled identically
                exec(var_name_str + ' = "'+ row[i] +'"')

        # Process all straight-forward elements into cdf and capture id in ids_d
        for t in ['ReportingUnit', 'Party']:  # TODO list may be munger-dependent
            for item in munger_fields_d[t]:  # for each case (most elts have only one case, but ReportingUnit has more) e.g. item = {'ExternalIdentifier': county,
                # 'Enumerations':{'ReportingUnitType': 'county'}}.
                # Cases must be mutually exclusive and exhaustive
                # following if must allow '' as a value (e.g., because Party can be the empty string), but not None or []
                if (eval(item['ExternalIdentifier']) == '' or eval(item['ExternalIdentifier'])) and eval(item['Condition']):
                    # get internal db name and id for ExternalIdentifier from the info in the df row ...
                    [cdf_id, cdf_name] = id_and_name_from_external(session,cdf_schema,meta,meta.tables[cdf_schema + '.'+ t], eval(item['ExternalIdentifier']), id_type_other_id, mu.name, item['InternalNameField'])
                    # ... or if no such is found in db, insert it!
                    if [cdf_id, cdf_name] == [None, None]:
                        cdf_name = eval(item['ExternalIdentifier'])
                        value_d = {item['InternalNameField']: cdf_name}  # usually 'Name' but not always
                        for e in item['Enumerations'].keys():  # e.g. e = 'ReportingUnitType'
                            [value_d[e + '_Id'], value_d['Other' + e]] = format_type_for_insert(session,meta.tables[meta.schema + '.' + e], item['Enumerations'][e])
                        for f in item['OtherFields'].keys():
                            value_d[f] = eval(item['OtherFields'][f])
                        if t == 'CandidateContest' or t == 'BallotMeasureContest':  # need to get ElectionDistrict_Id from contextual knowledge
                            value_d['ElectionDistrict_Id'] = ids_d['contest_reporting_unit_id']
                        cdf_id = id_from_select_or_insert(session,meta.tables[cdf_schema + '.' + t],  value_d)

                        # if newly inserted item is a ReportingUnit, insert all ComposingReportingUnit joins that can be deduced from the internal db name of the ReportingUnit
                        if t == 'ReportingUnit':
                            composing_from_reporting_unit_name(session, meta,cdf_schema,cdf_name,cdf_id)
            ids_d[t + '_Id'] = cdf_id

        # process Ballot Measures and Candidate Contests into CDF and capture ids into ids_d (depends on values in row):
        selection = eval(munger_fields_d['BallotMeasureSelection'][0]['ExternalIdentifier'])
        # *** cond = munger_fields_d['BallotMeasureSelection']['ExternalIdentifier'] +' in ballot_measure_selections.keys()'

        #%% fill Selection and Contest tables -- and for Candidate Contest, Candidate too -- and all associated joins.
        if selection in ballot_measure_selections.keys() :     # if selection is a Ballot Measure selection, assume contest is a Ballot Measure
            ids_d['selection_id'] = ballot_measure_selections[selection]
            # fill BallotMeasureContest
            value_d = {'Name':eval(munger_fields_d['BallotMeasureContest'][0]['ExternalIdentifier']),'ElectionDistrict_Id':state_id}  # all ballot measures are assumed to be state-level ***
            ids_d['contest_id'] = id_from_select_or_insert(session,meta.tables[cdf_schema + '.BallotMeasureContest'],  value_d)
            # fill BallotMeasureContestSelectionJoin ***
            value_d = {'BallotMeasureContest_Id':ids_d['contest_id'],'BallotMeasureSelection_Id':ids_d['selection_id']}
            id_from_select_or_insert(session,meta.tables[cdf_schema + '.BallotMeasureContestSelectionJoin'], value_d)

        else:       # if not a Ballot Measure (i.e., if a Candidate Contest)
            raw_office_name = eval(munger_fields_d['Office'][0]['ExternalIdentifier'])

            foreignid = id_from_select_only(session,meta.tables[cdf_schema+'.ExternalIdentifier'],{'IdentifierType_Id':id_type_other_id,'Value':raw_office_name,'OtherIdentifierType':mu.name})
            a = id_from_select_only(session,meta.tables[cdf_schema + '.Office'],{'ForeignId':foreignid})

            #qq = 'SELECT f."Id", f."Name" FROM {0}."ExternalIdentifier" AS e LEFT JOIN {0}."Office" AS f ON e."ForeignId" = f."Id" WHERE e."IdentifierType_Id" = %s AND e."Value" =  %s AND e."OtherIdentifierType" = %s;'
            # a = dbr.query(qq,[cdf_schema],[id_type_other_id, raw_office_name,mu.name],con,cur)
            if not a: # if Office is not already associated to the munger in the db (from state's context_dictionary, for example), skip this row
               continue
            ids_d['Office_Id'] = a[0][0]

            # Find Id for ReportingUnit for contest via context_dictionary['Office']
            # find reporting unit associated to contest (not reporting unit associated to df row)
            election_district_name = df.state.context_dictionary['Office'][a[0][1]]['ElectionDistrict']
            qq = 'SELECT "Id" FROM {0}."ReportingUnit" WHERE "Name" = %s'
            b = dbr.query(qq,[cdf_schema],[election_district_name,],con,cur)
            election_district_id = b[0][0]

            # insert into CandidateContest table
            votes_allowed = eval(munger_fields_d['CandidateContest'][0]['OtherFields']['VotesAllowed']) # TODO  misses other fields e.g. NumberElected
            value_d = {'Name':election_district_name,'ElectionDistrict_Id':election_district_id,'Office_Id':ids_d['Office_Id'],'VotesAllowed':votes_allowed}
            ids_d['contest_id'] = id_from_select_or_insert(session,meta.tables[cdf_schema + '.CandidateContest'], value_d)

            # insert into Candidate table
            ballot_name = eval(munger_fields_d['Candidate'][0]['ExternalIdentifier'])
            value_d = {'BallotName':ballot_name,'Election_Id':election_id,'Party_Id':ids_d['Party_Id']}
            ids_d['Candidate_Id'] = id_from_select_or_insert(session,meta,cdf_schema, 'Candidate', tables_d['Candidate'], value_d, con, cur)[0]

            # insert into CandidateSelection
            value_d = {'Candidate_Id':ids_d['Candidate_Id']}
            ids_d['selection_id'] = id_from_select_or_insert(session,meta.tables[cdf_schema +'.CandidateSelection'], value_d)

            # create record in CandidateContestSelectionJoin
            value_d = {'CandidateContest_Id':ids_d['contest_id'],
                       'CandidateSelection_Id': ids_d['selection_id'],
                       'Election_Id':election_id}
            id_from_select_or_insert(session,meta.tables[cdf_schema + '.CandidateContestSelectionJoin'], value_d)

        # fill ElectionContestJoin
        value_d = {'Election_Id': election_id, 'Contest_Id': ids_d['contest_id']}
        id_from_select_or_insert(session,meta.tables[cdf_schema + '.ElectionContestJoin'],  value_d)

        # process vote counts in row
        for ct,dic in munger_counts_d.items():
            value_d = {'Count':eval(ct),'ReportingUnit_Id':ids_d['ReportingUnit_Id'],'CountItemType_Id': dic['CountItemType_Id'],'OtherCountItemType':dic['OtherCountItemType']}
            # TODO dupes are a problem only when contest & reporting unit are specified.
            ids_d['VoteCount_Id']=id_from_select_or_insert(session,meta.tables[cdf_schema + '.VoteCount'], value_d,  'dupes_ok')

            # fill SelectionElectionContestVoteCountJoin
            value_d = {'Selection_Id':ids_d['selection_id'],'Contest_Id':ids_d['contest_id'],'Election_Id':ids_d['Election_Id'],'VoteCount_Id':ids_d['VoteCount_Id']}
            id_from_select_or_insert(session,meta.tables[cdf_schema + '.SelectionElectionContestVoteCountJoin'],  value_d)

        con.commit()
    return str(ids_d)

if __name__ == '__main__':
    import db_routines.Create_CDF_db as CDF
    from sqlalchemy.orm import sessionmaker

    cdf_schema='cdf_xx_test'
    eng,meta = dbr.sql_alchemy_connect(schema=cdf_schema,paramfile='../../local_data/database.ini')
    Session = sessionmaker(bind=eng)
    session = Session()

    external_name ='ALAMANCE;064'
    t = meta.tables[cdf_schema + '.ReportingUnit']
    identifiertype_id = 35
    otheridentifiertype = 'nc_export1'
    [office_id, office_name] = id_and_name_from_external(session, cdf_schema, meta, t, external_name, identifiertype_id, otheridentifiertype, internal_name_field='Name')

    print('Done!')

