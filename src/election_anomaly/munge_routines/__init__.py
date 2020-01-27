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

def id_and_name_from_external_PANDAS(ei_dframe, t_dframe, external_name, identifiertype_id, otheridentifiertype, internal_name_field='Name'):
    ## find the internal db name and id from external identifier

    ei_value_d = {'Value':external_name,'IdentifierType_Id':identifiertype_id,'OtherIdentifierType':otheridentifiertype}
    ei_filtered = ei_dframe.loc[(ei_dframe[list(ei_value_d)] == pd.Series(ei_value_d)).all(axis=1)]
    if ei_filtered.shape[0] == 1:
        id = ei_filtered['ForeignId'].to_list()[0]
        name = t_dframe.loc[id][internal_name_field]
        return id, name
    elif ei_filtered.shape[0] > 1:
        raise Exception('\n\t'.join['Unexpected duplicates found for','external_name: '+external_name, 'identifiertype_id: '+str(identifiertype_id),'otheridentifiertype: '+otheridentifiertype])
    else:
        return None, None

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

def id_from_select_only_PANDAS(dframe,value_d, mode='no_dupes',check_spelling = True):
    """Returns the Id of the record in table with values given in the dictionary value_d.
    On error (nothing found, or more than one found) returns 0"""

    # filter the dframe by the value_d conditions
    filtered_dframe = dframe.loc[(dframe[list(value_d)] == pd.Series(value_d)).all(axis=1)]

    if filtered_dframe.shape[0] == 0: # if no rows meet value_d criteria
        # check misspellings.corrections table and try again. Set check_spelling = False to avoid needless repeats
        if check_spelling:
            try:
                corrected_value_d = spellcheck(session,value_d)
                return id_from_select_only_PANDAS(dframe, corrected_value_d, mode, check_spelling=False)
            except:
                return 0
        else:
            return 0
    elif rp.rowcount >1 and mode == 'no_dupes':
        raise Exception('More than one record found for these values:\n\t'+str(value_d))
    else:
        return filtered_dframe.index.to_list()[0]

def id_from_select_or_insert(session,t, value_d, mode='no_dupes'):
    """ cdf_table is a table from the metadata; value_d gives the values for the fields in the table
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

def id_from_select_or_insert_PANDAS(dframe, value_d, mode='no_dupes'):
    """  value_d gives the values for the fields in the dataframe.
    If there is a corresponding record in the table, return the id
    If there is no corresponding record, insert one and return the id.
    (.e.g., value_d['Name'] = 'North Carolina;Alamance County');
    E.g., tables_d[table] = {'tablename':'ReportingUnit', 'fields':[{'fieldname':'Name','datatype':'TEXT'}],
    'enumerations':['ReportingUnitType','CountItemStatus'],'other_element_refs':[], 'unique_constraints':[['Name']],
    'not_null_fields':['ReportingUnitType_Id']
    modes with consequences: 'dupes_ok'
       } """
    # filter the dframe by the value_d conditions
    filtered_dframe = dframe.loc[(dframe[list(value_d)] == pd.Series(value_d)).all(axis=1)]

    if mode == 'no_dupes' and filtered_dframe.shape[0] > 1:
        raise Exception('Duplicate values found for ' + str(value_d))
    if filtered_dframe.shape[0] == 0:   # insert row if it's not already there
        filtered_dframe = filtered_dframe.append(value_d, ignore_index=True)
        dframe = dframe.append(value_d, ignore_index=True) # TODO check: does this alter dframe globally?

    assert filtered_dframe.shape[0] == 1, 'filtered dataframe does not have exactly one row'
    id = filtered_dframe.index.to_list()[0]
    return id, dframe

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

def composing_from_reporting_unit_name_PANDAS(ru_dframe,cruj_dframe,name,id=0):
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
        id, ru_dframe = id_from_select_or_insert_PANDAS(ru_dframe, {'Name': name})
    chain = name.split(';')
    for i in range(1,len(chain)+1):
        parent = ';'.join(chain[0:i])
        parent_id = id_from_select_only_PANDAS(ru_dframe, {'Name': parent})
        id, cruj_dframe = id_from_select_or_insert_PANDAS(cruj_dframe, {'ParentReportingUnit_Id': parent_id, 'ChildReportingUnit_Id': id})
    return cruj_dframe

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

def format_type_for_insert_PANDAS(dframe,txt,id_type_other_id):
    """This is designed for enumeration dframes, which must have an "Id" field and a "Txt" field.
    id_type_other_id is the id for 'other' IdentifierType
    This function returns a (type_id, othertype_text) pair; for types in the enumeration, returns (type_id for the given txt, ""),
    while for other types returns (type_id for "other",txt) """
    # TODO check that dframe columns are 'Id' and 'Txt'
    id_list = dframe.index[dframe['Txt'] == txt].to_list()
    if len(id_list) == 1:   # TODO prevent multiple fillings of *Type tables, which yield rowcounts > 1
        return([id_list[0],''])
    elif len(id_list) == 0:
        return[id_type_other_id,txt]
    else:
         raise Exception('Dataframe has duplicate rows with value ' + txt + ' in Txt column')

def raw_records_to_cdf(session,meta,df,mu,cdf_schema,state_id = 0,id_type_other_id = 0,cdf_table_filepath='CDF_schema_def_info/tables.txt'):
    """ munger-agnostic raw-to-cdf script; ***
    df is datafile, mu is munger """
    # TODO why did the dframe creation loop fail?
    #for tbl in ['ElectionType','CountItemType','ReportingUnitType','Election','ReportingUnit','Party','CandidateContest','Candidate','CandidateSelection','BallotMeasureContestSelectionJoin','ElectionContestJoin','ExternalIdentifier','BallotMeasureSelection']:
        #dframe_creation_string = tbl+'_dframe = pd.read_sql_table(\'' + tbl + '\',session.bind, cdf_schema, index_col=\'Id\')'
        #exec(dframe_creation_string)
        #print (dframe_creation_string)

    ElectionType_dframe = pd.read_sql_table('ElectionType', session.bind, cdf_schema, index_col='Id')
    CountItemType_dframe = pd.read_sql_table('CountItemType', session.bind, cdf_schema, index_col='Id')
    ReportingUnitType_dframe = pd.read_sql_table('ReportingUnitType', session.bind, cdf_schema, index_col='Id')
    Election_dframe = pd.read_sql_table('Election', session.bind, cdf_schema, index_col='Id')
    ReportingUnit_dframe = pd.read_sql_table('ReportingUnit', session.bind, cdf_schema, index_col='Id')
    Party_dframe = pd.read_sql_table('Party', session.bind, cdf_schema, index_col='Id')
    CandidateContest_dframe = pd.read_sql_table('CandidateContest', session.bind, cdf_schema, index_col='Id')
    Candidate_dframe = pd.read_sql_table('Candidate', session.bind, cdf_schema, index_col='Id')
    CandidateSelection_dframe = pd.read_sql_table('CandidateSelection', session.bind, cdf_schema, index_col='Id')
    BallotMeasureContest_dframe = pd.read_sql_table('BallotMeasureContest', session.bind, cdf_schema, index_col='Id')
    VoteCount_dframe = pd.read_sql_table('VoteCount', session.bind, cdf_schema, index_col='Id')
    SelectionElectionContestVoteCountJoin_dframe = pd.read_sql_table('SelectionElectionContestVoteCountJoin', session.bind, cdf_schema, index_col='Id')

    BallotMeasureContestSelectionJoin_dframe = pd.read_sql_table('BallotMeasureContestSelectionJoin', session.bind, cdf_schema, index_col='Id')

    ElectionContestJoin_dframe = pd.read_sql_table('ElectionContestJoin', session.bind, cdf_schema, index_col='Id')
    CandidateContestSelectionJoin_dframe = pd.read_sql_table('CandidateContestSelectionJoin', session.bind, cdf_schema, index_col='Id')

    ComposingReportingUnitJoin_dframe = pd.read_sql_table('ComposingReportingUnitJoin', session.bind, cdf_schema, index_col='Id')

    ExternalIdentifier_dframe = pd.read_sql_table('ExternalIdentifier', session.bind, cdf_schema, index_col='Id')

    BallotMeasureSelection_dframe = pd.read_sql_table('BallotMeasureSelection', session.bind, schema=cdf_schema,
                                                      index_col='Selection')

    #%% get id for IdentifierType 'other' if it was not passed as parameter
    if id_type_other_id == 0:
        IdentifierType_dframe = pd.read_sql_table('IdentifierType', session.bind, cdf_schema, index_col='Id')
        id_type_other_id = IdentifierType_dframe.index[IdentifierType_dframe['Txt'] == 'other'].to_list()[0]
        if not id_type_other_id:
            raise Exception('No Id found for IdentifierType \'other\'; fix IdentifierType table and rerun.')
    with open(cdf_table_filepath, 'r') as f:
        table_def_list = eval(f.read())
    tables_d = {}
    for table_def in table_def_list:
        tables_d[table_def[0]] = table_def[1]

    #%% get id for  election
    [electiontype_id, otherelectiontype] = format_type_for_insert_PANDAS(ElectionType_dframe,df.state.context_dictionary['Election'][df.election]['ElectionType'],id_type_other_id)
    value_d = {'Name': df.election, 'EndDate': df.state.context_dictionary['Election'][df.election]['EndDate'],
               'StartDate': df.state.context_dictionary['Election'][df.election]['StartDate'],
               'OtherElectionType': otherelectiontype, 'ElectionType_Id': electiontype_id}
    election_id, Election_dframe = id_from_select_or_insert_PANDAS(Election_dframe, value_d)

    # if state_id is not passed as parameter, select-or-insert state, get id (default Reporting Unit for ballot questions)
    if state_id == 0:
        [reportingunittype_id, otherreportingunittype] = format_type_for_insert_PANDAS(ReportingUnitType_dframe, 'state',id_type_other_id)
        value_d = {'Name': df.state.name, 'ReportingUnitType_Id': reportingunittype_id,
                   'OtherReportingUnitType': otherreportingunittype}
        state_id, ReportingUnit_dframe = id_from_select_or_insert_PANDAS(ReportingUnit_dframe, value_d)

    # store state_id and election_id for later use
    ids_d = {'state': state_id, 'Election_Id': election_id}  # to hold ids of found items for later reference


    munger_raw_cols = mu.content_dictionary['raw_cols'] # TODO is this used?
    # create dictionaries for processing data from rows. Not all CDF elements are included. E.g., 'Election' element is not filled from df rows, but from df.election

    munger_counts_d = mu.content_dictionary['counts_dictionary'] # TODO is this used?
    # look up id,type pairs for each kind of count, add info to counts dictionary
    for ct,dic in munger_counts_d.items():
        text = dic['CountItemType']
        [dic['CountItemType_Id'], dic['OtherCountItemType']] = format_type_for_insert_PANDAS(CountItemType_dframe,text,id_type_other_id)
    munger_fields_d = mu.content_dictionary['fields_dictionary']
    # TODO is the above used?

    raw_rows = pd.read_sql_table(df.table_name,session.bind,schema=df.state.schema_name)

    for index,row in raw_rows.iterrows():   # loop over content rows of dataframe
        # track progress
        if index % 5000 == 0:  # for every five-thousandth row
            print('\t\tProcessing item number ' + str(index) + ': ' + str(row))
        # Process all straight-forward elements into cdf and capture id in ids_d
        for t in ['ReportingUnit', 'Party']:  # TODO list may be munger-dependent
            for item in munger_fields_d[t]:  # for each case (most elts have only one case, but, e.g., ReportingUnit may have more) e.g. item = {'ExternalIdentifier': county,
                # 'Enumerations':{'ReportingUnitType': 'county'}}.
                # Cases must be mutually exclusive and exhaustive
                # following if must allow '' as a value (e.g., because Party can be the empty string), but not None or []
                if (eval(item['ExternalIdentifier']) == '' or eval(item['ExternalIdentifier'])) and eval(item['Condition']):
                    # get internal db name and id for ExternalIdentifier from the info in the df row ...
                    [cdf_id, cdf_name] = id_and_name_from_external_PANDAS(ExternalIdentifier_dframe,eval(t+'_dframe'), eval(item['ExternalIdentifier']), id_type_other_id, mu.name, item['InternalNameField'])
                    # ... or if no such is found in db, insert it!
                    if [cdf_id, cdf_name] == [None, None]:
                        cdf_name = eval(item['ExternalIdentifier'])
                        if cdf_name == '':
                            cdf_id = None
                        else:
                            value_d = {item['InternalNameField']: cdf_name}  # usually 'Name' but not always
                            for e in item['Enumerations'].keys():  # e.g. e = 'ReportingUnitType'
                                [value_d[e + '_Id'], value_d['Other' + e]] = format_type_for_insert(session,meta.tables[meta.schema + '.' + e], item['Enumerations'][e])
                            for f in item['OtherFields'].keys():
                                value_d[f] = eval(item['OtherFields'][f])
                            if t == 'CandidateContest' or t == 'BallotMeasureContest':  # need to get ElectionDistrict_Id from contextual knowledge
                                value_d['ElectionDistrict_Id'] = ids_d['contest_reporting_unit_id']
                            exec('cdf_id,' + t + '_dframe  = id_from_select_or_insert_PANDAS(' + t+'_dframe,  value_d)')

                        # if newly inserted item is a ReportingUnit, insert all ComposingReportingUnit joins that can be deduced from the internal db name of the ReportingUnit
                        if t == 'ReportingUnit':
                            ComposingReportingUnitJoin_dframe = composing_from_reporting_unit_name_PANDAS(ReportingUnit_dframe,ComposingReportingUnitJoin_dframe,cdf_name,cdf_id)
            ids_d[t + '_Id'] = cdf_id

        # process Ballot Measures and Candidate Contests into CDF and capture ids into ids_d (depends on values in row):
        selection = eval(munger_fields_d['BallotMeasureSelection'][0]['ExternalIdentifier'])
        # *** cond = munger_fields_d['BallotMeasureSelection']['ExternalIdentifier'] +' in BallotMeasureSelection_dict.keys()'

        #%% fill Selection and Contest tables -- and for Candidate Contest, Candidate too -- and all associated joins.
        if selection in BallotMeasureSelection_dframe.index:     # if selection is a Ballot Measure selection, assume contest is a Ballot Measure
            ids_d['selection_id'] = BallotMeasureSelection_dframe.loc[selection]['Id']
            # fill BallotMeasureContest
            value_d = {'Name':eval(munger_fields_d['BallotMeasureContest'][0]['ExternalIdentifier']),'ElectionDistrict_Id':state_id}  # all ballot measures are assumed to be state-level ***
            ids_d['contest_id'], BallotMeasureContest_dframe = id_from_select_or_insert_PANDAS(BallotMeasureContest_dframe,  value_d)
            # fill BallotMeasureContestSelectionJoin ***
            value_d = {'BallotMeasureContest_Id':ids_d['contest_id'],'BallotMeasureSelection_Id':ids_d['selection_id']}
            id_from_select_or_insert_PANDAS(BallotMeasureContestSelectionJoin_dframe, value_d)

        else:       # if not a Ballot Measure (i.e., if a Candidate Contest)
            raw_office_name = eval(munger_fields_d['Office'][0]['ExternalIdentifier'])

            a = id_from_select_only_PANDAS(ExternalIdentifier_dframe,{'IdentifierType_Id':id_type_other_id,'Value':raw_office_name,'OtherIdentifierType':mu.name})

            if not a: # if Office is not already associated to the munger in the db (from state's context_dictionary, for example), skip this row
               continue
            ids_d['Office_Id'] = a[0][0]

            # Find Id for ReportingUnit for contest via context_dictionary['Office']
            # find reporting unit associated to contest (not reporting unit associated to df row)
            election_district_name = df.state.context_dictionary['Office'][a[0][1]]['ElectionDistrict']
            election_district_id = id_from_select_only_PANDAS(ReportingUnit_dframe,{'Name':election_district_name})

            # insert into CandidateContest table
            votes_allowed = eval(munger_fields_d['CandidateContest'][0]['OtherFields']['VotesAllowed']) # TODO  misses other fields e.g. NumberElected
            value_d = {'Name':election_district_name,'ElectionDistrict_Id':election_district_id,'Office_Id':ids_d['Office_Id'],'VotesAllowed':votes_allowed}
            ids_d['contest_id'], CandidateContest_dframe= id_from_select_or_insert_PANDAS(CandidateContest_dframe,session,meta.tables[cdf_schema + '.CandidateContest'], value_d)

            # insert into Candidate table
            ballot_name = eval(munger_fields_d['Candidate'][0]['ExternalIdentifier'])
            value_d = {'BallotName':ballot_name,'Election_Id':election_id,'Party_Id':ids_d['Party_Id']}
            ids_d['Candidate_Id'], Candidate_dframe = id_from_select_or_insert_PANDAS(Candidate_dframe, value_d)

            # insert into CandidateSelection
            value_d = {'Candidate_Id':ids_d['Candidate_Id']}
            ids_d['selection_id'], CandidateSelection_dframe = id_from_select_or_insert_PANDAS(CandidateSelection_dframe, value_d)

            # create record in CandidateContestSelectionJoin
            value_d = {'CandidateContest_Id':ids_d['contest_id'],
                       'CandidateSelection_Id': ids_d['selection_id'],
                       'Election_Id':election_id}
            id_from_select_or_insert_PANDAS(CandidateContestSelectionJoin_dframe, value_d)

        # fill ElectionContestJoin
        value_d = {'Election_Id': election_id, 'Contest_Id': ids_d['contest_id']}
        id_from_select_or_insert_PANDAS(ElectionContestJoin_dframe, value_d)

        # process vote counts in row
        for ct,dic in munger_counts_d.items():
            value_d = {'Count':row[ct],'ReportingUnit_Id':ids_d['ReportingUnit_Id'],'CountItemType_Id': dic['CountItemType_Id'],'OtherCountItemType':dic['OtherCountItemType']}
            # TODO dupes are a problem only when contest & reporting unit are specified.
            ids_d['VoteCount_Id']=id_from_select_or_insert_PANDAS(VoteCount_dframe, value_d,  'dupes_ok')

            # fill SelectionElectionContestVoteCountJoin
            value_d = {'Selection_Id':ids_d['selection_id'],'Contest_Id':ids_d['contest_id'],'Election_Id':ids_d['Election_Id'],'VoteCount_Id':ids_d['VoteCount_Id']}
            id_from_select_or_insert_PANDAS(SelectionElectionContestVoteCountJoin_dframe,  value_d)

    return str(ids_d)

if __name__ == '__main__':
    import db_routines.Create_CDF_db as CDF
    import states_and_files as sf
    from sqlalchemy.orm import sessionmaker

    cdf_schema='cdf_xx_test'
    eng,meta = dbr.sql_alchemy_connect(schema=cdf_schema,paramfile='../../local_data/database.ini')
    Session = sessionmaker(bind=eng)
    session = Session()

    s = sf.create_state('XX', '../../local_data/XX')

    munger_path = '../../local_data/mungers/nc_export1.txt'
    print('Creating munger instance from ' + munger_path)
    mu = sf.create_munger(munger_path)

    print('Creating metafile instance')
    mf = sf.create_metafile(s, 'layout_results_pct.txt')

    print('Creating datafile instance')
    df = sf.create_datafile(s, 'General Election 2018-11-06', 'alamance.txt', mf, mu)

    raw_records_to_cdf(session,meta,df,mu,cdf_schema,0,0,'../CDF_schema_def_info/tables.txt')
    print('Done!')

