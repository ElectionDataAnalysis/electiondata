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

def id_and_name_from_external_PANDAS(ei_dframe, t_dframe, external_name, identifiertype_id, otheridentifiertype, internal_name_field='Name',t_dframe_Id_is_index=True):
    ## find the internal db name and id from external identifier

    ei_value_d = {'Value':external_name,'IdentifierType_Id':identifiertype_id,'OtherIdentifierType':otheridentifiertype}
    ei_filtered = ei_dframe.loc[(ei_dframe[list(ei_value_d)] == pd.Series(ei_value_d)).all(axis=1)]
    if ei_filtered.shape[0] == 1:
        table_id = ei_filtered['ForeignId'].to_list()[0]
        if t_dframe_Id_is_index:
            name = t_dframe.loc[table_id][internal_name_field]
        else:
            name = t_dframe[t_dframe['Id'] == table_id][internal_name_field].tolist()[0]
        return table_id, name
    elif ei_filtered.shape[0] > 1:
        raise Exception('\n\t'.join['Unexpected duplicates found for','external_name: '+external_name, 'identifiertype_id: '+str(identifiertype_id),'otheridentifiertype: '+otheridentifiertype])
    else:
        return None, None

def id_from_select_only_PANDAS(dframe,value_d, mode='no_dupes',dframe_Id_is_index=True,check_spelling = True):
    """Returns the Id of the record in table with values given in the dictionary value_d.
    On error (nothing found, or more than one found) returns 0"""

    # filter the dframe by the relevant value_d conditions
    cdf_value_d = {}
    for k,v in value_d.items():
        if k in dframe.columns:
            cdf_value_d[k] = v
    filtered_dframe = dframe.loc[(dframe[list(cdf_value_d)] == pd.Series(cdf_value_d)).all(axis=1)]

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
    elif filtered_dframe.shape[0] >1 and mode == 'no_dupes':
        raise Exception('More than one record found for these values:\n\t'+str(value_d))
    else:
        if dframe_Id_is_index:
            return filtered_dframe.index.to_list()[0]
        else:
            return filtered_dframe['Id'].to_list()[0]

def id_from_select_or_insert_PANDAS(dframe, value_d, session, schema, db_table_name,mode='no_dupes',dframe_Id_is_index=True):
    """  value_d gives the values for the fields in the dataframe.
    If there is a corresponding record in the table, return the id
    If there is no corresponding record, insert one and return the id.
    # TODO what id? If dataframe has not been committed to the db, how do we ensure uniqueness of the assigned id?
    (.e.g., value_d['Name'] = 'North Carolina;Alamance County');
    E.g., tables_d[table] = {'tablename':'ReportingUnit', 'fields':[{'fieldname':'Name','datatype':'TEXT'}],
    'enumerations':['ReportingUnitType','CountItemStatus'],'other_element_refs':[], 'unique_constraints':[['Name']],
    'not_null_fields':['ReportingUnitType_Id']
    modes with consequences: 'dupes_ok'
       } """
    # filter the dframe by the relevant value_d conditions # TODO This code repeats
    cdf_value_d = {}
    for k,v in value_d.items():
        if k in dframe.columns:
            cdf_value_d[k] = v

    # filter the dframe by the value_d conditions
    filtered_dframe = dframe.loc[(dframe[list(cdf_value_d)] == pd.Series(cdf_value_d)).all(axis=1)]

    if mode == 'no_dupes' and filtered_dframe.shape[0] > 1:
        raise Exception('Duplicate values found for ' + str(value_d))
    if filtered_dframe.shape[0] == 0:   # insert row if it's not already there
        filtered_dframe = filtered_dframe.append(value_d, ignore_index=True)
        dbr.dframe_to_sql(filtered_dframe, session, schema, db_table_name)
        if dframe_Id_is_index:
            index_col = 'Id'
        else:
            index_col = None
        dframe = pd.read_sql_table(db_table_name,session.bind,schema=cdf_schema,index_col=index_col) # TODO what about index?
        id = id_from_select_only_PANDAS(dframe,value_d,db_table_name,mode,dframe_Id_is_index)

    assert filtered_dframe.shape[0] == 1, 'filtered dataframe should have exactly one row'
    return id, dframe

def composing_from_reporting_unit_name_PANDAS(session,schema,ru_dframe,cruj_dframe,name,id=0):
    """inserts all ComposingReportingUnit joins that can be deduced from the internal db name of the ReportingUnit
    into the ComposingReportingUnitJoin dataframe; returns bigger dataframe.
    # Use the ; convention to identify all parents
    """

    if id == 0:
        child_id, ru_dframe = id_from_select_or_insert_PANDAS(ru_dframe, {'Name': name},session,schema,'ReportingUnit') # TODO does function actually change ru_dframe? Why?
    else: child_id = id
    chain = name.split(';')
    if len(chain) > 1:
        for i in range(1,len(chain)):
            parent = ';'.join(chain[0:i])
            parent_id = id_from_select_only_PANDAS(ru_dframe, {'Name': parent})
            unused_id, cruj_dframe = id_from_select_or_insert_PANDAS(cruj_dframe, {'ParentReportingUnit_Id': parent_id, 'ChildReportingUnit_Id': child_id},session,schema,'ComposingReportingUnitJoin')
    return cruj_dframe

def format_type_for_insert_PANDAS(dframe,txt,id_type_other_id,t_dframe_Id_is_index=True):
    """This is designed for enumeration dframes, which must have an "Id" field and a "Txt" field.
    other_id is the id for 'other' IdentifierType
    This function returns a (type_id, othertype_text) pair; for types in the enumeration, returns (type_id for the given txt, ""),
    while for other types returns (type_id for "other",txt) """
    # TODO check that dframe columns are 'Id' and 'Txt'
    if t_dframe_Id_is_index:
        id_list = dframe.index[dframe['Txt'] == txt].to_list()
    else:
        id_list = dframe[dframe['Txt'] == txt].to_list()
    if len(id_list) == 1:   # TODO prevent multiple fillings of *Type tables, which yield rowcounts > 1
        return([id_list[0],''])
    elif len(id_list) == 0:
        return[id_type_other_id,txt]
    else:
         raise Exception('Dataframe has duplicate rows with value ' + txt + ' in Txt column')

def fill_cdf_table_from_raw(session, row, cdf_schema, mu, t, ei_dframe, foreign_key_d = {}, filters=[], id_type_other_id=0, index_col='Id'):
    """
    t is name of table in cdf
    mu is munger
    `row` is a dataframe of the raw data file; debugger may not recognize its use, hidden in eval()
    NB: the name `row` is essential and appears in def of munger as of 1/2020
    """
    # get munger info
    munger_fields_d = mu.content_dictionary['fields_dictionary']

    ei_dict = {} # to hold internal_name - internal_id pairs
    ids_d = foreign_key_d  # note: name `ids_d` is used in definition of munger, so can't be changed.

    # filter the row dataframe
    for f in filters:
        row = row[eval(f)]
        print('\tFilter: '+f)

    t_dframe = pd.read_sql_table(t, session.bind, cdf_schema, index_col=index_col)
    for item in munger_fields_d[t]:  # there should be only one of these
        # loop through unique values in the raw file
        raw_column = eval(item['ExternalIdentifier'])
        print('\tFor table '+t+', '+ str(len(raw_column.unique())) + ' items to process')
        for external_name in raw_column.unique():
            if external_name and external_name.strip() != '':   # treat only items with content
                # print('\t\tProcessing '+external_name)
                # get internal db name and id for ExternalIdentifier
                [cdf_id, cdf_name] = id_and_name_from_external_PANDAS(ei_dframe, t_dframe, external_name, id_type_other_id, mu.name, item['InternalNameField']) # TODO note new flag t_dframe_Id_is_index for id_and_name_from_external
                # ... or if no such is found in db, insert it!
                if [cdf_id, cdf_name] == [None, None]:
                    if external_name:
                        cdf_name = external_name.strip()
                    value_d ={**{item['InternalNameField']: cdf_name},**foreign_key_d}
                    for e in item['Enumerations'].keys():
                        [value_d[e + '_Id'], value_d['Other' + e]] = format_type_for_insert_PANDAS(t_dframe,item['Enumerations'][e])
                        # TODO note new flag t_dframe_Id_is_index for format_type_for_insert_PANDAS
                        # format_type_for_insert(session,meta.tables[meta.schema + '.' + e], item['Enumerations'][e])
                    for f in item['OtherFields'].keys():
                        value_d[f] = eval(item['OtherFields'][f])
                    cdf_id = id_from_select_only_PANDAS(t_dframe,value_d) # TODO note new flag t_dframe_Id_is_index for id_from_select_only_PANDAS
                    if cdf_id == 0:  # if nothing found
                        # TODO must insert. But with what Id?  What is we pull 'Id' as a plain column to t_dframe?
                        cdf_id, t_dframe = id_from_select_or_insert_PANDAS(t_dframe, value_d,session,cdf_schema,t)
                if cdf_name is not None:
                    ei_dict[cdf_name] = cdf_id
        # % commit table to the db
    t_dframe = dbr.dframe_to_sql(t_dframe,session,cdf_schema,t)
    print('\tTable loaded to database: '+cdf_schema+'.'+t)

    #%% pull the table back from the db, to get Ids right.
    t_dframe = pd.read_sql_table(t,session.bind,cdf_schema,index_col=index_col)
    return t_dframe, ei_dict

def bulk_elements_to_cdf(session, mu, cdf_schema, row, election_id, id_type_other_id,state_id):
    """
    Create tables, which are repetitive,
    don't come from context (hence not BallotMeasureSelection, Party)
    and whose db Ids are needed for other insertions.
    `row` is a dataframe of the raw data file
    NB: the name `row` is essential and appears in def of munger as of 1/2020
    """
    assert int(id_type_other_id) and id_type_other_id != 0,'id_type_other_id must be a nonzero integer'
    assert int(state_id) and state_id !=0, 'state_id must be a nonzero integer'

    cdf_d = {}  # dataframe for each table
    ei_d = {}   # external-internal name dictionary for each table

    # get external identifier info
    cdf_d['ExternalIdentifier'] = pd.read_sql_table('ExternalIdentifier', session.bind, cdf_schema, index_col='Id')

    cdf_d['Party'] = pd.read_sql_table('Party',session.bind,cdf_schema,index_col='Id')

    party_ids = cdf_d['Party'].index.to_list()
    #  process Candidate

    cdf_ei = cdf_d['ExternalIdentifier']  # for legibility
    for party_id in party_ids:
        foreign_key_d = {'Election_Id':election_id,'Party_Id':party_id}
        raw_filters = [' row["Choice Party"] == "' + cdf_ei[cdf_ei['ForeignId'] == party_id]['Value'].to_list()[0] + '"']
        cdf_d['Candidate'], ei_d['Candidate'] = fill_cdf_table_from_raw(session, row, cdf_schema, mu, 'Candidate', cdf_d['ExternalIdentifier'], foreign_key_d=foreign_key_d, filters=raw_filters, id_type_other_id=id_type_other_id)

    # process CandidateSelection
    cdf_d['CandidateSelection'] = pd.DataFrame(cdf_d['Candidate'].index.values, columns=['Candidate_Id'])
    cdf_d['CandidateSelection'] = dbr.dframe_to_sql(cdf_d['CandidateSelection'], session, cdf_schema, 'CandidateSelection')

    # get BallotMeasureSelections to distinguish between BallotMeasure- and Candidate-Contests
    cdf_d['BallotMeasureSelection'] = pd.read_sql_table('BallotMeasureSelection',session.bind,cdf_schema,index_col='Id')
    bm_selections = cdf_d['BallotMeasureSelection']['Selection'].to_list()

    # process BallotMeasureContest
    foreign_key_d = {'state':state_id}
    raw_filters=[" | ".join(["(row['Choice'] == '" + i + "')" for i in bm_selections])] # TODO munger-dependent
    cdf_d['BallotMeasureContest'], ei_d['BallotMeasureContest'] = fill_cdf_table_from_raw(session, row, cdf_schema, mu, 'BallotMeasureContest', cdf_d['ExternalIdentifier'], filters=raw_filters, foreign_key_d=foreign_key_d, id_type_other_id=id_type_other_id)

    return cdf_d, id_type_other_id

def row_by_row_elements_to_cdf(session,mu,cdf_schema,raw_rows,cdf_d,election_id,id_type_other_id):
    """
    mu is a munger. cdf_d is a dictionary of dataframes
    """

    ids_d = {}
    name_d = {}

    cdf_d['Office'] = pd.read_sql_table('Office',session.bind,schema=cdf_schema,index_col='Id')
    cdf_d['BallotMeasureSelection'] = pd.read_sql_table('Office',session.bind,schema=cdf_schema,index_col='Id')

    # get BallotMeasureSelections to distinguish between BallotMeasure- and Candidate-Contests
    cdf_d['BallotMeasureSelection'] = pd.read_sql_table('BallotMeasureSelection',session.bind,cdf_schema,index_col='Id')
    bm_selections = cdf_d['BallotMeasureSelection']['Selection'].to_list()
    bm_filter = " | ".join(["(row['Choice'] == '" + i + "')" for i in bm_selections]) # TODO munger-dependent

    # TODO strip out any rows corresponding to untracked offices before looping

    for index, row in raw_rows.iterrows():
        # track progress
        frequency_of_report = 500
        if index % frequency_of_report == 0:
            print('\t\tProcessing row ' + str(index) + ':\n' + str(row))

        # if row corresponds to a CandidateContest for an Office that is not being tracked, skip it.
        if eval(bm_filter):    # if row is a BallotMeasure row
            ballot_measure_raw_row = True
        else:
            ballot_measure_raw_row = False

        if not ballot_measure_raw_row:
            ids_d['Office'] = id_from_select_only_PANDAS(cdf_d['Office'],{'Name':eval(mu.content_dictionary['fields_dictionary']['Office'][0]['ExternalIdentifier'])})
            if ids_d['Office'] == 0: # skip rows for which office was not explicitly listed in context folder
                continue
            ids_d['contest_id'] = id_from_select_only_PANDAS(cdf_d['CandidateContest'], {'Office_Id': ids_d['Office']})
            assert ids_d['contest_id'] !=0 , 'contest_id cannot be zero'

        for t in ['ReportingUnit','Party','Office']:
            # TODO error handling? What if id not found?
            for item in mu.content_dictionary['fields_dictionary'][t]:
                ids_d[t],name_d[t] = id_and_name_from_external_PANDAS(cdf_d['ExternalIdentifier'], cdf_d[t], eval(item['ExternalIdentifier']),id_type_other_id,mu.name,item['InternalNameField'])

        # process Candidate and BallotMeasure elements
        if ballot_measure_raw_row: # row info is for a Ballot Measure
            selection = eval(mu.content_dictionary['fields_dictionary']['BallotMeasureSelection'][0]['ExternalIdentifier'])
            ids_d['selection_id'] = cdf_d['BallotMeasureSelection'][cdf_d['BallotMeasureSelection']['Selection'] == selection].index.to_list()[0]
            ids_d['contest_id'] = id_from_select_only_PANDAS(cdf_d['BallotMeasureContest'],{'Name':eval(mu.content_dictionary['fields_dictionary']['BallotMeasureContest'][0]['ExternalIdentifier'])})
        # fill BallotMeasureContestSelectionJoin
            value_d = {'BallotMeasureContest_Id': ids_d['contest_id'], 'BallotMeasureSelection_Id': ids_d['selection_id']}
            join_id,cdf_d['BallotMeasureContestSelectionJoin'] = id_from_select_or_insert_PANDAS(cdf_d['BallotMeasureContestSelectionJoin'],value_d,session,cdf_schema,'BallotMeasureContestSelectionJoin')
        else:   # if Candidate row
            ballot_name = eval(mu.content_dictionary['fields_dictionary']['Candidate'][0]['ExternalIdentifier'])
            ids_d['Candidate'] = id_from_select_only_PANDAS(cdf_d['Candidate'],{'BallotName':ballot_name})
            ids_d['selection_id'] = id_from_select_only_PANDAS(cdf_d['CandidateSelection'],{'Candidate_Id':ids_d['Candidate']})

        # fill CandidateContestSelectionJoin
            value_d = {'CandidateContest_Id': ids_d['contest_id'], 'CandidateSelection_Id': ids_d['selection_id'],'Election_Id':election_id}
            join_id,cdf_d['CandidateContestSelectionJoin'] = id_from_select_or_insert_PANDAS(cdf_d['CandidateContestSelectionJoin'], value_d, session, cdf_schema, 'CandidateContestSelectionJoin')

        # fill ElectionContestJoin
        # TODO doesn't need to be row-by-row, can be done in bulk.
        value_d = {'Election_Id': election_id, 'Contest_Id': ids_d['contest_id']}
        join_id, cdf_d['ElectionContestJoin'] = id_from_select_or_insert_PANDAS(cdf_d['ElectionContestJoin'], value_d, session, cdf_schema,'ElectionContestJoin')

        for ct,dic in mu.content_dictionary['counts_dictionary'].items():
        # fill VoteCount
            value_d = {'Count':row[ct],'ReportingUnit_Id':ids_d['ReportingUnit'],'CountItemType_Id': dic['CountItemType_Id'],'OtherCountItemType':dic['OtherCountItemType']}
            # TODO dupes are a problem only when contest & reporting unit are specified.
            ids_d['VoteCount'], cdf_d['VoteCount'] =id_from_select_or_insert_PANDAS(cdf_d['VoteCount'], value_d, session, cdf_schema, 'VoteCount',  'dupes_ok')

        # fill SelectionElectionContestVoteCountJoin
            value_d = {'Selection_Id':ids_d['selection_id'],'Contest_Id':ids_d['contest_id'],'Election_Id':election_id,'VoteCount_Id':ids_d['VoteCount']}
            join_id, cdf_d['SelectionElectionContestVoteCountJoin'] = id_from_select_or_insert_PANDAS(cdf_d['SelectionElectionContestVoteCountJoin'],  value_d, session, cdf_schema,'SelectionElectionContestVoteCountJoin')
        if index % frequency_of_report == 0:
            print('\t\tPushing to db ')
            for t in ['BallotMeasureContestSelectionJoin', 'CandidateContestSelectionJoin', 'ElectionContestJoin',
                      'VoteCount', 'SelectionElectionContestVoteCountJoin']:
                print ('Pushing to database table '+ t)
                dbr.dframe_to_sql(cdf_d[t],session,cdf_schema,t)
            session.flush()

    for t in ['BallotMeasureContestSelectionJoin','CandidateContestSelectionJoin','ElectionContestJoin','VoteCount','SelectionElectionContestVoteCountJoin']:
        dbr.dframe_to_sql(cdf_d[t],session,cdf_schema,t)
    session.flush()
    return

def raw_records_to_cdf(session,meta,df,mu,cdf_schema,state_id = 0,id_type_other_id = 0,cdf_table_filepath='CDF_schema_def_info/tables.txt'):
    """ munger-agnostic raw-to-cdf script; ***
    df is datafile, mu is munger """
    # TODO why did the dframe creation loop fail?
    #for tbl in ['ElectionType','CountItemType','ReportingUnitType','Election','ReportingUnit','Party','CandidateContest','Candidate','CandidateSelection','BallotMeasureContestSelectionJoin','ElectionContestJoin','ExternalIdentifier','BallotMeasureSelection']:
        #dframe_creation_string = tbl+'_dframe = pd.read_sql_table(\'' + tbl + '\',session.bind, cdf_schema, index_col=\'Id\')'
        #exec(dframe_creation_string)
        #print (dframe_creation_string)
    dframe_list = ['ElectionType', 'CountItemType', 'ReportingUnitType', 'ReportingUnit', 'Party', 'Election',
                           'Office',
                           'CandidateContest', 'BallotMeasureContest',
                           'BallotMeasureSelection','Candidate', 'CandidateSelection', 'VoteCount',
                           'SelectionElectionContestVoteCountJoin', 'BallotMeasureContestSelectionJoin',
                           'ElectionContestJoin',
                           'CandidateContestSelectionJoin', 'ComposingReportingUnitJoin', 'ExternalIdentifier']
    #%% define dataframes
    cdf_d = {}
    cdf_d['ElectionType'] = pd.read_sql_table('ElectionType', session.bind, cdf_schema, index_col='Id')
    cdf_d['CountItemType'] = pd.read_sql_table('CountItemType', session.bind, cdf_schema, index_col='Id')
    cdf_d['ReportingUnitType'] = pd.read_sql_table('ReportingUnitType', session.bind, cdf_schema, index_col='Id')
    cdf_d['Election'] = pd.read_sql_table('Election', session.bind, cdf_schema, index_col='Id')
    cdf_d['ReportingUnit'] = pd.read_sql_table('ReportingUnit', session.bind, cdf_schema, index_col='Id')
    cdf_d['Party'] = pd.read_sql_table('Party', session.bind, cdf_schema, index_col='Id')
    cdf_d['Office'] = pd.read_sql_table('Office', session.bind, cdf_schema, index_col='Id')
    cdf_d['CandidateContest'] = pd.read_sql_table('CandidateContest', session.bind, cdf_schema, index_col='Id')
    cdf_d['Candidate'] = pd.read_sql_table('Candidate', session.bind, cdf_schema, index_col='Id')
    cdf_d['CandidateSelection'] = pd.read_sql_table('CandidateSelection', session.bind, cdf_schema, index_col='Id')
    cdf_d['BallotMeasureContest'] = pd.read_sql_table('BallotMeasureContest', session.bind, cdf_schema, index_col='Id')
    cdf_d['VoteCount'] = pd.read_sql_table('VoteCount', session.bind, cdf_schema, index_col='Id')
    cdf_d['SelectionElectionContestVoteCountJoin'] = pd.read_sql_table('SelectionElectionContestVoteCountJoin', session.bind, cdf_schema, index_col='Id')
    cdf_d['BallotMeasureContestSelectionJoin'] = pd.read_sql_table('BallotMeasureContestSelectionJoin', session.bind, cdf_schema, index_col='Id')
    cdf_d['ElectionContestJoin'] = pd.read_sql_table('ElectionContestJoin', session.bind, cdf_schema, index_col='Id')
    cdf_d['CandidateContestSelectionJoin'] = pd.read_sql_table('CandidateContestSelectionJoin', session.bind, cdf_schema, index_col='Id')
    cdf_d['ComposingReportingUnitJoin'] = pd.read_sql_table('ComposingReportingUnitJoin', session.bind, cdf_schema, index_col='Id')
    cdf_d['ExternalIdentifier'] = pd.read_sql_table('ExternalIdentifier', session.bind, cdf_schema, index_col='Id')
    cdf_d['BallotMeasureSelection'] = pd.read_sql_table('BallotMeasureSelection', session.bind, schema=cdf_schema,
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

    # get id for  election
    [electiontype_id, otherelectiontype] = format_type_for_insert_PANDAS(cdf_d['ElectionType'],df.state.context_dictionary['Election'][df.election]['ElectionType'],id_type_other_id)
    value_d = {'Name': df.election, 'EndDate': df.state.context_dictionary['Election'][df.election]['EndDate'],
               'StartDate': df.state.context_dictionary['Election'][df.election]['StartDate'],
               'OtherElectionType': otherelectiontype, 'ElectionType_Id': electiontype_id}
    election_id, Election_dframe = id_from_select_or_insert_PANDAS(cdf_d['Election'], value_d,session,cdf_schema,'Election')

    # if state_id is not passed as parameter, select-or-insert state, get id (default Reporting Unit for ballot questions)
    if state_id == 0:
        [reportingunittype_id, otherreportingunittype] = format_type_for_insert_PANDAS(cdf_d['ReportingUnitType'], 'state',id_type_other_id)
        value_d = {'Name': df.state.name, 'ReportingUnitType_Id': reportingunittype_id,
                   'OtherReportingUnitType': otherreportingunittype}
        state_id, cdf_d['ReportingUnit'] = id_from_select_or_insert_PANDAS(cdf_d['ReportingUnit'], value_d, session, cdf_schema,'ReportingUnit')

    # store state_id and election_id for later use
    ids_d = {'state': state_id, 'Election_Id': election_id}  # to hold ids of found items for later reference

    munger_raw_cols = mu.content_dictionary['raw_cols'] # TODO is this used?
    # create dictionaries for processing data from rows. Not all CDF elements are included. E.g., 'Election' element is not filled from df rows, but from df.election

    munger_counts_d = mu.content_dictionary['counts_dictionary'] # TODO is this used?
    # look up id,type pairs for each kind of count, add info to counts dictionary
    for ct,dic in munger_counts_d.items():
        text = dic['CountItemType']
        [dic['CountItemType_Id'], dic['OtherCountItemType']] = format_type_for_insert_PANDAS(cdf_d['CountItemType'], text,id_type_other_id)
    munger_fields_d = mu.content_dictionary['fields_dictionary']
    # TODO is the above used?
    #%% read raw data rows from db
    raw_rows = pd.read_sql_table(df.table_name,session.bind,schema=df.state.schema_name)

    bulk_items_already_loaded = input('Are bulk items (Candidate, etc.) already loaded (y/n)?\n')
    if bulk_items_already_loaded != 'y':
        cdf_dframe, unused = bulk_elements_to_cdf(session, mu, cdf_schema, raw_rows, election_id, id_type_other_id,ids_d['state'])

    row_by_row_elements_to_cdf(session,mu,cdf_schema,raw_rows,cdf_d,election_id,id_type_other_id)


    #%% process raw data row by row # TODO shold be defunct now below
    for index,row in raw_rows.iterrows():   # loop over content rows of dataframe
        # track progress
        frequency_of_report = 500
        if index % frequency_of_report == 0:
            print('\t\tProcessing item number ' + str(index) + ':\n' + str(row))
        #%% Process all straight-forward elements into cdf and capture id in ids_d
        for t in ['ReportingUnit']:  # TODO list may be munger-dependent
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
                        if cdf_name == '' or cdf_name == ' ':
                            cdf_id = None
                        else:
                            value_d = {item['InternalNameField']: cdf_name}  # usually 'Name' but not always
                            for e in item['Enumerations'].keys():  # e.g. e = 'ReportingUnitType'
                                [value_d[e + '_Id'], value_d['Other' + e]] = format_type_for_insert_PANDAS(eval(t+'_dframe'),item['Enumerations'][e])
                                # format_type_for_insert(session,meta.tables[meta.schema + '.' + e], item['Enumerations'][e])
                            for f in item['OtherFields'].keys():
                                value_d[f] = eval(item['OtherFields'][f])
                            if t == 'CandidateContest' or t == 'BallotMeasureContest':  # need to get ElectionDistrict_Id from contextual knowledge
                                value_d['ElectionDistrict_Id'] = ids_d['contest_reporting_unit_id']
                            exec('cdf_id,' + t + '_dframe  = id_from_select_or_insert_PANDAS(' + t+'_dframe,  value_d)', session, cdf_schema,t)

                        # if newly inserted item is a ReportingUnit, insert all ComposingReportingUnit joins that can be deduced from the internal db name of the ReportingUnit
                        if t == 'ReportingUnit':
                            ComposingReportingUnitJoin_dframe = composing_from_reporting_unit_name_PANDAS(cdf_d['ReportingUnit'],ComposingReportingUnitJoin_dframe,cdf_name,cdf_id)
            ids_d[t + '_Id'] = cdf_id
            if index % frequency_of_report == 0:
                print('\t\t\tmunger fields done for ' + t)

        #%% process Ballot Measures and Candidate Contests into CDF and capture ids into ids_d (depends on values in row):
        selection = eval(munger_fields_d['BallotMeasureSelection'][0]['ExternalIdentifier'])
        # *** cond = munger_fields_d['BallotMeasureSelection']['ExternalIdentifier'] +' in BallotMeasureSelection_dict.keys()'

        #%% fill Selection and Contest tables -- and for Candidate Contest, Candidate too -- and all associated joins.
        if selection in cdf_d['BallotMeasureSelection'].index:     # if selection is a Ballot Measure selection, assume contest is a Ballot Measure
            ids_d['selection_id'] = cdf_d['BallotMeasureSelection'].loc[selection]['Id']
            # fill BallotMeasureContest
            value_d = {'Name':eval(munger_fields_d['BallotMeasureContest'][0]['ExternalIdentifier']),'ElectionDistrict_Id':state_id}  # all ballot measures are assumed to be state-level ***
            ids_d['contest_id'], BallotMeasureContest_dframe = id_from_select_or_insert_PANDAS(BallotMeasureContest_dframe,  value_d,session,cdf_schema,'BallotMeasureContest')
            # fill BallotMeasureContestSelectionJoin ***
            value_d = {'BallotMeasureContest_Id':ids_d['contest_id'],'BallotMeasureSelection_Id':ids_d['selection_id']}
            join_id,BallotMeasureContestSelectionJoin_dframe = id_from_select_or_insert_PANDAS(BallotMeasureContestSelectionJoin_dframe, value_d, session, cdf_schema, 'BallotMeasureContestSelectionJoin')

        else:       # if not a Ballot Measure (i.e., if a Candidate Contest)
            raw_office_name = eval(munger_fields_d['Office'][0]['ExternalIdentifier'])

            ei_id = id_from_select_only_PANDAS(ExternalIdentifier_dframe,{'IdentifierType_Id':id_type_other_id,'Value':raw_office_name,'OtherIdentifierType':mu.name})

            if ei_id == 0: # if Office is not already associated to the munger in the db (from state's context_dictionary, for example), skip this row
               continue
            ids_d['Office_Id'] = ExternalIdentifier_dframe['ForeignId'].loc[ei_id]
            office_name = Office_dframe['Name'].loc[ids_d['Office_Id']]

            # Find Id for ReportingUnit for contest via context_dictionary['Office']
            # find reporting unit associated to contest (not reporting unit associated to df row)
            election_district_name = df.state.context_dictionary['Office'][office_name]['ElectionDistrict']
            election_district_id = id_from_select_only_PANDAS(cdf_d['ReportingUnit'],{'Name':election_district_name})

            # insert into CandidateContest table
            votes_allowed = eval(munger_fields_d['CandidateContest'][0]['OtherFields']['VotesAllowed']) # TODO  misses other fields e.g. NumberElected
            value_d = {'Name':election_district_name,'ElectionDistrict_Id':election_district_id,'Office_Id':ids_d['Office_Id'],'VotesAllowed':votes_allowed}
            ids_d['contest_id'], CandidateContest_dframe= id_from_select_or_insert_PANDAS(CandidateContest_dframe, value_d,session, cdf_schema, 'CandidateContest')

            # insert into Candidate table
            ballot_name = eval(munger_fields_d['Candidate'][0]['ExternalIdentifier'])
            value_d = {'BallotName':ballot_name,'Election_Id':election_id,'Party_Id':ids_d['Party_Id']}
            ids_d['Candidate_Id'], Candidate_dframe = id_from_select_or_insert_PANDAS(Candidate_dframe, value_d, session, cdf_schema,'Candidate')

            # insert into CandidateSelection
            value_d = {'Candidate_Id':ids_d['Candidate_Id']}
            ids_d['selection_id'], CandidateSelection_dframe = id_from_select_or_insert_PANDAS(CandidateSelection_dframe, value_d,session,cdf_schema,'CandidateSelection')

            # create record in CandidateContestSelectionJoin
            value_d = {'CandidateContest_Id':ids_d['contest_id'],
                       'CandidateSelection_Id': ids_d['selection_id'],
                       'Election_Id':election_id}
            join_id, CandidateContestSelectionJoin_dframe = id_from_select_or_insert_PANDAS(CandidateContestSelectionJoin_dframe, value_d, session, cdf_schema, 'CandidateContestSelectionJoin')




        # fill ElectionContestJoin
        value_d = {'Election_Id': election_id, 'Contest_Id': ids_d['contest_id']}
        join_id,ElectionContestJoin_dframe = id_from_select_or_insert_PANDAS(ElectionContestJoin_dframe, value_d,session, cdf_schema,'ElectionContestJoin')
        if index % frequency_of_report == 0:
            print('\t\t\tselection, contest and election-contest-join items entered')

        # process vote counts in row
        for ct,dic in munger_counts_d.items():
            value_d = {'Count':row[ct],'ReportingUnit_Id':ids_d['ReportingUnit_Id'],'CountItemType_Id': dic['CountItemType_Id'],'OtherCountItemType':dic['OtherCountItemType']}
            # TODO dupes are a problem only when contest & reporting unit are specified.
            ids_d['VoteCount_Id'], VoteCount_dframe =id_from_select_or_insert_PANDAS(VoteCount_dframe, value_d, session, cdf_schema, 'VoteCount', mode='dupes_ok')

            # fill SelectionElectionContestVoteCountJoin
            value_d = {'Selection_Id':ids_d['selection_id'],'Contest_Id':ids_d['contest_id'],'Election_Id':ids_d['Election_Id'],'VoteCount_Id':ids_d['VoteCount_Id']}
            join_id,SelectionElectionContestVoteCountJoin_dframe = id_from_select_or_insert_PANDAS(SelectionElectionContestVoteCountJoin_dframe,  value_d, session, cdf_schema, 'SelectionElectionContestJoin')

        if index % frequency_of_report == 0:
            for dframe in dframe_list:
                dbr.dframe_to_sql(eval(dframe + "_dframe"), session, cdf_schema, dframe)
                # exec(dframe + "_dframe.to_sql('" + dframe + "', session.bind, schema=cdf_schema, if_exists='append')")
            session.flush()

    #%% upload all dataframes to the cdf db
    for dframe in dframe_list:
        dbr.dframe_to_sql(eval(dframe + "_dframe"),session,cdf_schema,dframe)
        # exec(dframe + "_dframe.to_sql('" + dframe + "', session.bind, schema=cdf_schema, if_exists='append')")
    session.flush()
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

