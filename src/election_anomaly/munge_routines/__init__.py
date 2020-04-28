#!/usr/bin/python3
# munge_routines/__init__.py
# under construction

import db_routines as dbr
import user_interface as ui
import pandas as pd
import time
import re
import os
import numpy as np


def clean_raw_df(raw,munger):
    # TODO put all info about data cleaning into README.md (e.g., whitespace strip)

    # change all nulls to 0
    raw = raw.fillna('')
    # strip whitespace
    raw = raw.applymap(lambda x:x.strip())

    field_lists = list(munger.cdf_elements[munger.cdf_elements.source=='row'].fields)
    munger_fields = set().union(*field_lists)
    # TODO keep columns named in munger formulas; keep numerical columns; drop all else.
    if munger.header_row_count > 1:
        cols_to_munge = [x for x in raw.columns if x[munger.field_name_row] in munger_fields]
    else:
        cols_to_munge = [x for x in raw.columns if x in munger_fields]

    # TODO error check- what if cols_to_munge is missing something from munger_fields?

    # recast other columns as integer where possible.
    #  (recast leaves columns with text entries as non-numeric).
    num_columns = []
    for c in [x for x in raw.columns if x not in cols_to_munge]:
        try:
            raw[c] = raw[c].astype('int64',errors='raise')
            num_columns.append(c)
        except ValueError:
            pass

    raw = raw[cols_to_munge + num_columns]
    # recast all cols_to_munge to strings
    for c in cols_to_munge:
        raw[c] = raw[c].apply(str)
    # rename columns to munge by adding suffix
    renamer = {x:f'{x}_{munger.field_rename_suffix}' for x in cols_to_munge}
    raw.rename(columns=renamer,inplace=True)
    renamed_cols_to_munge = [f'{x}_{munger.field_rename_suffix}' for x in cols_to_munge]
    return raw, renamed_cols_to_munge, num_columns


def load_context_dframe_into_cdf(
        session,project_root,jurisdiction,source_df1,element,cdf_schema_def_dir='CDF_schema_def_info/'):
    """<source_df> should have all info needed for insertion into cdf:
    for enumerations, the value of the enumeration (e.g., 'precinct')
    for other fields, the value of the field (e.g., 'North Carolina;Alamance County').
"""
    # TODO check that source_df has the right format

    # dedupe source_df
    dupes,source_df = ui.find_dupes(source_df1)
    if not dupes.empty:
        print(f'WARNING: duplicates removed from dataframe, may indicate a problem.\n{source_df1}')

    # replace nulls with empty strings
    source_df.fillna('',inplace=True)

    enum_file = os.path.join(cdf_schema_def_dir,'elements',element,'enumerations.txt')
    if os.path.isfile(enum_file):  # (if not, there are no enums for this element)
        enums = pd.read_csv(enum_file,sep='\t')
        # get all relevant enumeration tables
        for e in enums['enumeration']:  # e.g., e = "ReportingUnitType"
            cdf_e = pd.read_sql_table(e,session.bind)
            # for every instance of the enumeration in the current table, add id and othertype columns to the dataframe
            if e in source_df.columns:
                source_df = enum_col_to_id_othertext(source_df,e,cdf_e)
        # TODO skipping assignment of CountItemStatus to ReportingUnit for now,
        #  since we can't assign an ReportingUnit as ElectionDistrict to Office
        #  (unless Office has a CountItemStatus; can't be right!)
        #  Note CountItemStatus is weirdly assigned to ReportingUnit in NIST CDF.
        #  Note also that CountItemStatus is not required, and a single RU can have many CountItemStatuses

    # TODO somewhere, check that no CandidateContest & Ballot Measure share a name; ditto for other false foreign keys

    # get Ids for any foreign key (or similar) in the table, e.g., Party_Id, etc.
    fk_file = os.path.join(cdf_schema_def_dir,'elements',element,'foreign_keys.txt')
    if os.path.isfile(fk_file):
        fks = pd.read_csv(fk_file,sep='\t',index_col='fieldname')
        for fn in fks.index:
            # append the Id corresponding to <fn> from the db
            refs = fks.loc[fn,'refers_to'].split(';')
            target = pd.concat([pd.read_sql_table(r,session.bind)[['Id','Name']] for r in refs],axis=1)
            target.rename(columns={'Id':fn,'Name':f'{fn}_Name'},inplace=True)
            source_df = source_df.merge(target,how='left',left_on=fn[:-3],right_on=f'{fn}_Name')
            source_df.drop([f'{fn}_Name'],axis=1)

    # commit info in source_df to corresponding cdf table to db
    dbr.dframe_to_sql(source_df,session,None,element)
    return


def add_munged_column(raw,mu,element,new_col_name):
    """Alters dataframe <raw> (in place), adding or redefining <new_col_name>
    via the string corresponding to <cdf_element>, per <munge_dictionary>"""
    if raw.empty:
        return raw
    else:

        # use regex to turn value string in munge dictionary into the corresponding commands (e.g., decode '<County>;<Precinct>'
        p = re.compile('(?P<text>[^<>]*)<(?P<field>[^<>]+)>')   # pattern to find text,field pairs
        q = re.compile('(?<=>)[^<]*$')                          # pattern to find text following last pair

        # add suffix as necessary
        formula = mu.cdf_elements.loc[element,'raw_identifier_formula']
        for x in mu.field_list:
            if x not in raw.columns:
                formula = formula.replace(f'<{x}>',f'<{x}_{mu.field_rename_suffix}>')

        text_field_list = re.findall(p,formula)
        last_text = re.findall(q,formula)

        if last_text:
            raw.loc[:,new_col_name] = last_text[0]
        else:
            raw.loc[:,new_col_name] = ''

        text_field_list.reverse()
        for t,f in text_field_list:
            raw.loc[:,new_col_name] = t + raw.loc[:,f] + raw.loc[:,new_col_name]
        return raw


def text_fragments_and_fields(formula):
    """Given a formula with fields enclosed in angle brackets,
    return a list of text-fragment,field pairs (in order of appearance) and a final text fragment.
    E.g., if formula is <County>;<Precinct>, returned are [(None,County),(';',Precinct)] and None."""
    # use regex to apply formula (e.g., decode '<County>;<Precinct>'
    p = re.compile('(?P<text>[^<>]*)<(?P<field>[^<>]+)>')  # pattern to find text,field pairs
    q = re.compile('(?<=>)[^<]*$')  # pattern to find text following last pair
    text_field_list = re.findall(p,formula)
    last_text = re.findall(q,formula)
    return text_field_list,last_text


def get_name_field(t):
    if t == 'BallotMeasureSelection':
        field = 'Selection'
    elif t == 'Candidate':
        field = 'BallotName'
    elif t == 'CountItemType':
        field = 'Txt'
    else:
        field = 'Name'
    return field


def add_munged_column_NEW(raw,munger,element,mode='row',inplace=True):
    """Alters dataframe <raw>, adding or redefining <element>_raw column
    via the <formula>."""
    if raw.empty:
        return raw
    if inplace:
        working = raw
    else:
        working = raw.copy()
    formula = munger.cdf_elements.loc[element,'raw_identifier_formula']
    if mode == 'row':
        for field in munger.field_list:
            formula = formula.replace(f'<{field}>',f'<{field}_{munger.field_rename_suffix}>')
    elif mode == 'column':
        for i in range(munger.header_row_count):
            formula = formula.replace(f'<{i}>',f'<variable_{i}>')

    text_field_list,last_text = text_fragments_and_fields(formula)

    if last_text:
        working.loc[:,f'{element}_raw'] = last_text[0]
    else:
        working.loc[:,f'{element}_raw'] = ''

    text_field_list.reverse()
    for t,f in text_field_list:
        assert f != f'{element}_raw',f'Column name conflicts with element name: {f}'
        working.loc[:,f'{element}_raw'] = t + working.loc[:,f] + working.loc[:,f'{element}_raw']
    return working


def contest_type_split(row,mu):
    if mu.ballot_measure_style=='yes_and_no_are_candidates':
        bm_row = row[row[mu.ballot_measure_selection_col].isin(mu.ballot_measure_selection_list)]
        cc_row = row[~row[mu.ballot_measure_selection_col].isin(mu.ballot_measure_selection_list)]
    elif mu.ballot_measure_style == 'yes_and_no_are_columns':
        bm_count_cols=mu.ballot_measure_count_column_selections.merge(mu.count_columns,how='left',left_on='fieldname',right_on='RawName')
        cc_row=row.copy()
        # TODO check that all CountItemTypes have either 2 or 0 columns for ballot measures
        # if any CountItemType has numbers in both yes and no bm columns, assume not a candidate contest
        for cit in bm_count_cols.CountItemType.unique():
            # each count item
            yes_col= bm_count_cols[(bm_count_cols.CountItemType==cit) & (bm_count_cols.selection=='Yes')].iloc[0].RawName
            no_col= bm_count_cols[(bm_count_cols.CountItemType==cit) & (bm_count_cols.selection=='No')].iloc[0].RawName
            cc_row=cc_row[~((cc_row[yes_col]!='') & (cc_row[no_col]!=''))]
        bm_row = row[~row.index.isin(cc_row.index)]
    else:
        raise Exception(f'Ballot measure style {mu.ballot_measure_style} not recognized')
    return bm_row.copy(), cc_row.copy()


def get_internal_ids(row_df,mu,table_df,element,internal_name_column,unmatched_dir,drop_unmatched=False):
    """replace columns in <raw> with raw_identifier values by columns with internal names
    """
    assert os.path.isdir(unmatched_dir), f'Argument {unmatched_dir} is not a directory'
    if drop_unmatched:
        how='inner'
    else:
        how='left'
    # join the 'cdf_internal_name' from the raw_identifier table -- this is the internal name field value,
    # no matter what the name field name is in the internal element table (e.g. 'Name', 'BallotName' or 'Selection')
    if f'{element}_external' not in row_df.columns:
        row_df = add_munged_column(row_df,mu,element,f'{element}_external')
    row_df = row_df.merge(
        mu.raw_identifiers[mu.raw_identifiers['cdf_element'] == element],
        how=how,
        left_on=f'{element}_external',
        right_on='raw_identifier_value',suffixes=['','_' + element + '_ei'])

    # Note: if how = left, unmatched elements get nan in fields from raw_identifiers table
    # TODO how do these nans flow through?

    row_df = row_df.drop(['raw_identifier_value','cdf_element',element + '_external'],axis=1)

    # ensure that there is a column in raw called by the element
    # containing the internal name of the element
    if 'Name_'+element+ '_ei' in row_df.columns:
        row_df.rename(columns={f'Name_{element}_ei':element},inplace=True)
    else:
        row_df.rename(columns={'cdf_internal_name':element},inplace=True)

    # join the element table Id and name columns.
    # This will create two columns with the internal name field,
    # whose names will be element (from above)
    # and either internal_name_column or internal_name_column_table_name
    row_df = row_df.merge(table_df[['Id',internal_name_column]],how='left',left_on=element,right_on=internal_name_column,suffixes=['','_' + element])
    if internal_name_column+'_' + element in row_df.columns:
        row_df=row_df.drop(internal_name_column +'_' + element,axis=1)
    else:
        row_df=row_df.drop([internal_name_column],axis=1)
    row_df.rename(columns={f'Id_{element}':f'{element}_Id'},inplace=True)
    return row_df


def replace_raw_with_internal_ids(row_df,mu,table_df,element,internal_name_column,unmatched_dir,drop_unmatched=False):
    """replace columns in <row_df> with raw_identifier values by columns with internal names and Ids
    from <table_df>, which has structure of a db table for <element>.
    # TODO If <element> is BallotMeasureContest or CandidateContest,
    #  contest_type column is added/updated
    """
    assert os.path.isdir(unmatched_dir), f'Argument {unmatched_dir} is not a directory'
    if drop_unmatched:
        how='inner'
    else:
        how='left'
    # join the 'cdf_internal_name' from the raw_identifier table -- this is the internal name field value,
    # no matter what the name field name is in the internal element table (e.g. 'Name', 'BallotName' or 'Selection')
    row_df = row_df.merge(
        mu.raw_identifiers[mu.raw_identifiers['cdf_element'] == element],
        how=how,
        left_on=f'{element}_raw',
        right_on='raw_identifier_value',suffixes=['',f'_{element}_ei'])

    # Note: if how = left, unmatched elements get nan in fields from raw_identifiers table
    # TODO how do these nans flow through?

    # drop extraneous cols from mu.raw_identifier, and drop original raw
    row_df = row_df.drop(['raw_identifier_value','cdf_element',f'{element}_raw'],axis=1)

    # ensure that there is a column in raw called by the element
    # containing the internal name of the element
    if f'_{element}_ei' in row_df.columns:
        row_df.rename(columns={f'_{element}_ei':element},inplace=True)
    else:
        row_df.rename(columns={'cdf_internal_name':element},inplace=True)

    # join the element table Id and name columns.
    # This will create two columns with the internal name field,
    # whose names will be element (from above)
    # and either internal_name_column or internal_name_column_table_name
    row_df = row_df.merge(
        table_df[['Id',internal_name_column]],how='left',left_on=element,right_on=internal_name_column)
    row_df=row_df.drop([internal_name_column],axis=1)
    row_df.rename(columns={'Id':f'{element}_Id'},inplace=True)
    return row_df


def add_non_id_cols_from_id(row_df,cdf_table,table_name):
    row_df=row_df.merge(cdf_table,left_on=table_name+'_Id',right_on='Id',how='left',suffixes=['','_'+table_name])
    row_df=row_df.drop('Id_'+table_name,axis=1)
    return row_df


def enum_col_from_id_othertext(df,enum,enum_df):
    """Returns a copy of dataframe <df>, replacing id and othertext columns
    (e.g., 'CountItemType_Id' and 'OtherCountItemType)
    with a plaintext <type> column (e.g., 'CountItemType')
        using the enumeration given in <enum_df>"""
    assert f'{enum}_Id' in df.columns,f'Dataframe lackes {enum}_Id column'
    assert f'Other{enum}' in df.columns,f'Dataframe lackes Other{enum} column'
    assert 'Txt' in enum_df.columns ,'Enumeration dataframe should have column \'Txt\''

    # ensure Id is in the index of enum_df (otherwise df index will be lost in merge)
    if 'Id' in enum_df.columns:
        enum_df = enum_df.set_index('Id')

    df = df.merge(enum_df,left_on=f'{enum}_Id',right_index=True)

    # if Txt value is 'other', use Other{enum} value instead
    df['Txt'].mask(df['Txt']!='other', other=df[f'Other{enum}'])
    df.rename(columns={'Txt':enum},inplace=True)
    df.drop([f'{enum}_Id',f'Other{enum}'],axis=1,inplace=True)
    return df


def enum_col_to_id_othertext(df,type_col,enum_df):
    """Returns a copy of dataframe <df>, replacing a plaintext <type_col> column (e.g., 'CountItemType') with
    the corresponding two id and othertext columns (e.g., 'CountItemType_Id' and 'OtherCountItemType
    using the enumeration given in <enum_df>"""
    assert type_col in df.columns
    assert 'Txt' in enum_df.columns and 'Id' in enum_df.columns,'Enumeration dataframe should have columns \'Id\' and \'Txt\''
    for c in ['Id','Txt']:
        if c in df.columns:
            # avoid conflict by temporarily renaming the column in the main dataframe
            assert c*3 not in df.colums, 'Column name '+c*3+' conflicts with variable used in code'
            df.rename(columns={c:c*3},inplace=True)
    df = df.merge(enum_df,how='left',left_on=type_col,right_on='Txt')
    df.rename(columns={'Id':f'{type_col}_Id'},inplace=True)
    df.loc[:,f'Other{type_col}']=''

    other_id_df = enum_df[enum_df['Txt']=='other']
    if not other_id_df.empty:
        other_id = other_id_df.iloc[0]['Id']
        df[f'{type_col}_Id'] = df[f'{type_col}_Id'].fillna(other_id)
        df.loc[df[f'{type_col}_Id'] == other_id,'Other'+type_col] = df.loc[df[f'{type_col}_Id'] == other_id,type_col]
    df = df.drop(['Txt',type_col],axis=1)
    for c in ['Id','Txt']:
        if c*3 in df.columns:
            # avoid restore name renaming the column in the main dataframe
            df.rename(columns={c*3:c},inplace=True)
    return df


def get_enum_value_from_id_othertext(enum_df,id,othertext):
    """Given an enumeration dframe (with cols 'Id' and 'Txt',
    along with an (<id>,<othertext>) pair, find and return the plain language
    value for that enumeration (e.g., 'general')."""
    if othertext != '':
        enum_val = othertext
    else:
        enum_val = enum_df[enum_df.Id == id].loc[:,'Txt'].to_list()[0]
    return enum_val


def get_id_othertext_from_enum_value(enum_df,value):
    """Given an enumeration dframe (with cols 'Id' and 'Txt',
        along with a plain language value for that enumeration
        (e.g., 'general'), return the (<id>,<othertext>) pair."""
    if value in enum_df.Txt.to_list():
        idx = enum_df[enum_df.Txt == value].loc[:,'Id'].to_list()[0]
        other_txt = ''
    else:
        idx = enum_df[enum_df.Txt == 'other'].loc[:,'Id'].to_list()[0]
        other_txt = value
    return idx,other_txt


def good_syntax(s):
    """Returns true if formula string <s> passes certain syntax test(s)"""
    good = True
    # check that angle brackets match
    #  split the string by opening angle bracket:
    split = s.split('<')
    lead = split[0]  # must be free of close angle brackets
    if '>' in lead:
        good = False
        return good
    else:
        p1 = re.compile(r'^\S')  # must start with non-whitespace
        p2 = re.compile(r'^[^>]*\S>[^>]*$')  # must contain exactly one >, preceded by non-whitespace
        for x in split[1:]:
            if not (p1.search(x) and p2.search(x)):
                good = False
                return good
    return good


def raw_elements_to_cdf_NEW(session,project_root,juris,mu,raw,info_cols,num_cols,finalize=True):
    """load data from <raw> into the database.
    Note that columns to be munged (e.g. County_xxx) have mu.field_rename_suffix (e.g., _xxx) added already"""
    working = raw.copy()

    # munge from other sources, including creating id column(s)
    # TODO what if contest_type (BallotMeasure or Candidate) has source 'other'?
    for t,r in mu.cdf_elements[mu.cdf_elements.source == 'other'].iterrows():
        # add column for element id
        idx = ui.pick_or_create_record(session,project_root,t)
        working[f'{t}_Id'] = idx

    # apply munging formula from row sources (after renaming fields in raw formula as necessary)
    for t in mu.cdf_elements[mu.cdf_elements.source == 'row'].index:
        working = add_munged_column_NEW(working,mu,t,mode='row')
        # TODO this finalization lumps BMContest and CContests together, not quite right
        if finalize == True:
            if t != 'BallotMeasureSelection':  # TODO ad hoc
                mu.finalize_element(t,working,juris,session,project_root)

    # remove original row-munge columns
    munged = [x for x in working.columns if x[-len(mu.field_rename_suffix):] == mu.field_rename_suffix]
    working.drop(munged,axis=1,inplace=True)

    # if there is just one numerical column, melt still creates dummy variable col
    #  in which each value is 'value'

    # reshape
    non_count_cols = [x for x in working.columns if x not in num_cols]
    working = working.melt(id_vars=non_count_cols)
    # rename count to Count
    #  if only one header row, rename variable to variable_0 for consistency
    #  NB: any unnecessary numerical cols (e.g., Contest Group ID) will not matter
    #  as they will be be missing from raw_identifiers.txt and hence will be ignored.
    # TODO check and correct: no num col names conflict with raw identifiers of column-source
    #  elements!
    working.rename(columns={'variable':'variable_0','value':'Count'},inplace=True)

    # apply munge formulas for column sources
    for t in mu.cdf_elements[mu.cdf_elements.source == 'column'].index:
        working = add_munged_column_NEW(working,mu,t,mode='column')
    # remove unnecessary columns
    not_needed = [f'variable_{i}' for i in range(mu.header_row_count)]
    working.drop(not_needed,axis=1,inplace=True)

    # append ids for BallotMeasureContests and CandidateContests
    working.loc[:,'contest_type'] = 'unknown'
    for c_type in ['BallotMeasure','Candidate']:
        # TODO make sure BallotMeasureContest gets ElectionDistrict_Id when it is inserted
        # TODO if CandidateContest.txt is loaded to db, don't load candidatecontests from Office.txt
        df_contest = pd.read_sql_table(f'{c_type}Contest',session.bind)
        working = replace_raw_with_internal_ids(
            working,mu,df_contest,f'{c_type}Contest',get_name_field(f'{c_type}Contest'),mu.path_to_munger_dir,
            drop_unmatched=False)

        # set contest_type where id was found
        working.loc[working[f'{c_type}Contest_Id'].notnull(),'contest_type'] = c_type

        # empty {type}Contest columns where id was not found
        # working.loc[working[f'{c_type}Contest_Id'].isnull(),f'{c_type}Contest'] = ''
        # working.loc[working[f'{c_type}Contest_Id'].isnull(),f'{c_type}Selection_raw'] = ''

        # drop column with munged name
        working.drop(f'{c_type}Contest',axis=1,inplace=True)

    # drop rows with unmatched contests
    working = working[working['contest_type'] != 'unknown']

    # get ids for remaining info sourced from rows and columns
    element_list = [t for t in mu.cdf_elements[mu.cdf_elements.source != 'other'].index if
                    (t[-7:] != 'Contest' and t[-9:] != 'Selection')]
    for t in element_list:
        # capture id from db in new column and erase any now-redundant cols
        df = pd.read_sql_table(t,session.bind)
        name_field = get_name_field(t)
        # NB: drop_unmatched = False to prevent losing BallotMeasureContests for BM-inessential fields
        if t == 'ReportingUnit' or t == 'CountItemType':
            drop = True
        else:
            drop = False
        working = replace_raw_with_internal_ids(working,mu,df,t,name_field,mu.path_to_munger_dir,drop_unmatched=drop)
        working.drop(t,axis=1,inplace=True)
        # working = add_non_id_cols_from_id(working,df,t)

    # append BallotMeasureSelection_Id, drop BallotMeasureSelection
    df_selection = pd.read_sql_table(f'BallotMeasureSelection',session.bind)
    working = replace_raw_with_internal_ids(
        working,mu,df_selection,'BallotMeasureSelection',get_name_field('BallotMeasureSelection'),
        mu.path_to_munger_dir,
        drop_unmatched=False)
    working.drop('BallotMeasureSelection',axis=1,inplace=True)

    # append CandidateSelection_Id
    #  First must load CandidateSelection table (not directly munged, not exactly a join either)
    #  Note left join, as not every record in working has a Candidate_Id
    # TODO maybe introduce Selection and Contest tables, have C an BM types refer to them?
    c_df = pd.read_sql_table('Candidate',session.bind)
    c_df.rename(columns={'Id':'Candidate_Id'},inplace=True)
    cs_df = dbr.dframe_to_sql(c_df,session,None,'CandidateSelection',return_records='original')
    # add CandidateSelection_Id column, merging on Candidate_Id
    working = working.merge(
        cs_df[['Candidate_Id','Id']],how='left',left_on='Candidate_Id',right_on='Candidate_Id')
    working.rename(columns={'Id':'CandidateSelection_Id'},inplace=True)

    for j in ['BallotMeasureContestSelectionJoin','CandidateContestSelectionJoin','ElectionContestJoin']:
        working = append_join_id(project_root,session,working,j)

    # Fill VoteCount and ElectionContestSelectionVoteCountJoin
    #  To get 'VoteCount_Id' attached to the correct row, temporarily add columns to VoteCount
    #  add ElectionContestSelectionVoteCountJoin columns to VoteCount

    # Define ContestSelectionJoin_Id field needed in ElectionContestSelectionVoteCountJoin
    ref_d = {'ContestSelectionJoin_Id':['BallotMeasureContestSelectionJoin_Id','CandidateContestSelectionJoin_Id']}
    working = append_multi_foreign_key(working,ref_d)

    # add extra columns to VoteCount table temporarily to allow proper join
    extra_cols = ['ElectionContestJoin_Id','ContestSelectionJoin_Id']
    dbr.add_integer_cols(session,'VoteCount',extra_cols)

    # upload to VoteCount table, pull  Ids
    working_fat = dbr.dframe_to_sql(working,session,None,'VoteCount',raw_to_votecount=True)
    working_fat.rename(columns={'Id':'VoteCount_Id'},inplace=True)
    session.commit()

    # upload to ElectionContestSelectionVoteCountJoin
    dbr.dframe_to_sql(working_fat,session,None,'ElectionContestSelectionVoteCountJoin')

    # drop extra columns
    dbr.drop_cols(session,'VoteCount',extra_cols)

    # TODO CandidateContest must get Office_Id
    return


def append_join_id(project_root,session,working,j):
    """Upload join data to db, get Ids,
    Append <join>_Id to <working>. Unmatched rows are kept"""
    j_path = os.path.join(
        project_root,'election_anomaly/CDF_schema_def_info/joins',j,'foreign_keys.txt')
    join_fk = pd.read_csv(j_path,sep='\t',index_col='fieldname')
    join_fk.loc[:,'refers_to_list'] = join_fk.refers_to.str.split(';')
    # create dataframe with cols named to match join table, content from corresponding column of working

    ww_cols = set().union(*join_fk.refers_to_list)  # all referents of cols in join table
    w_cols = [f'{x}_Id' for x in ww_cols]
    j_cols = list(join_fk.index)
    # drop dupes and any null rows
    join_df = working[w_cols].drop_duplicates(keep='first')  # take only nec cols from working and dedupe

    ref_d = {}
    for fn in join_fk.index:
        ref_d[fn] = [f'{x}_Id' for x in join_fk.loc[fn,'refers_to_list']]
    join_df = append_multi_foreign_key(join_df,ref_d)
    working = append_multi_foreign_key(working,ref_d)

    # remove any join_df rows with any *IMPORTANT* null and load to db
    unnecessary = [x for x in w_cols if x not in j_cols]
    join_df.drop(unnecessary,axis=1,inplace=True)

    join_df = join_df[join_df.notnull().all(axis=1)]
    join_df = dbr.dframe_to_sql(join_df,session,None,j)
    working = working.merge(join_df,how='left',left_on=j_cols,right_on=j_cols)
    working.rename(columns={'Id':f'{j}_Id'},inplace=True)
    return working


def append_multi_foreign_key(df,references):
    """<references> is a dictionary whose keys are fieldnames for the new column and whose value for any key
    is the list of reference targets.
    If a row in df has more than one non-null value, only the first will be taken."""
    df_copy = df.copy()
    for fn in references.keys():
        df_copy.loc[:,fn] = df_copy[references[fn]].fillna(-1).max(axis=1)
        # change any -1s back to nulls
        df_copy.loc[:,fn]=df_copy[fn].replace(-1,np.NaN)
    return df_copy


if __name__ == '__main__':
    p_root = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/'
    # pick db to use
    db_paramfile = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/database.ini'

    db_name = 'NC_2'

    # connect to db
    from sqlalchemy.orm import sessionmaker
    eng,meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
    Session = sessionmaker(bind=eng)
    sess = Session()

    ww = pd.read_csv('/Users/Steph-Airbook/Library/Preferences/PyCharm2019.3/scratches/tmp.txt',sep='\t')
    for j in ['ElectionContestJoin']:
        ww = append_join_id(p_root,sess,ww,j)
    print('Done!')
    exit()
