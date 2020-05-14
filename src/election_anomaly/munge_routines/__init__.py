#!/usr/bin/python3
# munge_routines/__init__.py
# under construction

import db_routines as dbr
import user_interface as ui
import pandas as pd
import re
import os
import numpy as np


def clean_raw_df(raw,munger):
    """Replaces nulls, strips whitespace, changes any blank entries in non-numeric columns to 'none or unknown'.
    Appends munger suffix to raw column names to avoid conflicts"""
    # TODO put all info about data cleaning into README.md (e.g., whitespace strip)

    # change all nulls to blank
    raw = raw.fillna('')
    # strip whitespace
    raw = raw.applymap(lambda x:x.strip())

    # TODO keep columns named in munger formulas; keep numerical columns; drop all else.
    if munger.header_row_count > 1:
        cols_to_munge = [x for x in raw.columns if x[munger.field_name_row] in munger.field_list]
    else:
        cols_to_munge = [x for x in raw.columns if x in munger.field_list]

    # TODO error check- what if cols_to_munge is missing something from munger.field_list?

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
    # recast all cols_to_munge to strings,
    # change all blanks to "none or unknown"
    for c in cols_to_munge:
        raw[c] = raw[c].apply(str)
        raw[c] = raw[c].replace('','none or unknown')
    # rename columns to munge by adding suffix
    renamer = {x:f'{x}_{munger.field_rename_suffix}' for x in cols_to_munge}
    raw.rename(columns=renamer,inplace=True)
    renamed_cols_to_munge = [f'{x}_{munger.field_rename_suffix}' for x in cols_to_munge]
    return raw, renamed_cols_to_munge, num_columns


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


def add_munged_column(raw,munger,element,mode='row',inplace=True):
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


def replace_raw_with_internal_ids(row_df,juris,table_df,element,internal_name_column,unmatched_dir,drop_unmatched=False):
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
    # use dictionary.txxt from context

    raw_identifiers = pd.read_csv(os.path.join(juris.path_to_juris_dir,'context/dictionary.txt'),sep='\t')
    row_df = row_df.merge(
        raw_identifiers[raw_identifiers['cdf_element'] == element],
        how=how,
        left_on=f'{element}_raw',
        right_on='raw_identifier_value',suffixes=['',f'_{element}_ei'])

    # Note: if how = left, unmatched elements get nan in fields from dictionary table
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


def enum_col_from_id_othertext(df,enum,enum_df,drop_old=True):
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
    if drop_old:
        df.drop([f'{enum}_Id',f'Other{enum}'],axis=1,inplace=True)
    return df


def enum_col_to_id_othertext(df,type_col,enum_df,drop_old=True):
    """Returns a copy of dataframe <df>, replacing a plaintext <type_col> column (e.g., 'CountItemType') with
    the corresponding two id and othertext columns (e.g., 'CountItemType_Id' and 'OtherCountItemType
    using the enumeration given in <enum_df>"""
    if df.empty:
        # add two columns
        df[f'{type_col}_Id'] = df[f'Other{type_col}'] = df.iloc[:,0]
    else:
        assert type_col in df.columns
        assert 'Txt' in enum_df.columns ,'Enumeration dataframe should have a \'Txt\' column'

        # ensure Id is a column, not the index, of enum_df (otherwise df index will be lost in merge)
        if 'Id' not in enum_df.columns:
            enum_df['Id'] = enum_df.index

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
        df = df.drop(['Txt'],axis=1)
        for c in ['Id','Txt']:
            if c*3 in df.columns:
                # avoid restore name renaming the column in the main dataframe
                df.rename(columns={c*3:c},inplace=True)
    if drop_old:
        df = df.drop([type_col],axis=1)
    return df


def enum_value_from_id_othertext(enum_df,id,othertext):
    """Given an enumeration dframe (with cols 'Id' and 'Txt', or index and column 'Txt'),
    along with an (<id>,<othertext>) pair, find and return the plain language
    value for that enumeration (e.g., 'general')."""

    # ensure Id is a column, not the index, of enum_df (otherwise df index will be lost in merge)
    if 'Id' not in enum_df.columns:
        enum_df['Id'] = enum_df.index

    if othertext != '':
        enum_val = othertext
    else:
        enum_val = enum_df[enum_df.Id == id].loc[:,'Txt'].to_list()[0]
    return enum_val


def enum_value_to_id_othertext(enum_df,value):
    """Given an enumeration dframe,
        along with a plain language value for that enumeration
        (e.g., 'general'), return the (<id>,<othertext>) pair."""
    # ensure Id is in the index of enum_df (otherwise df index will be lost in merge)
    if 'Id' in enum_df.columns:
        enum_df = enum_df.set_index('Id')

    if value in enum_df.Txt.to_list():
        idx = enum_df[enum_df.Txt == value].first_valid_index()
        other_txt = ''
    else:
        idx = enum_df[enum_df.Txt == 'other'].first_valid_index()
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


def munge_and_melt(mu,raw,num_cols):
    """Does not alter raw; returns Transformation of raw:
     all row- and column-sourced mungeable info into columns (but doesn't translate via dictionary)
    new column names are, e.g., ReportingUnit_raw, Candidate_raw, etc.
    """
    working = raw.copy()

    # apply munging formula from row sources (after renaming fields in raw formula as necessary)
    for t in mu.cdf_elements[mu.cdf_elements.source == 'row'].index:
        working = add_munged_column(working,mu,t,mode='row')

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
        working = add_munged_column(working,mu,t,mode='column')
    # remove unnecessary columns
    not_needed = [f'variable_{i}' for i in range(mu.header_row_count)]
    working.drop(not_needed,axis=1,inplace=True)

    return working


def raw_elements_to_cdf(session,project_root,juris,mu,raw,num_cols):
    """load data from <raw> into the database.
    Note that columns to be munged (e.g. County_xxx) have mu.field_rename_suffix (e.g., _xxx) added already"""
    working = raw.copy()

    # enter elements from sources outside raw data, including creating id column(s)
    # TODO what if contest_type (BallotMeasure or Candidate) has source 'other'?
    for t,r in mu.cdf_elements[mu.cdf_elements.source == 'other'].iterrows():
        # add column for element id
        idx = ui.pick_or_create_record(session,project_root,t)
        working[f'{t}_Id'] = idx

    working = munge_and_melt(mu,working, num_cols)

    # append ids for BallotMeasureContests and CandidateContests
    working.loc[:,'contest_type'] = 'unknown'
    for c_type in ['BallotMeasure','Candidate']:
        # TODO make sure BallotMeasureContest gets ElectionDistrict_Id when it is inserted
        # TODO if CandidateContest.txt is loaded to db, don't load candidatecontests from Office.txt
        df_contest = pd.read_sql_table(f'{c_type}Contest',session.bind)
        working = replace_raw_with_internal_ids(
            working,juris,df_contest,f'{c_type}Contest',get_name_field(f'{c_type}Contest'),mu.path_to_munger_dir,
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
        working = replace_raw_with_internal_ids(working,juris,df,t,name_field,mu.path_to_munger_dir,drop_unmatched=drop)
        working.drop(t,axis=1,inplace=True)
        # working = add_non_id_cols_from_id(working,df,t)

    # append BallotMeasureSelection_Id, drop BallotMeasureSelection
    df_selection = pd.read_sql_table(f'BallotMeasureSelection',session.bind)
    working = replace_raw_with_internal_ids(
        working,juris,df_selection,'BallotMeasureSelection',get_name_field('BallotMeasureSelection'),
        mu.path_to_munger_dir,
        drop_unmatched=False)
    working.drop('BallotMeasureSelection',axis=1,inplace=True)

    # append CandidateSelection_Id
    #  First must load CandidateSelection table (not directly munged, not exactly a join either)
    #  Note left join, as not every record in working has a Candidate_Id
    # TODO maybe introduce Selection and Contest tables, have C an BM types refer to them?
    c_df = pd.read_sql_table('Candidate',session.bind)
    c_df.rename(columns={'Id':'Candidate_Id'},inplace=True)
    cs_df = dbr.dframe_to_sql(c_df,session,'CandidateSelection',return_records='original')
    # add CandidateSelection_Id column, merging on Candidate_Id
    working = working.merge(
        cs_df[['Candidate_Id','Id']],how='left',left_on='Candidate_Id',right_on='Candidate_Id')
    working.rename(columns={'Id':'CandidateSelection_Id'},inplace=True)

    # TODO: warn user if contest is munged but candidates are not
    # TODO warn user if BallotMeasureSelections not recognized in dictionary.txt
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
    working_fat = dbr.dframe_to_sql(working,session,'VoteCount',raw_to_votecount=True)
    working_fat.rename(columns={'Id':'VoteCount_Id'},inplace=True)
    session.commit()

    # TODO check that all candidates in munged contests (including write ins!) are munged
    # upload to ElectionContestSelectionVoteCountJoin
    dbr.dframe_to_sql(working_fat,session,'ElectionContestSelectionVoteCountJoin')

    # drop extra columns
    dbr.drop_cols(session,'VoteCount',extra_cols)

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
    join_df = dbr.dframe_to_sql(join_df,session,j)
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
    pass
    exit()
