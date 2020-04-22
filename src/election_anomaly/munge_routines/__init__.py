#!/usr/bin/python3
# munge_routines/__init__.py
# under construction

import db_routines as dbr
import user_interface as ui
import pandas as pd
import time
import re
import os


def clean_raw_df(raw,munger):
    # TODO put all info about data cleaning into README.md (e.g., whitespace strip)

    # change all nulls to 0
    raw = raw.fillna(0)
    # strip whitespace
    # raw = raw.applymap(lambda x:x.strip())

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
    # TODO check that this can be used to update the db as well as initialize it

    # dedupe source_df
    dupes,source_df = ui.find_dupes(source_df1)
    if not dupes.empty:
        print(f'WARNING: duplicates removed from dataframe, may indicate a problem.\n{source_df1}')

    enum_file = os.path.join(cdf_schema_def_dir,'Tables',element,'enumerations.txt')
    if os.path.isfile(enum_file):  # (if not, there are no enums for this element)
        enums = pd.read_csv(os.path.join(cdf_schema_def_dir,'Tables',element,'enumerations.txt'),sep='\t')
        # get all relevant enumeration tables
        for e in enums['enumeration']:  # e.g., e = "ReportingUnitType"
            cdf_e = pd.read_sql_table(e,session.bind)
            # for every instance of the enumeration in the current table, add id and othertype columns to the dataframe
            if e in source_df.columns:
                source_df = enum_col_to_id_othertext(source_df,e,cdf_e)
        # TODO skipping assignment of CountItemStatus to ReportingUnit for now,
        # TODO since we can't assign an ReportingUnit as ElectionDistrict to Office
        #  (unless Office has a CountItemStatus; can't be right!)
        # TODO note CountItemStatus is weirdly assigned to ReportingUnit in NIST CDF.
        #  Note also that CountItemStatus is not required, and a single RU can have many CountItemStatuses

    #  commit info in source_df to corresponding cdf table to db
    dbr.dframe_to_sql(source_df,session,None,element)
    if element == 'Office':
        # upload ReportingUnits from context/ReportingUnit.txt to db and upload corresponding CandidateContests too

        # ReportingUnits
        ru = pd.read_csv(os.path.join(jurisdiction.path_to_juris_dir,'context/ReportingUnit.txt'),sep='\t')
        nulls_ok = False
        while not nulls_ok:
            # find any rows that have nulls, ask user to fix.
            nulls = ru[ru.isnull().any(axis=1)]
            if nulls.empty:
                nulls_ok = True
            else:
                input(
                    f'The file context/ReportingUnit.txt has some blank or null entries.\n'
                    f'Fill blanks, or erase rows with blanks and hit return to continue.\n')
                ru = pd.read_csv(os.path.join(jurisdiction.path_to_juris_dir,'context/ReportingUnit.txt'),sep='\t')

        cdf_rut = pd.read_sql_table('ReportingUnitType',session.bind)
        ru = enum_col_to_id_othertext(ru,'ReportingUnitType',cdf_rut)
        cdf_ru = dbr.dframe_to_sql(ru,session,None,'ReportingUnit')

        eds_ok = False
        while not eds_ok:
            # find any ElectionDistricts that are not in the cdf ReportingUnit table.
            cdf_ru = pd.read_sql_table('ReportingUnit',session.bind,None,index_col=None)
            # note: cdf Id column is *not* the index for the dataframe cdf_d['ReportingUnit'].

            office_ed = source_df.drop(['Name'],axis=1)
            ru_list = list(cdf_ru['Name'].unique())
            new_ru = office_ed[~office_ed['ElectionDistrict'].isin(ru_list)]

            if not new_ru.empty:
                ui.show_sample(list(new_ru.ElectionDistrict.unique()),'office election districts',
                               condition='are not in the ReportingUnit table of the common data format',
                               outfile='missing_reportingunits.txt',
                               dir=os.path.join(jurisdiction.path_to_juris_dir,'output'))
                input(f'Please add any missing Election Districts to context/ReportingUnit.txt and hit return to continue.\n')
                rut_list = list(cdf_rut['Txt'])
                ru = ui.fill_context_file(
                    os.path.join(jurisdiction.path_to_juris_dir,'context'),
                    os.path.join(project_root,'templates/context_templates'),
                    'ReportingUnit',rut_list,'ReportingUnitType')
                #  then upload to db
                ru = enum_col_to_id_othertext(ru,'ReportingUnitType',cdf_rut)
                cdf_ru = dbr.dframe_to_sql(ru,session,None,'ReportingUnit')
                ru_list = list(cdf_ru['Name'].unique())
            else:
                eds_ok = True
        # CandidateContests
        jurisdiction.prepare_candidatecontests(session)
    return


def add_munged_column(raw,mu,element,new_col_name):
    """Alters dataframe <raw> (in place), adding or redefining <new_col_name>
    via the string corresponding to <cdf_element>, per <munge_dictionary>"""
    if raw.empty:
        return raw
    else:
        raw_copy = raw.copy()

        # use regex to turn value string in munge dictionary into the corresponding commands (e.g., decode '<County>;<Precinct>'
        p = re.compile('(?P<text>[^<>]*)<(?P<field>[^<>]+)>')   # pattern to find text,field pairs
        q = re.compile('(?<=>)[^<]*$')                          # pattern to find text following last pair
        text_field_list = re.findall(p,mu.cdf_tables.loc[element,'raw_identifier_formula'])
        last_text = re.findall(q,mu.cdf_tables.loc[element,'raw_identifier_formula'])

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


def add_munged_column_NEW(raw,formula,element):
    """Alters dataframe <raw> (in place), adding or redefining <element> column
    via the <formula>"""
    if raw.empty:
        return raw
    else:
        text_field_list,last_text = text_fragments_and_fields(formula)

        if last_text:
            raw.loc[:,element] = last_text[0]
        else:
            raw.loc[:,element] = ''

        text_field_list.reverse()
        for t,f in text_field_list:
            assert f != element,f'Column name conflicts with element name: {f}'
            raw.loc[:,element] = t + raw.loc[:,f] + raw.loc[:,element]
        return raw


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
# TODO check index of enum_df is 'Id'
    df = df.merge(enum_df,left_on=f'{enum}_Id',right_index=True)
    # TODO if Txt value is 'other', use Other{enum} value instead

    df['Txt'].mask(df['Txt']!='other', other=df[f'Other{enum}'])
    df.rename(columns={'Txt':enum},inplace=True)
    df.drop([f'{enum}_Id',f'Other{enum}'],axis=1,inplace=True)
    return df


def enum_col_to_id_othertext(df,type_col,enum_df):
    """Returns a copy of dataframe <df>, replacing a plaintext <type> column (e.g., 'CountItemType') with
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


def raw_elements_to_cdf_NEW(session,project_root,mu,raw,info_cols,num_cols):
    """load data from <raw> into the database.
    Note that columns to be munged (e.g. County_xxx) have mu.field_rename_suffix (e.g., _xxx) added already"""
    working = raw.copy()

    # munge from other sources
    for t,r in mu.cdf_elements[mu.cdf_elements.source == 'other'].iterrows():
        # add column for element id
        idx = ui.pick_or_create_record(session,project_root,t)
        working[f'{t}_Id'] = idx

    # munge info from row sources (after renaming fields in raw formula as necessary)
    for t,r in mu.cdf_elements[mu.cdf_elements.source == 'row'].iterrows():
        formula = r['raw_identifier_formula']
        for field in r['fields']:
            formula = formula.replace(field,f'{field}_{mu.field_rename_suffix}')
        working = add_munged_column_NEW(working,formula,t)

    # remove original row-munge columns
    munged = [x for x in working.columns if x[-len(mu.field_rename_suffix):] == mu.field_rename_suffix]
    working.drop(munged,axis=1,inplace=True)

    # if there is just one numerical column, melt still creates dummy variable col
    #  in which each value is 'value'
    # reshape
    non_num_cols = [x for x in working.columns if x not in num_cols]
    working = working.melt(id_vars=non_num_cols)
    # rename count to VoteCount
    #  if only one header row, rename variable to variable_0 for consistency
    #  NB: any unnecessary numerical cols (e.g., Contest Group ID) will not matter
    #  as they will be be missing from raw_identifiers.txt and hence will be ignored.
    # TODO check and correct: no num col names conflict with raw identifiers of column-source
    #  elements!
    working.rename(columns={'variable':'variable_0','value':'VoteCount'},inplace=True)

    # munge from column sources
    for t,r in mu.cdf_elements[mu.cdf_elements.source == 'column'].iterrows():
        formula = r['raw_identifier_formula']
        for i in range(mu.header_row_count):
            formula = formula.replace(f'{i}',f'variable_{i}')
        working = add_munged_column_NEW(working,formula,t)
    # remove unnecessary columns
    not_needed = [f'variable_{i}' for i in range(mu.header_row_count)]
    working.drop(not_needed,axis=1,inplace=True)

    # get lists of table names from db
    [cdf_elements,cdf_enumerations,cdf_joins,others] = dbr.get_cdf_db_table_names(session.bind)

    # loop through all cdf_element tables t (respecting foreign key ordering)
        # load raw data into t and pull t.Ids
    for t in cdf_elements:
        # TODO
        # load raw data into context if necessary
        # load raw data into db if necessary
        # capture id from db in new column
        pass
    # erase all but id and vote-count columns from working
    # loop through all join tables j (respecting foreign key ordering) -- except ECSVCJoin
        # load raw data into j and pull j.Ids

    # Fill VoteCount and ElectionContestSelectionVoteCountJoin
    #  To get 'VoteCount_Id' attached to the correct row, temporarily add columns to VoteCount
    #  add ElectionContestSelectionVoteCountJoin columns to VoteCount

    # TODO
    return


def raw_elements_to_cdf_OLD(session,mu,row,contest_type,election_id,election_type,juris_id):
    """
    NOTE: Tables from context assumed to exist already in db
    (e.g., Party, ComposingReportingUnitJoin, Election, ReportingUnit etc.)
    """
    # TODO some of this could be done once for both BallotMeasure and Candidate contests. Rearrange for efficiency?
    assert contest_type in ['BallotMeasure','Candidate'], f'Contest type {contest_type} not recognized'

    # change any empty count_column values to zero
    c = mu.count_columns.RawName.to_list()
    row[c] = row[c].applymap(lambda x: '0' if x=='' else x)

    cdf_d = {}  # dataframe for each table
    for element in ['Party','BallotMeasureSelection','ReportingUnit','Office','CountItemType','CandidateContest']:
        cdf_d[element] = pd.read_sql_table(element, session.bind)
        # note: keep 'Id as df column (not index) so we have access in merges below.

    vc_col_d = {x['RawName']:x['CountItemType'] for i,x in mu.count_columns.iterrows()}

    col_list = list(vc_col_d.values()) + ['Election_Id','ReportingUnit_Id',
                                          'ReportingUnitType_Id', 'OtherReportingUnitType',
                                          'Selection_Id','Contest_Id']
                                    # TODO feature: do we need CountItemStatus_Id or OtherCountItemStatus in col_list?
    vote_type_list=list({v for k,v in vc_col_d.items()})

    # add columns corresponding to cdf fields
    # election id
    row.loc[:,'Election_Id'] = election_id

    # to make sure all added columns get labeled well, make sure 'Name' and 'Id' are existing columns
    if 'Name' not in row.columns:
        row.loc[:,'Name'] = None
    if 'Id' not in row.columns:
        row.loc[:,'Id'] = None

    element_list = ['ReportingUnit']
    if contest_type == 'Candidate':
        element_list.append('Party')
    for element in element_list:
        #  merge works for all columns, even if, say, Party is null, because of how='left' (left join) in add_munged_column function
        row=get_internal_ids(row,mu,cdf_d[element],element,"Name",mu.path_to_munger_dir)
        row=add_non_id_cols_from_id(row,cdf_d[element],element)

    if contest_type=='BallotMeasure':
        add_munged_column(row,mu,'BallotMeasureContest','BallotMeasureContest')

        # create DataFrame bm_contest_selections of contest-selection-id tuples
        if mu.ballot_measure_style == 'yes_and_no_are_columns':
            bmc={}
            new_rows={}
            bm_contests = row['BallotMeasureContest'].drop_duplicates()
            for sel in ['Yes','No']:    # internal BallotMeasureSelection options
                bmc[sel]=pd.DataFrame(zip(bm_contests,[sel] * len(bm_contests)),
                                      columns=['BallotMeasureContest','BallotMeasureSelection'])
                new_rows[sel]=row.copy()
                # add column for selection
                new_rows[sel].loc[:,'BallotMeasureSelection'] = sel
                # melt vote counts into appropriately titled columns
                for vt in vote_type_list:
                    vt_df = mu.count_columns[mu.count_columns.CountItemType==vt].merge(
                        mu.ballot_measure_count_column_selections,left_on='RawName',right_on='fieldname'
                    )
                    raw_name = vt_df[vt_df.selection==sel].iloc[0]['RawName']
                    new_rows[sel].loc[:,vt]=row[raw_name]
            row = pd.concat([new_rows[k] for k in new_rows.keys()])
            bm_contest_selection = pd.concat([bmc[k] for k in bmc.keys()])

        elif mu.ballot_measure_style == 'yes_and_no_are_candidates':
            add_munged_column(row,mu,'BallotMeasureSelection','BallotMeasureSelection_external')
            row = get_internal_ids(
                row,mu,cdf_d['BallotMeasureSelection'],'BallotMeasureSelection','Selection',mu.path_to_munger_dir)
            bm_contest_selection = row[['BallotMeasureContest','BallotMeasureSelection']].drop_duplicates()
            row.rename(columns=vc_col_d,inplace=True)  # standardize vote-count column names
        else:
            raise Exception(f'Ballot measure style \'{mu.ballot_measure_style}\' not recognized')
        bm_contest_selection.columns = ['Name','Selection']
        # internal db name for ballot measure contest matches name in file
        bm_contest_selection.loc[:,'ElectionDistrict_Id'] = juris_id  # append column for ElectionDistrict Id
        print('WARNING: all ballot measure contests assumed to have the whole datafile jurisdiction as their district')

        # Load BallotMeasureContest table to cdf schema
        cdf_d['BallotMeasureContest'] = dbr.dframe_to_sql(bm_contest_selection[['Name','ElectionDistrict_Id']]
                                                          .drop_duplicates(),session,None,'BallotMeasureContest')

        # add BallotMeasure-related ids needed later
        bm_row = row.merge(cdf_d['BallotMeasureSelection'],left_on='BallotMeasureSelection',
                           right_on='Selection',suffixes=['','_Selection'])
        bm_row.rename(columns={'Id_Selection':'Selection_Id'},inplace=True)
        bm_row = bm_row.merge(cdf_d['BallotMeasureContest'],left_on='BallotMeasureContest',
                              right_on='Name',suffixes=['','_Contest'])
        bm_row.rename(columns={'Id_Contest':'Contest_Id'},inplace=True)

        # Load BallotMeasureContestSelectionJoin table to cdf schema
        bmcsj_df = bm_row[['Contest_Id','Selection_Id']].drop_duplicates()
        bmcsj_df.rename(columns={'Contest_Id':'BallotMeasureContest_Id','Selection_Id':'BallotMeasureSelection_Id'},
                        inplace=True)
        cdf_d['BallotMeasureContestSelectionJoin'] = dbr.dframe_to_sql(bmcsj_df,session,None,
                                                                       'BallotMeasureContestSelectionJoin')

        # Load ElectionContestJoin table (for ballot measures)
        ecj_df = cdf_d['BallotMeasureContest'].copy()
        ecj_df.loc[:,'Election_Id'] = election_id
        ecj_df.rename(columns={'Id': 'Contest_Id'}, inplace=True)
        cdf_d['ElectionContestJoin'] = dbr.dframe_to_sql(ecj_df,session,None,'ElectionContestJoin',return_records='original')

        # create dframe of vote counts (with join info) for ballot measures
        bm_vote_counts = bm_row[col_list]

        bm_vote_counts=bm_vote_counts.melt(
            id_vars=['Election_Id','Contest_Id','Selection_Id','ReportingUnit_Id'],
            value_vars=vote_type_list,var_name='CountItemType',value_name='Count')
        bm_vote_counts=enum_col_to_id_othertext(bm_vote_counts,'CountItemType',cdf_d['CountItemType'])
        vote_counts = bm_vote_counts

    elif contest_type=='Candidate':
        # process rows with candidate contests
        # Find CandidateContest_external and CandidateContest_Id
        # and omit rows with contests not  given in CandidateContest table (filled from context)
        add_munged_column(row,mu,'CandidateContest','CandidateContest_external')
        cc_row=get_internal_ids(
            row,mu,cdf_d['CandidateContest'],'CandidateContest','Name',mu.path_to_munger_dir,drop_unmatched=True)
        cc_row.rename(columns={'CandidateContest_Id':'Contest_Id'},inplace=True)

        # load Candidate table
        add_munged_column(cc_row,mu,'Candidate','BallotName')
        candidate_df = cc_row[['BallotName','Party_Id','Election_Id']].copy().drop_duplicates()
        candidate_df = candidate_df[candidate_df['BallotName'].notnull()]
        # TODO add  other notnull criteria
        cdf_d['Candidate'] = dbr.dframe_to_sql(candidate_df,session,None,'Candidate')

        # load CandidateSelection
        cs_df = cdf_d['Candidate'].copy()
        cs_df.rename(columns={'Id':'Candidate_Id'},inplace=True)
        cdf_d['CandidateSelection'] = dbr.dframe_to_sql(cs_df,session,None,'CandidateSelection')


        # add Candidate ids needed later
        cc_row = cc_row.merge(cs_df,left_on='BallotName',right_on='BallotName',
                              suffixes=['','_Candidate'])
        cc_row.rename(columns={'Id_Candidate':'Candidate_Id'},inplace=True)
        cc_row = cc_row.merge(cdf_d['CandidateSelection'],left_on='Candidate_Id',
                              right_on='Candidate_Id',suffixes=['','_Selection'])
        cc_row.rename(columns={'Id_Selection':'Selection_Id'},inplace=True)

        # Office depends on the election type and the CandidateContest.
        if election_type == 'general':
            # Office name is same as CandidateContest name
            cc_row['Office'] = cc_row['CandidateContest']
        elif election_type == 'primary':
            # Office name is derived from CandidateContest name
            cc_row['Office'] = cc_row['CandidateContest'].str.split(' Primary;').iloc[0][0]
        else:
            raise Exception(f'Election type {election_type} not recognized by the code') # TODO add all election types
        cc_row.merge(cdf_d['Office'],left_on='Office',right_on='Name',suffixes=['','_Office'])
        cc_row.rename(columns={'Id_Office':'Office_Id'},inplace=True)

        # load ElectionContestJoin for Candidate Contests
        ecj_df = cc_row[['Contest_Id','Election_Id']].drop_duplicates()
        cdf_d['ElectionContestJoin'] = dbr.dframe_to_sql(ecj_df,session,None,'ElectionContestJoin')

        #  load CandidateContestSelectionJoin
        ccsj_df = cc_row[['Contest_Id','Selection_Id']].drop_duplicates()
        ccsj_df.rename(columns={'Contest_Id':'CandidateContest_Id','Selection_Id':'CandidateSelection_Id'},inplace=True)
        cdf_d['CandidateContestSelectionJoin'] = dbr.dframe_to_sql(ccsj_df,session,None,'CandidateContestSelectionJoin')

        # load candidate counts
        cc_row.rename(columns=vc_col_d,inplace=True)  # standardize vote-count column names
        cc_vote_counts = cc_row[col_list]
        # TODO for Florida, why is there no 'Count' column for Candidate contest_type?
        cc_vote_counts=cc_vote_counts.melt(
            id_vars=['Election_Id','Contest_Id','Selection_Id','ReportingUnit_Id'],
            value_vars=vote_type_list,var_name='CountItemType',value_name='Count')
        cc_vote_counts=enum_col_to_id_othertext(cc_vote_counts,'CountItemType',cdf_d['CountItemType'])
        vote_counts = cc_vote_counts

    else:
        raise Exception(f'Contest type {contest_type} not recognized.')
    # TODO do we need to check whether vote_counts is empty?

    # To get 'VoteCount_Id' attached to the correct row, temporarily add columns to VoteCount
    # add SelectionElectionContestJoin columns to VoteCount
    extra_cols = ['Election_Id','Contest_Id','Selection_Id']
    dbr.add_integer_cols(session,'VoteCount',extra_cols)
    print('Upload to VoteCount')
    start = time.time()
    vote_counts_fat = dbr.dframe_to_sql(vote_counts,session,None,'VoteCount',raw_to_votecount=True)
    vote_counts_fat.rename(columns={'Id':'VoteCount_Id'},inplace=True)
    session.commit()
    end = time.time()
    print(f'\tSeconds required to upload VoteCount: {round(end - start)}')
    print('Upload to SelectionElectionContestVoteCountJoin')
    start = time.time()

    cdf_d['SelectionElectionContestVoteCountJoin'] = dbr.dframe_to_sql(vote_counts_fat,session,None,'SelectionElectionContestVoteCountJoin')
    end = time.time()
    print(f'\tSeconds required to upload SelectionElectionContestVoteCountJoin: {round(end - start)}')
    print('Drop columns from cdf table')
    dbr.drop_cols((session,'VoteCount',extra_cols))
    q = 'ALTER TABLE "VoteCount" DROP COLUMN "Election_Id",' \
        ' DROP COLUMN "Contest_Id" ,  DROP COLUMN "Selection_Id" '
    sql_ids=[]
    strs = []
    dbr.raw_query_via_SQLALCHEMY(session,q,sql_ids,strs)

    return


if __name__ == '__main__':

    print('Done!')
    exit()
