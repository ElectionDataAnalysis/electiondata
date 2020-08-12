from election_anomaly import db_routines as dbr
from election_anomaly import user_interface as ui
from election_anomaly import juris_and_munger as jm
import pandas as pd
from pandas.api.types import is_numeric_dtype
import re
import os
import numpy as np
from sqlalchemy.orm.session import Session


class MungeError(Exception):
    pass


def generic_clean(df:pd.DataFrame) -> pd.DataFrame:
    """Replaces nulls, strips whitespace."""
    # TODO put all info about data cleaning into README.md (e.g., whitespace strip)
    # TODO return error if cleaning fails, including dtypes of columns
    working = df.copy()
    for c in working.columns:
        if is_numeric_dtype(working[c]):
            working[c] = working[c].fillna(0)
        else:
            working[c] = working[c].fillna('')
            try:
                working[c] = working[c].apply(lambda x:x.strip())
            except AttributeError:
                pass
    return working


def cast_cols_as_int(df: pd.DataFrame, col_list: list,mode='name',error_msg='') -> pd.DataFrame:
    """recast columns as integer where possible, leaving columns with text entries as non-numeric)"""
    if mode == 'index':
        num_columns = [df.columns[idx] for idx in col_list]
    elif mode == 'name':
        num_columns = [c for c in df.columns if c in col_list]
    else:
        raise ValueError(f'Mode {mode} not recognized')
    for c in num_columns:
        try:
            df[c] = df[c].astype('int64',errors='raise')
        except ValueError as e:
            print(f'{error_msg}\nColumn {c} cannot be cast as integer:\n{e}')
    return df


def none_or_unknown(df:pd.DataFrame,cols:list) -> pd.DataFrame:
    """ change all blanks to 'none or unknown' """
    df_copy = df.copy()
    for c in cols:
        df.loc[:, c] = df[c].apply(str)
        df.loc[:, c] = df[c].replace('', 'none or unknown')
    return df_copy


def munge_clean(raw: pd.DataFrame, munger: jm.Munger):
    """Drop unnecessary columns.
    Append '_SOURCE' suffix to raw column names to avoid conflicts"""
    working = raw.copy()
    # drop columns that are neither count columns nor used in munger formulas
    #  define columns named in munger formulas
    if munger.header_row_count > 1:
        munger_formula_columns = [x for x in working.columns if x[munger.field_name_row] in munger.field_list]
    else:
        munger_formula_columns = [x for x in working.columns if x in munger.field_list]

    if munger.field_name_row is None:
        count_columns_by_name = [munger.field_names_if_no_field_name_row[idx] for idx in munger.count_columns]
    else:
        count_columns_by_name = [working.columns[idx] for idx in munger.count_columns]
    # TODO error check- what if cols_to_munge is missing something from munger.field_list?

    # keep columns named in munger formulas; keep count columns; drop all else.
    working = working[munger_formula_columns + count_columns_by_name]

    # add suffix '_SOURCE' to certain columns to avoid any conflict with db table names
    # (since no db table name ends with _SOURCE)
    renamer = {x:f'{x}_SOURCE' for x in munger_formula_columns}
    working.rename(columns=renamer, inplace=True)
    return working


def text_fragments_and_fields(formula):
    """Given a formula with fields enclosed in angle brackets,
    return a list of text-fragment,field pairs (in order of appearance) and a final text fragment.
    E.g., if formula is <County>;<Precinct>, returned are [(None,County),(';',Precinct)] and None."""
    # use regex to apply formula (e.g., decode '<County>;<Precinct>'
    p = re.compile('(?P<text>[^<>]*)<(?P<field>[^<>]+)>')  # pattern to find text,field pairs
    q = re.compile('(?<=>)[^<]*$')  # pattern to find text following last pair
    text_field_list = re.findall(p,formula)
    if not text_field_list:
        last_text = [formula]
    else:
        last_text = re.findall(q,formula)
    return text_field_list,last_text


def add_column_from_formula(working: pd.DataFrame, formula: str, new_col: str, err: dict, suffix=None) -> (pd.DataFrame, dict):
    """If <suffix> is given, add it to each field in the formula"""
    text_field_list, last_text = text_fragments_and_fields(formula)

    # add suffix, if required
    if suffix:
        text_field_list = [(t,f'{f}{suffix}') for (t,f) in text_field_list]

    if last_text:
        working.loc[:, new_col] = last_text[0]
    else:
        working.loc[:, new_col] = ''
    text_field_list.reverse()
    for t, f in text_field_list:
        try:
            working.loc[:, new_col] = working.loc[:, f].apply(lambda x: f'{t}{x}') + working.loc[:,new_col]
        except KeyError:
            ui.add_error(err,'munge-error',f'missing column {f}')
    return working, err


def add_munged_column(
        raw: pd.DataFrame, munger: jm.Munger, element: str, err: dict, mode: str = 'row',
        inplace: bool = True) -> (pd.DataFrame, dict):
    """Alters dataframe <raw>, adding or redefining <element>_raw column
    via the <formula>. Assumes "_SOURCE" has been appended to all columns of raw
    Does not alter row count."""
    if not err:
        err = {}
    if raw.empty:
        return raw, err
    if inplace:
        working = raw
    else:
        working = raw.copy()

    try:
        formula = munger.cdf_elements.loc[element,'raw_identifier_formula']
        if mode == 'row':
            for field in munger.field_list:
                formula = formula.replace(f'<{field}>',f'<{field}_SOURCE>')
        elif mode == 'column':
            for i in range(munger.header_row_count):
                formula = formula.replace(f'<{i}>',f'<variable_{i}>')

        working, err = add_column_from_formula(working, formula,f'{element}_raw', err)

    except:
        e = f'Error munging {element}. Check raw_identifier_formula for {element} in cdf_elements.txt'
        if 'cdf_elements.txt' in err.keys():
            err['cdf_elements.txt'].append(e)
        else:
            err['cdf_elements.txt'] = [e]

    # compress whitespace for <element>_raw
    working.loc[:,f'{element}_raw'] = working[f'{element}_raw'].apply(compress_whitespace)
    return working, err


def compress_whitespace(s:str) -> str:
    """Return a string where every instance of consecutive whitespaces in <s> has been replace by a single space,
    and leading and trailing whitespace is eliminated"""
    new_s = re.sub(r'\s+',' ',s)
    new_s = new_s.strip()
    return new_s


def replace_raw_with_internal_ids(
        row_df: pd.DataFrame, juris: jm.Jurisdiction, table_df: pd.DataFrame, element: str, internal_name_column: str
        ,error: dict, drop_unmatched: bool=False, mode: str='row') -> (pd.DataFrame, dict):
    """replace columns in <row_df> with raw_identifier values by columns with internal names and Ids
    from <table_df>, which has structure of a db table for <element>.
    # TODO If <element> is BallotMeasureContest or CandidateContest,
    #  contest_type column is added/updated
    """
    if drop_unmatched:
        how='inner'
    else:
        how='left'
    # join the 'cdf_internal_name' from the raw_identifier table -- this is the internal name field value,
    # no matter what the name field name is in the internal element table (e.g. 'Name', 'BallotName' or 'Selection')
    # use dictionary.txt from jurisdiction

    raw_identifiers = pd.read_csv(os.path.join(juris.path_to_juris_dir,'dictionary.txt'),sep='\t')

    # error/warning for unmatched elements to be dropped
    raw_ids_for_element = raw_identifiers[raw_identifiers['cdf_element'] == element]
    if drop_unmatched:
        to_be_dropped = row_df[~row_df[f'{element}_raw'].isin(raw_ids_for_element['raw_identifier_value'])]
        if to_be_dropped.shape[0] == row_df.shape[0]:
            e =f'No {element} was found in \'dictionary.txt\''
            if 'munge_error' in error.keys():
                error['munge_error'].append(e)
            else:
                error['munge_error'] = [e]
            return row_df.drop(row_df.index), error
        elif not to_be_dropped.empty:
            e = f'Warning: Results for {to_be_dropped.shape[0]} rows with unmatched {element}s ' \
                f'will not be loaded to database.'
            if 'munge_warning' in error.keys():
                error['munge_warning'].append(e)
            else:
                error['munge_warning'] = [e]

    row_df = row_df.merge(raw_ids_for_element,how=how,
                          left_on=f'{element}_raw',
                          right_on='raw_identifier_value',suffixes=['',f'_{element}_ei'])

    if row_df.empty:
        e = f'No raw {element} in \'dictionary.txt\' matched any raw {element} derived from the result file'
        if 'munge_error' in error.keys():
            error['munge_error'].append(e)
        else:
            error['munge_error'] = [e]
        return row_df, error

    # Note: if how = left, unmatched elements get nan in fields from dictionary table
    # TODO how do these nans flow through?

    if mode == 'column':
        # drop rows that melted from unrecognized columns, EVEN IF drop_unmatched=False.
        #  These rows are ALWAYS extraneous. Drop cols where raw_identifier is not null
        #  but no cdf_internal_name was found
        row_df = row_df[(row_df['raw_identifier_value'].isnull()) | (row_df['cdf_internal_name'].notnull())]
        # TODO more efficient to drop these earlier, before melting

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
    row_df = row_df.drop([internal_name_column],axis=1)
    row_df.rename(columns={'Id':f'{element}_Id'},inplace=True)
    return row_df, error


def enum_col_from_id_othertext(df,enum,enum_df,drop_old=True):
    """Returns a copy of dataframe <df>, replacing id and othertext columns
    (e.g., 'CountItemType_Id' and 'OtherCountItemType)
    with a plaintext <type> column (e.g., 'CountItemType')
        using the enumeration given in <enum_df>"""
    assert f'{enum}_Id' in df.columns,f'Dataframe lacks {enum}_Id column'
    assert f'Other{enum}' in df.columns,f'Dataframe lacks Other{enum} column'
    assert 'Txt' in enum_df.columns,'Enumeration dataframe should have column \'Txt\''

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
        assert 'Txt' in enum_df.columns,'Enumeration dataframe should have a \'Txt\' column'

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
            df.loc[
                df[f'{type_col}_Id'] == other_id,'Other'+type_col] = df.loc[df[f'{type_col}_Id'] == other_id,type_col]
        df = df.drop(['Txt'],axis=1)
        for c in ['Id','Txt']:
            if c*3 in df.columns:
                # avoid restore name renaming the column in the main dataframe
                df.rename(columns={c*3:c},inplace=True)
    if drop_old:
        df = df.drop([type_col],axis=1)
    return df


def enum_value_from_id_othertext(enum_df,idx,othertext):
    """Given an enumeration dframe (with cols 'Id' and 'Txt', or index and column 'Txt'),
    along with an (<id>,<othertext>) pair, find and return the plain language
    value for that enumeration (e.g., 'general')."""

    # ensure Id is a column, not the index, of enum_df (otherwise df index will be lost in merge)
    if 'Id' not in enum_df.columns:
        enum_df['Id'] = enum_df.index

    if othertext != '':
        enum_val = othertext
    else:
        enum_val = enum_df[enum_df.Id == idx].loc[:,'Txt'].to_list()[0]
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


def fk_plaintext_dict_from_db_record(session,element,db_record,excluded=None):
    """Return a dictionary of <name>:<value> for any <name>_Id that is a foreign key
    in the <element> table, excluding any foreign key in the list <excluded>"""
    fk_dict = {}
    fk_df = dbr.get_foreign_key_df(session,element)
    if excluded:
        for i,r in fk_df.iterrows():
            # TODO normalize: do elts of <excluded> end in '_Id' or not?
            if i not in excluded and i[:-3] not in excluded:
                fk_dict[i] = dbr.name_from_id(session,r['foreign_table_name'],db_record[i])
    else:
        for i,r in fk_df.iterrows():
            fk_dict[i] = dbr.name_from_id(session,r['foreign_table_name'],db_record[i])
    return fk_dict


def enum_plaintext_dict_from_db_record(session,element,db_record):
    """Return a dictionary of <enum>:<plaintext> for all enumerations in
    <db_record>, which is itself a dictionary of <field>:<value>"""
    enum_plaintext_dict = {}
    element_df_columns = pd.read_sql_table(element,session.bind,index_col='Id').columns
    # TODO INEFFICIENT don't need all of element_df; just need columns
    # identify enumerations by existence of `<enum>Other` field
    enum_list = [x[5:] for x in element_df_columns if x[:5] == 'Other']
    for e in enum_list:
        enum_df = pd.read_sql_table(e,session.bind)
        enum_plaintext_dict[e] = enum_value_from_id_othertext(enum_df,db_record[f'{e}_Id'],db_record[f'Other{e}'])
    return enum_plaintext_dict


def db_record_from_file_record(session,element,file_record):
    db_record = file_record.copy()
    enum_list = dbr.get_enumerations(session,element)
    for e in enum_list:
        enum_df = pd.read_sql_table(e,session.bind)
        db_record[f'{e}_Id'],db_record[f'Other{e}'] = \
            enum_value_to_id_othertext(enum_df,db_record[e])
        db_record.pop(e)
    fk_df = dbr.get_foreign_key_df(session,element)
    for fk in fk_df.index:
        if fk[:-3] not in enum_list:
            db_record[fk] = dbr.name_to_id(session,fk_df.loc[fk,'foreign_table_name'],file_record[fk[:-3]])
            db_record.pop(fk[:-3])
    return db_record


def good_syntax(s):
    """Returns true if formula string <s> passes certain syntax main_routines(s)"""
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


def munge_and_melt(mu,raw,count_cols,err):
    """Does not alter raw; returns Transformation of raw:
     all row- and column-sourced mungeable info into columns (but doesn't translate via dictionary)
    new column names are, e.g., ReportingUnit_raw, Candidate_raw, etc.
    """
    working = raw.copy()

    # apply munging formula from row sources (after renaming fields in raw formula as necessary)
    for t in mu.cdf_elements[mu.cdf_elements.source == 'row'].index:
        working, err = add_munged_column(working,mu,t,err,mode='row')

    # remove original row-munge columns
    munged = [x for x in working.columns if x[-7:] == '_SOURCE']
    working.drop(munged,axis=1,inplace=True)

    # if there is just one numerical column, melt still creates dummy variable col
    #  in which each value is 'value'
    # TODO how to ensure such files munge correctly?

    # reshape
    non_count_cols = [x for x in working.columns if x not in count_cols]
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
        working,err = add_munged_column(working,mu,t,err,mode='column')

    # remove unnecessary columns
    not_needed = [f'variable_{i}' for i in range(mu.header_row_count)]
    working.drop(not_needed,axis=1,inplace=True)

    return working, err


def add_constant_column(df,col_name,col_value):
    new_df = df.assign(**dict.fromkeys([col_name], col_value))
    return new_df


def add_contest_id(df: pd.DataFrame, juris: jm.Jurisdiction, err: dict, session: Session) -> (pd.DataFrame, dict):
    working = df.copy()

    # add column for contest_type
    working = add_constant_column(working,'contest_type','unknown')

    # append ids for BallotMeasureContests and CandidateContests
    for c_type in ['BallotMeasure','Candidate']:
        df_contest = pd.read_sql_table(f'{c_type}Contest',session.bind)
        [working, err] = replace_raw_with_internal_ids(
            working,
            juris,
            df_contest,
            f'{c_type}Contest',
            dbr.get_name_field(f'{c_type}Contest'),
            err,
            drop_unmatched=False)

        # set contest_type where id was found
        working.loc[working[f'{c_type}Contest_Id'].notnull(), 'contest_type'] = c_type

        # drop column with munged name
        working.drop(f'{c_type}Contest', axis=1, inplace=True)

    # drop rows with unmatched contests
    to_be_dropped = working[working['contest_type'] == 'unknown']
    working_temp = working[working['contest_type'] != 'unknown']
    if working_temp.empty:
        e = 'No contests in database matched. No results will be loaded to database.'
        if 'munge_warning' in err.keys():
            err['munge_warning'].append(e)
        else:
            err['munge_warning'] = [e]
        return pd.DataFrame(), err

    elif not to_be_dropped.empty:
        ui.add_error(
            err, 'munge_warning',
            f'Warning: Results for {to_be_dropped.shape[0]} rows with unmatched contests will not be loaded to database.')
    working = working_temp

    # define Contest_Id based on contest_type,
    working.loc[working['contest_type'] == 'Candidate','Contest_Id'] = working.loc[
        working['contest_type'] == 'Candidate','CandidateContest_Id']
    working.loc[working['contest_type'] == 'BallotMeasure','Contest_Id'] = working.loc[
        working['contest_type'] == 'BallotMeasure','BallotMeasureContest_Id']

    return working, err
    # get ids for remaining info sourced from rows and columns
    element_list = [t for t in mu.cdf_elements.index if
                    (t[-7:] != 'Contest' and t[-9:] != 'Selection')]
    for t in element_list:
        # capture id from db in new column and erase any now-redundant cols
        df = pd.read_sql_table(t,session.bind)
        name_field = dbr.get_name_field(t)
        # set drop_unmatched = True for fields necessary to BallotMeasure rows,
        #  drop_unmatched = False otherwise to prevent losing BallotMeasureContests for BM-inessential fields
        if t == 'ReportingUnit' or t == 'CountItemType':
            drop = True
        else:
            drop = False
        try:
            [working, err] = replace_raw_with_internal_ids(
                working, juris, df, t, name_field, mu.path_to_munger_dir, err, drop_unmatched=drop)
            working.drop(t,axis=1,inplace=True)
        except:
            e = f'Error adding internal ids for {t}.'
            if t == 'CountItemType':
                e += ' Are CountItemTypes correct in dictionary.txt?'
            ui.add_error(err, 'munge-error', f'Error adding internal ids for {t}.')


def add_selection_id(df: pd.DataFrame, juris: jm.Jurisdiction, mu: jm.Munger, err: dict, session: Session) -> (pd.DataFrame, dict):
    # append BallotMeasureSelection_Id, drop BallotMeasureSelection
    working = df.copy()
    df_selection = pd.read_sql_table(f'BallotMeasureSelection',session.bind)
    [working, err] = replace_raw_with_internal_ids(
        working,juris,df_selection,'BallotMeasureSelection',dbr.get_name_field('BallotMeasureSelection'),
        err,
        drop_unmatched=False,
        mode=mu.cdf_elements.loc['BallotMeasureSelection','source'])
    # drop BallotMeasure records without a legitimate BallotMeasureSelection
    # i.e., keep CandidateContest records and records with a legit BallotMeasureSelection
    working = working[(working['contest_type'] =='Candidate') | (working['BallotMeasureSelection'] != 'none or unknown')]

    working.drop('BallotMeasureSelection',axis=1,inplace=True)

    # append CandidateSelection_Id
    #  Load CandidateSelection table (not directly munged, not exactly a join either)
    #  Note left join, as not every record in working has a Candidate_Id

    c_df = working[['Candidate_Id','Party_Id']]
    c_df = c_df.drop_duplicates()
    c_df = c_df[c_df['Candidate_Id'].notnull()]
    cs_df, e = dbr.dframe_to_sql(c_df,session,'CandidateSelection',return_records='original')
    if e:
        ui.add_error(err,'database',e)
    # add CandidateSelection_Id column, merging on Candidate_Id and Party_Id

    if cs_df.empty:
        working = add_constant_column(working, 'CandidateSelection_Id', np.nan)
    else:
        working = working.merge(
            cs_df[['Party_Id','Candidate_Id','Id']],how='left',
            left_on=['Candidate_Id','Party_Id'],right_on=['Candidate_Id','Party_Id'])
        working.rename(columns={'Id':'CandidateSelection_Id'},inplace=True)

    # drop records with a CC_Id but no CS_Id (i.e., keep if CC_Id is null or CS_Id is not null)
    working = working[(working['CandidateContest_Id'].isnull()) | (working['CandidateSelection_Id']).notnull()]

    # define Selection_Id based on contest_type,
    working.loc[working['contest_type'] == 'Candidate','Selection_Id'] = working.loc[
        working['contest_type'] == 'Candidate','CandidateSelection_Id']
    working.loc[working['contest_type'] == 'BallotMeasure','Selection_Id'] = working.loc[
        working['contest_type'] == 'BallotMeasure','BallotMeasureSelection_Id']
    return working, err


def raw_elements_to_cdf(
        session, project_root: str, juris: jm.Jurisdiction, mu: jm.Munger,raw: pd.DataFrame, count_cols: list,
        err: dict, ids=None) -> dict:
    """load data from <raw> into the database."""
    working = raw.copy()

    # enter elements from sources outside raw data, including creating id column(s)
    working = add_constant_column(working,'Election_Id',ids[1])
    working = add_constant_column(working,'_datafile_Id',ids[0])

    working, err = munge_and_melt(mu,working,count_cols,err)

    working, err = add_contest_id(working, juris, err, session)
    if working.empty:
        return err
    # FIXME use Contest_Id below as necessary

    # get ids for remaining info sourced from rows and columns
    element_list = [t for t in mu.cdf_elements.index if
                    (t[-7:] != 'Contest' and t[-9:] != 'Selection')]
    for t in element_list:
        # capture id from db in new column and erase any now-redundant cols
        df = pd.read_sql_table(t,session.bind)
        name_field = dbr.get_name_field(t)
        # set drop_unmatched = True for fields necessary to BallotMeasure rows,
        #  drop_unmatched = False otherwise to prevent losing BallotMeasureContests for BM-inessential fields
        if t == 'ReportingUnit' or t == 'CountItemType':
            drop = True
        else:
            drop = False
        try:
            [working, err] = replace_raw_with_internal_ids(
                working, juris, df, t, name_field, err, drop_unmatched=drop)
            working.drop(t,axis=1,inplace=True)
        except:
            e = f'Error adding internal ids for {t}.'
            if t == 'CountItemType':
                e += ' Are CountItemTypes correct in dictionary.txt?'
            ui.add_error(err, 'munge-error', f'Error adding internal ids for {t}.')

    working, err = add_selection_id(working, juris, mu, err, session)
    if working.empty:
        return err
    # TODO remove redundancy
    if working.empty:
        e = 'No contests found, or no selections found for contests.'
        err = ui.add_error(err,'datafile',e)
        return err

    for j in ['ContestSelectionJoin','ElectionContestJoin']:
        working, err = append_join_id(project_root, session, working, j, err)

    # Fill VoteCount and ElectionContestSelectionVoteCountJoin
    #  To get 'VoteCount_Id' attached to the correct row, temporarily add columns to VoteCount
    #  add ElectionContestSelectionVoteCountJoin columns to VoteCount

    extra_cols = ['ElectionContestJoin_Id','ContestSelectionJoin_Id','_datafile_Id']
    try:
        dbr.add_integer_cols(session,'VoteCount',extra_cols)
    except:
        print(f'Warning: extra columns not added to VoteCount table.')

    # upload to VoteCount table, pull  Ids
    working_fat, e = dbr.dframe_to_sql(working,session,'VoteCount',raw_to_votecount=True)
    if e:
        ui.add_error(err,'database',e)
    working_fat.rename(columns={'Id':'VoteCount_Id'},inplace=True)
    session.commit()

    # TODO check that all candidates in munged contests (including write ins!) are munged
    # upload to ElectionContestSelectionVoteCountJoin
    data, e = dbr.dframe_to_sql(working_fat,session,'ElectionContestSelectionVoteCountJoin')
    if e:
        ui.add_error(err,'database',e)

    # drop extra columns
    dbr.drop_cols(session,'VoteCount',extra_cols)

    return err


def append_join_id(project_root: str, session, working: pd.DataFrame, j: str, err: dict) -> (pd.DataFrame, dict):
    """Upload join data to db, get Ids,
    Append <join>_Id to <working>. Unmatched rows are kept"""
    j_path = os.path.join(
        project_root,'election_anomaly/CDF_schema_def_info/joins',j,'foreign_keys.txt')
    join_fk = pd.read_csv(j_path,sep='\t',index_col='fieldname')
    join_fk.loc[:,'refers_to_list'] = join_fk.refers_to.str.split(';')
    # create dataframe with cols named to match join table, content from corresponding column of working

    refs = set().union(*join_fk.refers_to_list)  # all referents of cols in join table
    ref_ids = [f'{x}_Id' for x in refs]
    j_cols = list(join_fk.index)
    # drop dupes and any null rows
    join_df = working[ref_ids].drop_duplicates(keep='first')  # take only nec cols from working and dedupe

    ref_d = {}
    for fn in join_fk.index:
        ref_d[fn] = [f'{x}_Id' for x in join_fk.loc[fn,'refers_to_list']]
    join_df, err = append_multi_foreign_key(join_df,ref_d, err)
    working, err = append_multi_foreign_key(working,ref_d, err)

    # remove any join_df rows with any *IMPORTANT* null and load to db
    unnecessary = [x for x in ref_ids if x not in j_cols]
    join_df.drop(unnecessary,axis=1,inplace=True)
    # remove any row with a null value in all columns
    join_df = join_df[join_df.notnull().any(axis=1)]
    # warn user of rows with null value in some columns
    bad_rows = join_df.isnull().any(axis=1)
    if bad_rows.any():
        ui.add_error(
            err,'munge-warning',
            f'Warning: there are {bad_rows.shape[0]} rows proposed for {j} that have nulls values. '
            f'These will not be uploaded')
    # remove any row with a null value in any column
    join_df = join_df[join_df.notnull().all(axis=1)]
    # add column for join id
    if join_df.empty:
        working[f'{j}_Id'] = np.nan
    else:
        join_df, e = dbr.dframe_to_sql(join_df,session,j)
        if e:
            ui.add_error(err,'database',e)
        working = working.merge(join_df,how='left',left_on=j_cols,right_on=j_cols)
        working.rename(columns={'Id':f'{j}_Id'},inplace=True)
    return working, err


def append_multi_foreign_key(df: pd.DataFrame, references: dict, err: dict) -> (pd.DataFrame, dict):
    """<references> is a dictionary whose keys are fieldnames for the new column
    and whose value for any key is the list of reference targets.
    If a row in df has more than one non-null value, only the first will be taken.
    Return the a copy of <df> with a column added for each fieldname in <references>.keys()"""
    # TODO inefficient if only one ref target
    df_copy = df.copy()
    for fn in references.keys():
        if df_copy[references[fn]].isnull().all().all():
            # if everything is null, just add the necessary column with all null values
            df_copy.loc[:, fn] = np.nan
            err = ui.add_error(err, 'munge_warning', f'Nothing matched to {fn}')
        else:
            # expect at most one non-null entry in each row; find the value of that one
            df_copy.loc[:,fn] = df_copy[references[fn]].fillna(-1).max(axis=1)
            # change any -1s back to nulls (if all were null, return null)
            df_copy.loc[:,fn]=df_copy[fn].replace(-1,np.NaN)
    return df_copy, err


if __name__ == '__main__':
    pass
    exit()
