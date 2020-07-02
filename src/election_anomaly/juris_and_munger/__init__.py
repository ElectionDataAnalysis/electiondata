import os.path

from election_anomaly import db_routines
from election_anomaly import db_routines as dbr
import pandas as pd
from election_anomaly import munge_routines as mr
from election_anomaly import user_interface as ui
import re
import numpy as np
from pathlib import Path


class Jurisdiction:
    def check_against_raw_results(self,results_df,munger,numerical_columns):
        """Warn user of any mungeable elements in <results_df> that are not
        translatable via dictionary.txt"""
        finished = False
        changed = False
        while not finished:
            d = pd.read_csv(
                os.path.join(
                    self.path_to_juris_dir,'dictionary.txt'
                ),sep='\t',index_col='cdf_element')

            problems = []
            # for each relevant element
            others = [x for x in munger.cdf_elements.index if
                     x not in ['BallotMeasureContest','CandidateContest','BallotMeasureSelection','Candidate']]

            # find missing contests
            missing_bmc = check_element_against_raw_results(
                'BallotMeasureContest',results_df,munger,numerical_columns,d)[['BallotMeasureContest_raw']]
            missing_cc = check_element_against_raw_results(
                'CandidateContest',results_df,munger,numerical_columns,d)[['CandidateContest_raw']]
            missing_contest = missing_bmc.merge(missing_cc,how='inner',left_index=True,right_index=True)
            if not missing_contest.empty:
                ui.show_sample(missing_contest,f'Contests','cannot be translated')
                problems.append(f'At least one contest unrecognized by dictionary.txt')

            # find missing candidates/selections
            missing_bms = check_element_against_raw_results(
                'BallotMeasureSelection',results_df,munger,numerical_columns,d)
            missing_c = check_element_against_raw_results(
                'Candidate',results_df,munger,numerical_columns,d)
            missing_s = missing_bms.merge(missing_c,how='inner',left_index=True,right_index=True)

            if not missing_s.empty:
                ui.show_sample(missing_s,f'selections','cannot be translated')
                problems.append(f'At least one selection unrecognized by dictionary.txt')
            for el in others:
                missing = check_element_against_raw_results(el,results_df,munger,numerical_columns,d)
                if not missing.empty:
                    ui.show_sample(missing,f'{el}s','cannot be translated')
                    problems.append(f'At least one {el} unrecognized by dictionary.txt')
            if problems:
                prob_str = '\n\t'.join(problems)
                ignore = input(f'Summary of omissions:\n\t{prob_str}\nContinue despite omissions (y/n)?')
                if ignore == 'y':
                    finished = True
                else:
                    input(f'Make any necessary changes to dictionary.txt, then hit return to continue.')
                    changed = True
            else:
                finished = True
        return changed

    def load_juris_to_db(self,session,project_root):
        """Load info from each element in the Jurisdiction's directory into the db"""
        # for element in Jurisdiction directory (except dictionary, remark)
        juris_elements = [
            x[:-4] for x in os.listdir(self.path_to_juris_dir)
            if x != 'remark.txt' and x != 'dictionary.txt' and x[0] != '.']
        # reorder juris_elements for efficiency
        leading = ['ReportingUnit','Office','CandidateContest']
        trailing = ['ExternalIdentifier']
        juris_elements = leading + [
            x for x in juris_elements if x not in leading and x not in trailing
        ] + trailing
        error = {}
        for element in juris_elements:
            # read df from Jurisdiction directory
            load_juris_dframe_into_cdf(session,element,self.path_to_juris_dir,project_root,error)
        if error:
            for element in juris_elements:
                dbr.truncate_table(session, element)
            return error
        return None

    def __init__(self,short_name,path_to_parent_dir):
        """ short_name is the name of the directory containing the jurisdiction info, including data,
         and is used other places as well.
         path_to_parent_dir is the parent directory of dir_name
        """
        self.short_name = ui.pick_or_create_directory(path_to_parent_dir,short_name)
        self.path_to_juris_dir = os.path.join(path_to_parent_dir, self.short_name)

        with open(os.path.join(self.path_to_juris_dir,'remark.txt'),'r') as f:
            remark = f.read()
        print(f'\n\nJurisdiction {short_name} initialized! Note:\n{remark}')


class Munger:
    def check_against_self(self):
        """check that munger is internally consistent"""
        problems = []

        # every source is either row, column or other
        bad_source = [x for x in self.cdf_elements.source if x not in ['row','column','other']]
        if bad_source:
            b_str = ','.join(bad_source)
            problems.append(f'''At least one source in cdf_elements.txt is not recognized: {b_str} ''')

        # formulas have good syntax
        bad_formula = [x for x in self.cdf_elements.raw_identifier_formula.unique() if not mr.good_syntax(x)]
        if bad_formula:
            f_str = ','.join(bad_formula)
            problems.append(f'''At least one formula in cdf_elements.txt has bad syntax: {f_str} ''')

        # for each column-source record in cdf_element, contents of bracket are numbers in the header_rows
        p_not_just_digits = re.compile(r'<.*\D.*>')
        p_catch_digits = re.compile(r'<(\d+)>')
        bad_column_formula = set()
        for i,r in self.cdf_elements[self.cdf_elements.source == 'column'].iterrows():
            if p_not_just_digits.search(r['raw_identifier_formula']):
                bad_column_formula.add(r['raw_identifier_formula'])
            else:
                integer_list = [int(x) for x in p_catch_digits.findall(r['raw_identifier_formula'])]
                bad_integer_list = [x for x in integer_list if (x > self.header_row_count-1 or x < 0)]
                if bad_integer_list:
                    bad_column_formula.add(r['raw_identifier_formula'])
        if bad_column_formula:
            cf_str = ','.join(bad_column_formula)
            problems.append(f'''At least one column-source formula in cdf_elements.txt has bad syntax: {cf_str} ''')

        # TODO if field in formula matches an element self.cdf_element.index,
        #  check that rename is not also a column
        if problems:
            error = {}
            error["munger_internal_consistency"] = ", ".join(problems)
            return error
        else:
            return None

    def check_against_datafile(self,datafile_path):
        """check that munger is compatible with datafile <raw>;
        offer user chance to correct munger"""

        # initialize to keep syntax-checker happy
        raw = pd.DataFrame([[]])

        checked = False
        while not checked:
            problems = []

            # check encoding
            try:
                raw = ui.read_datafile(self,datafile_path)
            except UnicodeEncodeError:
                problems.append(f'Datafile is not encoded as {self.encoding}.')

            # check that all count_columns are indeed read as integers
            bad_columns = [raw.columns[idx] for idx in self.count_columns if raw.dtypes[idx] != 'int64']
            if bad_columns:
                bad_col_string = '\n\t'.join(bad_columns)
                problems.append(f'Munger fails to parse some VoteCount columns in the results file as integers:\n'
                                f'{bad_col_string}')

            non_count_integer_cols = [x for x in raw.columns if raw[x].dtype == 'int64' and
                                      raw.columns.get_loc(x) not in self.count_columns]
            if non_count_integer_cols:
                ncic_string = '\n\t'.join(non_count_integer_cols)
                ncic_ok = input(f'Munger parses the following columns as integers, but does not recognize them as'
                                f'VoteCount columns.\n{ncic_string}\nIs this correct (y/n)?\n')
                if ncic_ok != 'y':
                    problems.append(f'Count_columns line in the format.txt file needs to be corrected.\n'
                                    f'Value should be a comma-separated list of integers. Convention is\n'
                                    f'to label the leftmost column 0.')

            col_fields = '\n\t'.join(raw.columns)
            cf_ok = input(f'Munger reads the following column fields from datafile (one per line):\n\t'
                          f'{col_fields}\nAre these correct (y/n)?\n')
            if cf_ok == 'y' and raw.shape[1] <3:
                cf_ok = input(f'Are you sure? Is each SEPARATE LINE above a single field (y/n)?\n')
            if cf_ok != 'y':
                problems.append(f'Either column_field_row ({col_fields}) or file_type ({self.file_type}) is incorrect.')

            # user confirm first data row
            first_data_row = '\t'.join([f'{x}' for x in raw.iloc[0]])
            fdr_ok = input(f'Munger thinks the first data row is:\n{first_data_row}\n'
                           f'Is this correct (y/n)?\n')
            if fdr_ok != 'y':
                problems.append('header_row_count does not match the datafile.')

            if problems:
                ui.report_problems(problems)
                input(f'Correct the problems by editing the files in the directory {self.path_to_munger_dir}\n'
                      f'Then hit enter to continue.')
                [self.cdf_elements,self.header_row_count,self.field_name_row,self.count_columns,
                 self.file_type,self.encoding,self.thousands_separator] = read_munger_info_from_files(
                    self.path_to_munger_dir)
            else:
                checked = True
        # TODO allow user to pick different munger from file system
        return

    def __init__(self,dir_path,project_root=None,check_files=True):
        """<dir_path> is the directory for the munger."""
        if not project_root:
            project_root = ui.get_project_root()
        self.name= os.path.basename(dir_path)  # e.g., 'nc_general'
        self.path_to_munger_dir = dir_path

        # create dir if necessary
        if os.path.isdir(dir_path):
            print(f'Directory {self.name} exists.')
        else:
            print(f'Creating directory {self.name}')
            Path(dir_path).mkdir(parents=True,exist_ok=True)

        if check_files:
            ensure_munger_files(self.name,project_root=project_root)
        [self.cdf_elements,self.header_row_count,self.field_name_row,self.count_columns,
         self.file_type,self.encoding,self.thousands_separator] = read_munger_info_from_files(
            self.path_to_munger_dir)

        self.field_rename_suffix = '___'  # NB: must not match any suffix of a cdf element name;

        # used repeatedly, so calculated once for convenience
        self.field_list = set()
        for t,r in self.cdf_elements.iterrows():
            self.field_list=self.field_list.union(r['fields'])


def read_munger_info_from_files(dir_path):
    # read cdf_element info and
    cdf_elements = pd.read_csv(os.path.join(dir_path,'cdf_elements.txt'),sep='\t',index_col='name').fillna('')
    # add row for _datafile element
    datafile_elt = pd.DataFrame([['','other']],columns=['raw_identifier_formula','source'],index=['_datafile'])
    cdf_elements = cdf_elements.append(datafile_elt)
    # add column for list of fields used in formulas
    cdf_elements['fields'] = [[]]*cdf_elements.shape[0]
    for i,r in cdf_elements.iterrows():
        text_field_list,last_text = mr.text_fragments_and_fields(cdf_elements.loc[i,'raw_identifier_formula'])
        cdf_elements.loc[i,'fields'] = [f for t,f in text_field_list]

    # read formatting info
    format_info = pd.read_csv(os.path.join(dir_path,'format.txt'),sep='\t',index_col='item')
    field_name_row = int(format_info.loc['field_name_row','value'])
    header_row_count = int(format_info.loc['header_row_count','value'])
    count_columns = [int(x) for x in format_info.loc['count_columns','value'].split(',')]
    file_type = format_info.loc['file_type','value']
    encoding = format_info.loc['encoding','value']
    thousands_separator = format_info.loc['thousands_separator','value']
    if thousands_separator in ['None','',np.nan]:
        thousands_separator = None
    # TODO warn if encoding not recognized

    # TODO if cdf_elements.txt uses any cdf_element names as fields in any raw_identifiers formula,
    #   will need to rename some columns of the raw file before processing.
    return [cdf_elements,header_row_count,field_name_row,count_columns,file_type,encoding,thousands_separator]

# TODO combine ensure_jurisdiction_files with ensure_juris_files
def ensure_jurisdiction_files(juris_path,project_root):
    # create jurisdiction directory
    try:
        os.mkdir(juris_path)
    except FileExistsError:
        print(f'Directory {juris_path} already exists, will be preserved')
    else:
        print(f'Directory {juris_path} created')

    # ensure the contents of the jurisdiction directory are correct
    ensure_juris_files(juris_path,project_root)
    return


def ensure_juris_files(juris_path,project_root):
    """Check that the jurisdiction files are complete and consistent with one another.
    Check for extraneous files in Jurisdiction directory.
    Assumes Jurisdiction directory exists. Assumes dictionary.txt is in the template file"""

    templates_dir = os.path.join(project_root,'templates/jurisdiction_templates')
    # ask user to remove any extraneous files
    extraneous = ['unknown']
    while extraneous:
        extraneous = [f for f in os.listdir(juris_path) if
                      f != 'remark.txt' and f not in os.listdir(templates_dir) and f[0] != '.']
        if extraneous:
            ui.report_problems(extraneous,msg=f'There are extraneous files in {juris_path}')
            input(f'Remove all extraneous files; then hit return to continue.')

    template_list = [x[:-4] for x in os.listdir(templates_dir)]

    # reorder template_list, so that first things are created first
    ordered_list = ['dictionary','ReportingUnit','Office','CandidateContest']
    template_list = ordered_list + [x for x in template_list if x not in ordered_list]

    # ensure necessary all files exist
    for juris_file in template_list:
        print(f'\nChecking {juris_file}.txt')
        cf_path = os.path.join(juris_path,f'{juris_file}.txt')
        # if file does not already exist in jurisdiction directory, create from template and invite user to fill
        try:
            temp = pd.read_csv(os.path.join(templates_dir,f'{juris_file}.txt'),sep='\t')
        except pd.errors.EmptyDataError:
            print(f'Template file {juris_file}.txt has no contents')
            temp = pd.DataFrame()
        if not os.path.isfile(cf_path):
            temp.to_csv(cf_path,sep='\t',index=False)
            input(f'File {juris_file}.txt has just been created.\n'
                  f'Enter information in the file, then hit return to continue.')

        # if file exists, check format against template
        cf_df = pd.read_csv(os.path.join(juris_path,f'{juris_file}.txt'),sep='\t')
        format_confirmed = False
        while not format_confirmed:
            if set(cf_df.columns) != set(temp.columns):
                cols = '\t'.join(temp.columns.to_list())
                input(f'Columns of {juris_file}.txt need to be (tab-separated):\n'
                      f' {cols}\n'
                      f'Edit {juris_file}.txt, and hit return to continue.')
            else:
                format_confirmed = True

        if juris_file == 'ExternalIdentifier':
            dedupe(cf_path)
        elif juris_file == 'dictionary':
            dedupe(cf_path)
        else:
            # run dupe check
            dedupe(cf_path)
            # check for problematic null entries
            check_nulls(juris_file,cf_path,project_root)
    # check dependencies
    for juris_file in [x for x in template_list if x != 'remark' and x != 'dictionary']:
        # check dependencies
        check_dependencies(juris_path,juris_file)
    # remark
    rem_path = os.path.join(juris_path,'remark.txt')
    try:
        with open(rem_path,'r') as f:
            remark = f.read()
        print(f'Current contents of remark.txt is:\n{remark}\n')
    except FileNotFoundError:
        open(rem_path, 'a').close()  # create empty file
    input(
        f'In the file remark.txt, add or correct anything that '
        f'user should know about the jurisdiction.\n'
        f'Then hit return to continue.')
    return


# noinspection PyUnresolvedReferences
def ensure_munger_files(munger_name,project_root=None):
    """Check that the munger files are complete and consistent with one another.
    Adds munger directory and files if they do not exist. 
    Assumes dictionary.txt is in the template file"""
    # define path to directory for the specific munger
    munger_path = os.path.join(project_root,'mungers',munger_name)
    # ensure all files exist
    created = []
    if not os.path.isdir(munger_path):
        created.append(munger_path)
        os.makedirs(munger_path)
    templates = os.path.join(project_root,'templates/munger_templates')
    template_list = [x[:-4] for x in os.listdir(templates)]


    error = {}
    # create each file if necessary
    for munger_file in template_list:
        cf_path = os.path.join(munger_path,f'{munger_file}.txt')
        # if file does not already exist in munger dir, create from template and invite user to fill
        file_exists = os.path.isfile(cf_path)
        if not file_exists:
            temp = pd.read_csv(os.path.join(templates,f'{munger_file}.txt'),sep='\t')
            created.append(f'{munger_file}.txt')
            temp.to_csv(cf_path,sep='\t',index=False)

        # if file exists, check format against template
        if file_exists:
            err = check_munger_file_format(munger_path, munger_file, templates)
            if err:
                error[f'{munger_file}.txt'] = err

    # check contents of each file if they were not newly created and
    # if they have successfully been checked for the format
    if file_exists and not error:
        err = check_munger_file_contents(munger_name,project_root=project_root)
        if err:
            error["contents"] = err

    if created:
        created = ', '.join(created)
        error["newly_created"] = created
    if error:
        return error
    return None


def check_munger_file_format(munger_path, munger_file, templates):
    cf_df = pd.read_csv(os.path.join(munger_path,f'{munger_file}.txt'),sep='\t')
    temp = pd.read_csv(os.path.join(templates,f'{munger_file}.txt'),sep='\t')
    problems = []
    # check column names are correct
    if set(cf_df.columns) != set(temp.columns):
        cols = '\t'.join(temp.columns.to_list())
        problems.append(f'Columns of {munger_file}.txt need to be (tab-separated):\n'
            f' {cols}\n')

    # check first column matches template
    #  check same number of rows
    elif cf_df.shape[0] != temp.shape[0]:
        first_col = '\n'.join(list(temp.iloc[:,0]))
        problems.append(
            f'Wrong number of rows in {munger_file}.txt. \nFirst column must be exactly:\n{first_col}')
    elif set(cf_df.iloc[:,0]) != set(temp.iloc[:,0]):
        first_error = (cf_df.iloc[:,0] != temp.iloc[:,0]).index.to_list()[0]
        first_col = '\n'.join(list(temp.iloc[:,0]))
        problems.append(f'First column of {munger_file}.txt must be exactly:\n{first_col}\n'
                        f'First error is at row {first_error}: {cf_df.loc[first_error]}')

    if problems:
        problems = ', '.join(problems)
        error = {}
        error["format_problems"] = problems
    else:
        error = None
    return error

def check_munger_file_contents(munger_name,project_root=None):
    """check that munger files are internally consistent; offer user chance to correct"""
    # define path to munger's directory
    if not project_root:
        project_root = ui.get_project_root()
    munger_dir = os.path.join(project_root,'mungers',munger_name)

    problems = []
    warns = []

    # read cdf_elements and format from files
    cdf_elements = pd.read_csv(os.path.join(munger_dir,'cdf_elements.txt'),sep='\t').fillna('')
    format_df = pd.read_csv(os.path.join(munger_dir,'format.txt'),sep='\t',index_col='item').fillna('')
    template_format_df = pd.read_csv(
        os.path.join(
            project_root,'templates/munger_templates/format.txt'
        ),sep='\t',index_col='item'
    ).fillna('')

    # format.txt has the required items
    req_list = template_format_df.index
    missing_items = [x for x in req_list if x not in format_df.index]
    if missing_items:
        item_string = ','.join(missing_items)
        problems.append(f'Format file is missing some items: {item_string}')

    # entries in format.txt are of correct type
    if not format_df.loc['header_row_count','value'].isnumeric():
        problems.append(f'In format file, header_row_count must be an integer'
                        f'({format_df.loc["header_row_count","value"]} is not.)')
    if not format_df.loc['field_name_row','value'].isnumeric():
        problems.append(f'In format file, field_name_row must be an integer '
                        f'({format_df.loc["field_name_row","value"]} is not.)')
    if not format_df.loc['encoding','value'] in ui.recognized_encodings:
        warns.append(f'Encoding {format_df.loc["field_name_row","value"]} in format file is not recognized.')

    # every source is either row, column or other
    bad_source = [x for x in cdf_elements.source if x not in ['row','column','other']]
    if bad_source:
        b_str = ','.join(bad_source)
        problems.append(f'''At least one source in cdf_elements.txt is not recognized: {b_str} ''')

    # formulas have good syntax
    bad_formula = [x for x in cdf_elements.raw_identifier_formula.unique() if not mr.good_syntax(x)]
    if bad_formula:
        f_str = ','.join(bad_formula)
        problems.append(f'''At least one formula in cdf_elements.txt has bad syntax: {f_str} ''')

    # for each column-source record in cdf_element, contents of bracket are numbers in the header_rows
    p_not_just_digits = re.compile(r'<.*\D.*>')
    p_catch_digits = re.compile(r'<(\d+)>')
    bad_column_formula = set()

    # problems found above may cause this block of code to error out, so this is
    # wrapped in a try block since it returns a general error message
    for i,r in cdf_elements[cdf_elements.source == 'column'].iterrows():
        if p_not_just_digits.search(r['raw_identifier_formula']):
            bad_column_formula.add(r['raw_identifier_formula'])
        else:
            integer_list = [int(x) for x in p_catch_digits.findall(r['raw_identifier_formula'])]
            bad_integer_list = [
                x for x in integer_list if (x > int(format_df.loc['header_row_count','value'])-1 or x < 0)]
            if bad_integer_list:
                bad_column_formula.add(r['raw_identifier_formula'])
    if bad_column_formula:
        cf_str = ','.join(bad_column_formula)
        problems.append(f'''At least one column-source formula in cdf_elements.txt has bad syntax: {cf_str} ''')

    # TODO if field in formula matches an element self.cdf_element.index,
    #  check that rename is not also a column
    error = {}
    if problems:
        error['problems'] = '\n\t'.join(problems)
    if warns:
        error['warnings'] = '\n\t'.join(warns)

    if error:
        return error
    return None


def dedupe(f_path,warning='There are duplicates'):
    # TODO allow specificaiton of unique constraints
    df = pd.read_csv(f_path,sep='\t')
    dupes = True
    while dupes:
        dupes_df,df = ui.find_dupes(df)
        if dupes_df.empty:
            dupes = False
            print(f'No dupes in {f_path}')
        else:
            print(f'WARNING: {warning}\n')
            ui.show_sample(dupes_df,'lines','are duplicates')
            input(f'Edit the file to remove the duplication, then hit return to continue')
            df = pd.read_csv(f_path,sep='\t')
    return df


def check_nulls(element,f_path,project_root):
    # TODO write description
    nn_path = os.path.join(
        project_root,'election_anomaly/CDF_schema_def_info/elements',element,'not_null_fields.txt')
    not_nulls = pd.read_csv(nn_path,sep='\t')
    df = pd.read_csv(f_path,sep='\t')

    nulls = True
    while nulls:
        problems = []
        for nn in not_nulls.not_null_fields.unique():
            # if nn is an Id, name in jurisdiction file is element name
            if nn[-3:] == '_Id':
                nn = nn[:-3]
            n = df[df[nn].isnull()]
            if not n.empty:
                ui.show_sample(n,f'Lines in {element} file',f'have illegal nulls in {nn}')
                problems.append(nn)
        if problems:
            input(f'Fix the nulls, then hit enter to continue.')
        else:
            nulls = False
    return


def check_dependencies(juris_dir,element):
    """Looks in <juris_dir> to check that every dependent column in <element>.txt
    is listed in the corresponding jurisdiction file. Note: <juris_dir> assumed to exist.
    """
    d = juris_dependency_dictionary()
    f_path = os.path.join(juris_dir,f'{element}.txt')
    assert os.path.isdir(juris_dir)
    element_df = pd.read_csv(f_path,sep='\t',index_col=None)

    # Find all dependent columns
    dependent = [c for c in element_df if c in d.keys()]
    changed_elements = set()
    report = [f'In {element}.txt:']
    for c in dependent:
        target = d[c]
        ed = pd.read_csv(os.path.join(
            juris_dir,f'{element}.txt'),sep='\t',header=0).fillna('').loc[:,c].unique()

        # create list of elements, removing any nulls
        ru = list(
            pd.read_csv(
                os.path.join(
                    juris_dir,f'{target}.txt'),sep='\t').fillna('').loc[:,db_routines.get_name_field(target)])
        try:
            ru.remove(np.nan)
        except ValueError:
            pass

        missing = [x for x in ed if x not in ru]
        if len(missing) == 0:
            report.append(f'Every {c} in {element}.txt is a {target}.')
        elif len(missing) == 1 and missing == ['']:  # if the only missing is null or blank
            # TODO some dependencies are ok with null (eg. PrimaryParty) and some are not
            report.append(f'Some {c} are null, and every non-null {c} is a {target}.')
        else:
            changed_elements.add(element)
            changed_elements.add(target)
            print(f'Every {c} must be a {target}. This is not optional!!')
            ui.show_sample(missing,f'{c}s',f'are not yet {target}s')
            input(f'Please make corrections to {element}.txt or additions to {target}.txt to resolve the problem.\n'
                  'Then his return to continue.')
            changed_elements.update(check_dependencies(juris_dir,target))
    if dependent:
        print('\n\t'.join(report))
    if changed_elements:
        print(f'(Directory is {juris_dir}')
    return changed_elements


def juris_dependency_dictionary():
    """Certain fields in jurisdiction files refer to other jurisdiction files.
    E.g., ElectionDistricts are ReportingUnits"""
    d = {'ElectionDistrict':'ReportingUnit','Office':'Office','PrimaryParty':'Party','Party':'Party',
         'Election':'Election'}
    return d


# TODO before processing jurisdiction files into db, alert user to any duplicate names.
#  Enforce name change? Or just suggest?
def load_juris_dframe_into_cdf(session,element,juris_path,project_root,error,load_refs=True):
    """ TODO
    """
    # TODO fail gracefully if file does not exist
    cdf_schema_def_dir = os.path.join(project_root,'election_anomaly/CDF_schema_def_info')
    df = pd.read_csv(os.path.join(juris_path,f'{element}.txt'),sep='\t').fillna('none or unknown')
    # TODO check that df has the right format

    # TODO deal with duplicate 'none or unknown' records
    if element != 'ExternalIdentifier':
        # add 'none or unknown' line to df
        d = {c:'none or unknown' for c in df.columns}
        fields_file = os.path.join(cdf_schema_def_dir,'elements',element,'fields.txt')
        fields = pd.read_csv(fields_file,sep='\t',index_col='fieldname')
        for f in fields.index:
            # change any non-string, non-foreign-id fields to default values
            t = fields.loc[f,'datatype']
            if t == 'Date':
                d[f]='1000-01-01'
            elif t == 'Integer':
                d[f] = -1
            elif t != 'String':
                raise TypeError(f'Datatype {t} not recognized')
        df = df.append([d])

    # dedupe df
    dupes,df = ui.find_dupes(df)
    if not dupes.empty:
        print(f'WARNING: duplicates removed from dataframe, may indicate a problem.\n')
        #ui.show_sample(dupes,f'lines in {element} source data','are duplicates')
        if not element in error:
            error[element] = {}
        error[element]["found_duplicates"] = True

    # replace nulls with empty strings
    df.fillna('',inplace=True)

    # replace plain text enumerations from file system with id/othertext from db
    enum_file = os.path.join(cdf_schema_def_dir,'elements',element,'enumerations.txt')
    if os.path.isfile(enum_file):  # (if not, there are no enums for this element)
        enums = pd.read_csv(enum_file,sep='\t')
        # get all relevant enumeration tables
        for e in enums['enumeration']:  # e.g., e = "ReportingUnitType"
            cdf_e = pd.read_sql_table(e,session.bind)
            # for every instance of the enumeration in the current table, add id and othertype columns to the dataframe
            if e in df.columns:
                df = mr.enum_col_to_id_othertext(df,e,cdf_e)
        # TODO skipping assignment of CountItemStatus to ReportingUnit for now,
        #  since we can't assign an ReportingUnit as ElectionDistrict to Office
        #  (unless Office has a CountItemStatus; can't be right!)
        #  Note CountItemStatus is weirdly assigned to ReportingUnit in NIST CDF.
        #  Note also that CountItemStatus is not required, and a single RU can have many CountItemStatuses

    # TODO somewhere, check that no CandidateContest & Ballot Measure share a name; ditto for other false foreign keys

    # get Ids for any foreign key (or similar) in the table, e.g., Party_Id, etc.
    fk_file_path = os.path.join(
            cdf_schema_def_dir,'elements',element,'foreign_keys.txt')
    if os.path.isfile(fk_file_path):
        foreign_keys = pd.read_csv(fk_file_path,sep='\t',index_col='fieldname')

        for fn in foreign_keys.index:
            refs = foreign_keys.loc[fn,'refers_to'].split(';')

            try:
                df = get_ids_for_foreign_keys(session,df,element,fn,refs,load_refs,error)
                print(f'Database foreign id assigned for each {fn} in {element}.')
            except ForeignKeyException as e:
                if load_refs:
                    for r in refs:
                        load_juris_dframe_into_cdf(session,r,juris_path,project_root,error)
                    # try again to load main element (but don't load referred-to again)
                    load_juris_dframe_into_cdf(session,element,juris_path,project_root,error,load_refs=False)
            except Exception as e:
                if not element in error:
                    error[element] = {}
                error[element]["jurisdiction"] = \
                    f"""{e}\nThere may be something wrong with the file {element}.txt.
                    You may need to make changes to the Jurisdiction directory and try again."""

    # commit info in df to corresponding cdf table to db
    data, err = dbr.dframe_to_sql(df,session,element)
    if err:
        if not element in error:
            error[element] = {}
        error[element]["database"] = err
    return


def get_ids_for_foreign_keys(session,df1,element,foreign_key,refs,load_refs,error):
    """ TODO <fn> is foreign key"""
    df = df1.copy()
    # append the Id corresponding to <fn> from the db
    foreign_elt = f'{foreign_key[:-3]}'
    interim = f'{foreign_elt}_Name'

    target_list = []
    for r in refs:
        ref_name_field = db_routines.get_name_field(r)

        r_target = pd.read_sql_table(r,session.bind)[['Id',ref_name_field]]
        r_target.rename(columns={'Id':foreign_key,ref_name_field:interim},inplace=True)
        if element == 'ExternalIdentifier':
            # add column for cdf_table of referent
            r_target.loc[:,'cdf_element'] = r

        target_list.append(r_target)

    target = pd.concat(target_list)

    if element == 'ExternalIdentifier':
        # join on cdf_element name as well
        df = df.merge(
            target,how='left',left_on=['cdf_element','internal_name'],right_on=['cdf_element',interim])
        # rename 'Foreign_Id' to 'Foreign' for consistency in definition of missing
        # TODO why is ExternalIdentifier special in this regard?
        #  Is it that ExternalIdentifier doesn't have a name field?
        df.rename(columns={foreign_key:foreign_elt},inplace=True)
    else:
        df = df.merge(target,how='left',left_on=foreign_elt,right_on=interim)

    missing = df[(df[foreign_elt].notnull()) & (df[interim].isnull())]
    if missing.empty:
        df.drop([interim],axis=1)
    else:
        if load_refs:
            # Always try to handle/fill in the missing IDs
            raise ForeignKeyException(f'For some {element} records, {foreign_elt} was not found')
        else:
            if not element in error:
                error[element] = {}
            error[element]["foreign_key"] = \
            f"For some {element} records, {foreign_elt} was not found"
    return df


def check_element_against_raw_results(el,results_df,munger,numerical_columns,d):
    mode = munger.cdf_elements.loc[el,'source']

    # restrict to element in question; add row for 'none or unknown'
    none_series = pd.Series(
        {'cdf_internal_name':'none or unknown','raw_identifier_value':'none or unknown'},name=el)
    d_restricted = d[d.index == el].append(none_series)

    translatable = [x for x in d_restricted['raw_identifier_value']]

    if mode == 'row':
        # add munged column
        raw_fields = [f'{x}_{munger.field_rename_suffix}' for x in munger.cdf_elements.loc[el,'fields']]
        try:
            relevant = results_df[raw_fields].drop_duplicates()
        except KeyError:
            formula = munger.cdf_elements.loc[el,"raw_identifier_formula"]
            raise mr.MungeError(
                f'Required column from formula {formula} not found in file columns:\n{results_df.columns}')
        mr.add_munged_column(relevant,munger,el,mode=mode)
        # check for untranslatable items
        missing = relevant[~relevant[f'{el}_raw'].isin(translatable)]

    elif mode == 'column':
        # add munged column
        formula = munger.cdf_elements.loc[el,'raw_identifier_formula']
        text_field_list,last_text = mr.text_fragments_and_fields(formula)
        # create raw element column from formula applied to num col headers
        val = {}  # holds evaluations of fields
        raw_val = {}  # holds the raw value for column c
        for c in numerical_columns:
            raw_val[c] = ''
            # evaluate f-th entry in the column whose 0th entry is c
            for t,f in text_field_list:
                if int(f) == 0:  # TODO make prettier, assumes first line is col header in df
                    val[f] = c
                else:
                    val[f] = results_df.loc[f - 1,c]
                raw_val[c] += t + val[f]
        # TODO check that numerical cols are correctly identified even when more than one header row
        relevant = pd.DataFrame(pd.Series([raw_val[c] for c in numerical_columns],name=f'{el}_raw'))
        # check for untranslatable items # TODO code repeated from above
        if el in d.index:
            # if there are any items in d corresponding to the element <el>, do left merge and identify nulls
            # TODO more natural way to do this, without taking cases?
            relevant = relevant.merge(
                d.loc[[el]],how='left',left_on=f'{el}_raw',right_on='raw_identifier_value')
            missing = relevant[relevant.cdf_internal_name.isnull()]
        else:
            missing = relevant

    else:
        missing = pd.DataFrame([])
        print(f'Not checking {el}, which is not read from the records in the results file.')
    return missing


class ForeignKeyException(Exception):
    pass


if __name__ == '__main__':
    print('Done (juris_and_munger)!')
