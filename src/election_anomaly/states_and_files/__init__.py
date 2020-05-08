#!usr/bin/python3
import os.path
import db_routines as dbr
import pandas as pd
import warnings   # TODO use warnings module to handle warnings in all files
import munge_routines as mr
import user_interface as ui
import re
import numpy as np
from pathlib import Path
import csv


class Jurisdiction:
    def check_against_raw_results(self,results_df,munger):
        """Warn user of any mungeable elements in <results_df> that are not
        translatable via context/dictionary.txt"""
        d = pd.read_csv(
            os.path.join(
                self.path_to_juris_dir,'context/dictionary.txt'
            ),sep='\t',index_col='cdf_element')

        # for each relevant element
        for el in munger.cdf_elements.index:
            mode = munger.cdf_elements.loc[el,'source']
            if mode in ['row','column']:
                # add munged column
                raw_fields = [f'{x}_{munger.field_rename_suffix}' for x in munger.cdf_elements.loc[el,'fields']]
                relevant = results_df[raw_fields].drop_duplicates()
                mr.add_munged_column(relevant,munger,el,mode=mode)
                # check for untranslatable items
                relevant = relevant.merge(
                    d.loc[el],left_on=f'{el}_raw',right_on='raw_identifier_value')
                missing = relevant[relevant.cdf_internal_name.isnull()]
                if not missing.empty:
                    ui.show_sample(missing,f'{el}s','cannot be translated')
        return

    def add_to_context_dir(self,element,df):
        """Add the data in the dataframe <df> to the file corresponding
        to <element> in the Jurisdiction's context folder.
        <df> must have all columns matching the columns in context/<element>.
        OK for <df> to have extra columns"""
        # TODO
        try:
            elts = pd.read_csv(f'{self.path_to_juris_dir}context/{element}.txt',sep='\t')
        except FileNotFoundError:
            print(f'File {self.path_to_juris_dir}context/{element}.txt does not exist')
            return
        else:
            for col in elts.columns:
                if col not in df.columns:
                    warnings.warn(f'WARNING: Column {col} not found, will be added with no data.')
                    print('Be sure all necessary data is added before processing the file')
                    df.loc[:,col] = ''
            # pull and order necessary columns from <df>
            df_new = pd.concat([df[elts.columns],elts])
            df_new.to_csv(f'{self.path_to_juris_dir}context/{element}.txt',sep='\t',index=False)

            # TODO insert into cdf.<element>, using mr.load_context_dframe_into_cdf
            #  in separate function, or in wrapper

            return

    def load_context_to_db(self,session,project_root):
        """ """  # TODO
        cdf_schema_def_dir = os.path.join(project_root,'election_anomaly/CDF_schema_def_info')
        # for element in context directory (except dictionary, remark)
        context_dir = os.path.join(self.path_to_juris_dir,'context')
        context_files = os.listdir(context_dir)
        context_elements = [
            x[:-4] for x in context_files if x != 'remark.txt' and x != 'dictionary.txt']
        for element in context_elements:
            # read df from context directory
            df = pd.read_csv(os.path.join(context_dir,f'{element}.txt'),sep='\t')
            # load df to db
            load_context_dframe_into_cdf(session,df,element,cdf_schema_def_dir)
        return

    def __init__(self,short_name,path_to_parent_dir,project_root=None,check_context=False):        # reporting_units,elections,parties,offices):
        """ short_name is the name of the directory containing the jurisdiction info, including data,
         and is used other places as well.
         path_to_parent_dir is the parent directory of dir_name
        """
        if not project_root:
            project_root = ui.get_project_root()

        self.short_name = ui.pick_or_create_directory(path_to_parent_dir,short_name)
        self.path_to_juris_dir = os.path.join(path_to_parent_dir, self.short_name)

        with open(os.path.join(self.path_to_juris_dir,'context/remark.txt'),'r') as f:
            remark = f.read()
        print(f'\n\nJurisdiction {short_name} initialized! Note:\n{remark}')


class Munger:
    def check_against_self(self):
        """check that munger is internally consistent; offer user chance to correct munger"""
        checked = False
        while not checked:
            checked = True
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
                checked = False
                problem_str = '\n\t'.join(problems)
                print(f'Problems found:\n{problem_str} ')
                input(f'Correct the problems by editing the files in the directory {self.path_to_munger_dir}\n'
                      f'Then hit enter to continue.')
                [self.cdf_elements,self.atomic_reporting_unit_type,self.header_row_count,self.field_name_row,
                 self.separator,self.encoding] = read_munger_info_from_files(self.path_to_munger_dir)
        return

    def check_against_db(self,sess):
        """check that munger is consistent with db; offer user chance to correct munger"""
        checked = False
        while not checked:
            problems = []
            # set of cdf_elements in cdf_elements.txt is same as set pulled from db
            [db_elements, db_enumerations, db_joins, db_others] = dbr.get_cdf_db_table_names(sess.bind)
            db_elements.add('CountItemType')
            db_elements.add('BallotMeasureSelection')
            db_elements.remove('CandidateSelection')
            db_elements.remove('ExternalIdentifier')
            db_elements.remove('VoteCount')
            db_elements.remove('Office')
            # TODO why these? make this programmatic

            m_elements = self.cdf_elements.index
            db_only = [x for x in db_elements if x not in m_elements]
            m_only = [x for x in m_elements if x not in db_elements]

            if db_only:
                db_str = ','.join(db_only)
                problems.append(f'Some cdf elements in the database are not listed in the munger: {db_str}')
            if m_only:
                m_str = ','.join(m_only)
                problems.append(f'Some cdf elements in the munger are not in the database: {m_str}')

            if problems:
                checked = False
                problem_str = '\n\t'.join(problems)
                print(f'Problems found:\n{problem_str} ')
                input(f'Correct the problems by editing the files in the directory {self.path_to_munger_dir}\n'
                      f'Then hit enter to continue.')
                [self.cdf_elements,self.atomic_reporting_unit_type,self.header_row_count,self.field_name_row,
                 self.separator,self.encoding] = read_munger_info_from_files(self.path_to_munger_dir)
            else:
                checked = True
                print(f'Munger {self.name} checked against database.')
        return

    def check_against_datafile(self,datafile_path):
        """check that munger is compatible with datafile <raw>;
        offer user chance to correct munger"""
        checked = False
        while not checked:
            problems = []

            # check encoding
            try:
                # TODO how best to get separator \t read correction from file?
                sep= self.separator.replace('\\t','\t')
                raw = pd.read_csv(
                    datafile_path,sep=sep,dtype=str,encoding=self.encoding,quoting=csv.QUOTE_MINIMAL,
                    header=None).fillna('')
            except UnicodeEncodeError:
                problems.append(f'Datafile is not encoded as {self.encoding}.')

            col_fields = '\n\t'.join(raw.iloc[self.field_name_row])
            cf_ok = input(f'Munger reads the following column fields from datafile (one per line):\n\t'
                          f'{col_fields}\n Are these correct (y/n)?\n')
            if cf_ok == 'y' and raw.shape[1] <3:
                cf_ok = input(f'Are you sure? Is each SEPARATE LINE above a single field (y/n)?\n')
            if cf_ok != 'y':
                problems.append('Either column_field_row or separator is incorrect.')
            # user confirm format.atomic_reporting_unit_type
            first_data_row = '\t'.join([f'{x}' for x in raw.iloc[self.header_row_count]])
            fdr_ok = input(f'Munger thinks the first data row is:\n{first_data_row}\n'
                           f'Is this correct (y/n)?\n')
            if fdr_ok != 'y':
                problems.append('header_row_count does not match the datafile.')
            arut_ok = input(f'Munger assumes that each vote count in the file is associated to '
                            f'a single {self.atomic_reporting_unit_type}. Is this correct (y/n)?\n')
            if arut_ok != 'y':
                problems.append(f'atomic_reporting_unit_type for munger does not match datafile')

            if problems:
                problem_str = '\n\t'.join(problems)
                print(f'Problems found:\n{problem_str} ')
                input(f'Correct the problems by editing the files in the directory {self.path_to_munger_dir}\n'
                      f'Then hit enter to continue.')
                [self.cdf_elements,self.atomic_reporting_unit_type,self.header_row_count,self.field_name_row,
                 self.separator,self.encoding] = read_munger_info_from_files(self.path_to_munger_dir)
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
        [self.cdf_elements,self.atomic_reporting_unit_type,self.header_row_count,self.field_name_row,
         self.separator,self.encoding] = read_munger_info_from_files(self.path_to_munger_dir)

        self.field_rename_suffix = '___' # NB: must not match any suffix of a cdf element name;

        # used repeatedly, so calculated once for convenience
        self.field_list = set()
        for t,r in self.cdf_elements.iterrows():
            self.field_list=self.field_list.union(r['fields'])


def read_munger_info_from_files(dir_path):
    # read cdf_element info and add column for list of fields used in formulas
    cdf_elements = pd.read_csv(os.path.join(dir_path,'cdf_elements.txt'),sep='\t',index_col='name').fillna('')
    cdf_elements.loc[:,'fields'] = None
    for i,r in cdf_elements.iterrows():
        text_field_list,last_text = mr.text_fragments_and_fields(cdf_elements.loc[i,'raw_identifier_formula'])
        cdf_elements.loc[i,'fields'] = [f for t,f in text_field_list]

    # read formatting info
    format_info = pd.read_csv(os.path.join(dir_path,'format.txt'),sep='\t',index_col='item')
    atomic_reporting_unit_type = format_info.loc['atomic_reporting_unit_type','value']
    field_name_row = int(format_info.loc['field_name_row','value'])
    header_row_count = int(format_info.loc['header_row_count','value'])
    separator = format_info.loc['separator','value']
    encoding = format_info.loc['encoding','value']
    # TODO warn if encoding not recognized

    # TODO if cdf_elements.txt uses any cdf_element names as fields in any raw_identifiers formula,
    #   will need to rename some columns of the raw file before processing.
    return [cdf_elements, atomic_reporting_unit_type,header_row_count,field_name_row,separator,encoding]


def ensure_jurisdiction_files(juris_path,project_root):
    # create jurisdiction directory
    try:
        os.mkdir(juris_path)
    except FileExistsError:
        print(f'Directory {juris_path} already exists, will be preserved')
    else:
        print(f'Directory {juris_path} created')

    # create subdirectories
    subdir_list = ['context','data','output']
    for sd in subdir_list:
        sd_path = os.path.join(juris_path,sd)
        try:
            os.mkdir(sd_path)
        except FileExistsError:
            print(f'Subdirectory {sd} already exists, will be preserved')
        else:
            print(f'Subdirectory {sd} created')
    ensure_context_files(juris_path,project_root)
    return


def ensure_context_files(juris_path,project_root):
    """Check that the context files are complete and consistent with one another.
    Assumes context directory exists. Assumes dictionary.txt is in the template file"""

    context_dir = os.path.join(juris_path,'context')
    # ensure all files exist
    templates = os.path.join(project_root,'templates/context_templates')
    template_list = [x[:-4] for x in os.listdir(templates)]
    # move 'dictionary' to front of template_list, so that it is created first
    template_list = ['dictionary'] + [x for x in template_list if x != 'dictionary']

    for context_file in template_list:
        print(f'Checking {context_file}.txt')
        cf_path = os.path.join(context_dir,f'{context_file}.txt')
        # if file does not already exist in context dir, create from template and invite user to fill
        try:
            temp = pd.read_csv(os.path.join(templates,f'{context_file}.txt'),sep='\t')
        except pd.error.EmptyDataError:
            print(f'Template file {context_file}.txt has no contents')
            temp = pd.DataFrame()
        if not os.path.isfile(cf_path):
            temp.to_csv(cf_path,sep='\t',index=False)
            input(f'Enter information in the file {context_file}.txt. Then hit return to continue.')

        # if file exists, check format against template
        cf_df = pd.read_csv(os.path.join(context_dir,f'{context_file}.txt'),sep='\t')
        format_confirmed = False
        while not format_confirmed:
            if set(cf_df.columns) != set(temp.columns):
                cols = '\t'.join(temp.columns.to_list())
                input(f'Columns of {context_file}.txt need to be (tab-separated):\n'
                      f' {cols}\n'
                      f'Edit {context_file}.txt, and hit return to continue.')
            else:
                format_confirmed = True

        if context_file == 'ExternalIdentifier':
            dedupe(cf_path)
        elif context_file == 'dictionary':
            dedupe(cf_path)
        else:
            # run dupe check
            dedupe(cf_path)
            # check for problematic null entries
            check_nulls(context_file,cf_path,project_root)
    # check dependencies
    for context_file in [x for x in template_list if x != 'remark' and x != 'dictionary']:
        # check dependencies
        check_dependencies(context_dir,context_file)
    # remark
    rem_path = os.path.join(context_dir,'remark.txt')
    try:
        with open(rem_path,'r') as f:
            remark = f.read()
        print(f'Current contents of remark.txt is:\n{remark}\n')
    except FileNotFoundError:
        open(rem_path, 'a').close()  # create empty file
    input(
        f'In the file context/remark.txt, add or correct anything that '
        f'user should know about the jurisdiction.\n'
        f'Then hit return to continue.')
    return


def ensure_munger_files(munger_name,project_root=None):
    """Check that the munger files are complete and consistent with one another.
    Assumes munger directory exists. Assumes dictionary.txt is in the template file"""
    if not project_root:
        project_root = ui.get_project_root()

    # define path to directory for the specific munger
    munger_path = os.path.join(project_root,'mungers',munger_name)
    # ensure all files exist
    templates = os.path.join(project_root,'templates/munger_templates')
    template_list = [x[:-4] for x in os.listdir(templates)]

    # create each file if necessary
    for munger_file in template_list:
        print(f'Checking {munger_file}.txt')
        cf_path = os.path.join(munger_path,f'{munger_file}.txt')
        # if file does not already exist in munger dir, create from template and invite user to fill
        try:
            temp = pd.read_csv(os.path.join(templates,f'{munger_file}.txt'),sep='\t')
        except pd.error.EmptyDataError:
            print(f'Template file {munger_file}.txt has no contents')
            temp = pd.DataFrame()
        if not os.path.isfile(cf_path):
            temp.to_csv(cf_path,sep='\t',index=False)
            input(f'Enter information in the file {munger_file}.txt. Then hit return to continue.')

        # if file exists, check format against template
        cf_df = pd.read_csv(os.path.join(munger_path,f'{munger_file}.txt'),sep='\t')
        format_confirmed = False
        while not format_confirmed:
            problems = []
            # check columns are correct
            if set(cf_df.columns) != set(temp.columns):
                cols = '\t'.join(temp.columns.to_list())
                problems.append(f'Columns of {munger_file}.txt need to be (tab-separated):\n'
                      f' {cols}\n')

            # check first column matches template
            if cf_df.empty or (cf_df.iloc[:,0] != temp.iloc[:,0]).any():
                first_col = '\n'.join(list(cf_df.iloc[:0]))
                problems.append(f'First column of {munger_file}.txt must be:\n{first_col}')
            if problems:
                prob_str = '\n\t'.join(problems)
                input(f'There are problems:\n\t{prob_str}\nEdit {munger_file}.txt, and hit return to continue.')
            else:
                format_confirmed = True
    # check contents of each file
    check_munger_file_contents(munger_name,project_root=project_root)
    return


def check_munger_file_contents(munger_name,project_root=None):
    """check that munger files are internally consistent; offer user chance to correct"""
    # define path to munger's directory
    if not project_root:
        project_root = ui.get_project_root()
    munger_dir = os.path.join(project_root,'mungers',munger_name)

    checked = False
    while not checked:
        problems = []

        # read cdf_elements and format from files
        cdf_elements = pd.read_csv(os.path.join(munger_dir,'cdf_elements.txt'),sep='\t').fillna('')
        format_df = pd.read_csv(os.path.join(munger_dir,'format.txt'),sep='\t',index_col='item').fillna('')
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
        for i,r in cdf_elements[cdf_elements.source == 'column'].iterrows():
            if p_not_just_digits.search(r['raw_identifier_formula']):
                bad_column_formula.add(r['raw_identifier_formula'])
            else:
                integer_list = [int(x) for x in p_catch_digits.findall(r['raw_identifier_formula'])]
                bad_integer_list = [x for x in integer_list if (x > format_df.header_row_count-1 or x < 0)]
                if bad_integer_list:
                    bad_column_formula.add(r['raw_identifier_formula'])
        if bad_column_formula:
            cf_str = ','.join(bad_column_formula)
            problems.append(f'''At least one column-source formula in cdf_elements.txt has bad syntax: {cf_str} ''')

        # check entries in format.txt
        if not format_df.loc['header_row_count','value'].isnumeric():
            problems.append('In format file, header_row_count must be an integer')
        if not format_df.loc['field_name_row','value'].isnumeric():
            problems.append('In format file, field_name_row must be an integer')

        # TODO if field in formula matches an element self.cdf_element.index,
        #  check that rename is not also a column
        if problems:
            problem_str = '\n\t'.join(problems)
            print(f'Problems found:\n{problem_str} ')
            input(f'Correct the problems by editing files in {munger_dir}\n'
                  f'Then hit enter to continue.')
        else:
            checked = True
    return


def dedupe(f_path,warning='There are duplicates'):
    # TODO allow specificaiton of unique constraints
    df = pd.read_csv(f_path,sep='\t')
    dupes = True
    while dupes:
        dupes,df = ui.find_dupes(df)
        if dupes.empty:
            dupes = False
            print(f'No dupes in {f_path}')
        else:
            print(f'WARNING: {warning}\n')
            ui.show_sample(dupes,'lines','are duplicates')
            input(f'Edit the file to remove the duplication, then hit return to continue')
            df = pd.read_csv(f_path,sep='\t')
    return df


def check_nulls(element,f_path,project_root):
    # TODO
    nn_path = os.path.join(
        project_root,'election_anomaly/CDF_schema_def_info/elements',element,'not_null_fields.txt')
    not_nulls = pd.read_csv(nn_path,sep='\t')
    df = pd.read_csv(f_path,sep='\t')

    nulls = True
    while nulls:
        problems = []
        for nn in not_nulls.not_null_fields.unique():
            # if nn is an Id, name in context file is element name
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


def check_dependencies(context_dir,element):
    """Looks in <context_dir> to check that every dependent column in <element>.txt
    is listed in the corresponding context file. Note: <context_dir> assumed to exist.
    """
    d = context_dependency_dictionary()
    # context_dir = os.path.join(self.path_to_juris_dir,"context")
    f_path = os.path.join(context_dir,f'{element}.txt')
    assert os.path.isdir(context_dir)
    element_df = pd.read_csv(f_path,sep='\t',index_col=None)

    # Find all dependent columns
    dependent = [c for c in element_df if c in d.keys()]
    changed_elements = set()
    report = [f'In context/{element}.txt:']
    for c in dependent:
        target = d[c]
        ed = pd.read_csv(os.path.join(
            context_dir,f'{element}.txt'),sep='\t',header=0).fillna('').loc[:,c].unique()

        # create list of elements, removing any nulls
        ru = list(pd.read_csv(os.path.join(context_dir,f'{target}.txt'),sep='\t').fillna('').loc[:,'Name'])
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
            changed_elements.update(check_dependencies(context_dir,target))
    if dependent:
        print ('\n\t'.join(report))
    if changed_elements:
        print(f'(Directory is {context_dir}')
    return changed_elements


def finalize_context_element(context_dir,element):
    """Guides user to make any necessary or desired changes in context/<element>.txt
    and makes corresponding changes to db""" # TODO rewrite desciption
    finalized = False
    while not finalized:
        #self.prepare_context_and_db(element,results,jurisdiction,sess,project_path=project_root)

        # check dependencies
        all_ok = False
        while not all_ok:
            changed_elements = check_dependencies(context_dir,element)
            if changed_elements:
                # recheck items from change list
                for ce in changed_elements:
                    check_dependencies(context_dir,ce)
            else:
                all_ok=True

        fin = input(f'Is the file {element}.txt finalized to your satisfaction (y/n)?\n')
        if fin == 'y':
            finalized = True
        else:
            input('Make your changes, then hit return to continue.')
    return


def context_dependency_dictionary():
    """Certain fields in context files refer to other context files.
    E.g., ElectionDistricts are ReportingUnits"""
    d = {'ElectionDistrict':'ReportingUnit','Office':'Office','PrimaryParty':'Party','Party':'Party',
         'Election':'Election'}
    return d


# TODO before processing context files into db, alert user to any duplicate names.
#  Enforce name change? Or just suggest?
def load_context_dframe_into_cdf(
        session,source_df1,element,cdf_schema_def_dir):
    """<source_df> should have all info needed for insertion into cdf:
    for enumerations, the plaintext value of the enumeration (e.g., 'precinct')
    for other fields, the value of the field (e.g., 'North Carolina;Alamance County').
"""
    if not source_df1.empty:
        # TODO check that source_df has the right format

        # dedupe source_df
        dupes,source_df = ui.find_dupes(source_df1)
        if not dupes.empty:
            print(f'WARNING: duplicates removed from dataframe, may indicate a problem.\n')
            ui.show_sample(dupes,f'lines in {element} source data','are duplicates')

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
                    source_df = mr.enum_col_to_id_othertext(source_df,e,cdf_e)
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
        dbr.dframe_to_sql(source_df,session,element)
    return


if __name__ == '__main__':
    print('Done (states_and_files)!')
