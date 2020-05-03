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
    def create_db(self):
        # create db
        con = dbr.establish_connection()
        cur = con.cursor()
        dbr.create_database(con,cur,self.short_name)
        cur.close()
        con.close()
        return

    def prepare_candidatecontests(self,session):
        """create/update corresponding CandidateContest records for general and primary election contests
        (and insert in cdf db if they don't already exist)"""
        # TODO need to run this only when/if Offices change

        office_file = os.path.join(
            self.path_to_juris_dir,'context/Office.txt')
        co_kwargs = {'sep':'\t'}
        context_office = pd.read_csv(office_file,**co_kwargs)
        ui.resolve_nulls(context_office,office_file,kwargs=co_kwargs)

        cdf_office = pd.read_sql_table('Office',session.bind,index_col=None)
        cdf_ru = pd.read_sql_table('ReportingUnit',session.bind,index_col=None)

        # create dataframe of general election candidatecontests (cc) with right data
        cc = cdf_office.merge(
            context_office,left_on='Name',right_on='Name',suffixes=['','_context']
        ).merge(
            cdf_ru,left_on='ElectionDistrict',right_on='Name',suffixes=['_office','_ru']
        )
        # restrict to columns we need and set order
        cc = cc[['Name_office','VotesAllowed','NumberElected','NumberRunoff','Id_office','Id_ru','IsPartisan']]
        # ensure names of columns are correct (note we name contest after office)
        cc.rename(columns={'Id_ru':'ElectionDistrict_Id','Id_office':'Office_Id','Name_office':'Name'},inplace=True)
        # insert values for 'PrimaryParty_Id' column
        cc.loc[:,'PrimaryParty_Id'] = None
        # save cc with just general elections (as basis for defining party primary contests);
        # cc_all will accumulate all party primaries too
        cc_all = cc.copy()

        cdf_p = pd.read_sql_table('Party',session.bind,None,index_col=None)
        for party_id in cdf_p['Id'].to_list():
            cc_primary = cc[cc['IsPartisan']].copy()  # non-partisan contests don't have party primaries, so omit them.
            cc_primary.loc[:,'PrimaryParty_Id'] = party_id
            cc_primary.loc[:,'Name'] = cc_primary['Name'] + ' Primary;' + cdf_p[cdf_p['Id'] == party_id].iloc[0]['Name']
            # TODO can we use f'' here?
            cc_all = pd.concat([cc_all,cc_primary])

        dbr.dframe_to_sql(cc_all,session,'CandidateContest')
        return cc_all

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

    def extract_context(self,results,munger,element):
        # TODO
        """Return a dataframe of context info for <element> extracted
        from raw results file <results> via <munger>, suitable for
        applying add_to_context_dir()"""
        # TODO help user add appropriate lines to munger/raw_identifiers.txt and update munger
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


class Munger:
    def check_new_results_dataset(self,results,jurisdiction,sess,project_root='.'):
        """<results> is a results dataframe of a single <contest_type>;
        this routine should add what's necessary to the munger to treat the dataframe,
        keeping backwards compatibility and exiting gracefully if dataframe needs different munger."""

        # TODO write or omit!
        return

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
                [self.cdf_elements,self.atomic_reporting_unit_type,self.header_row_count,
                 self.field_name_row,] = read_munger_info_from_files(self.path_to_munger_dir)
        return

    def check_against_db(self,sess):
        """check that munger is consistent with db; offer user chance to correct munger"""
        checked = False
        while not checked:
            checked = True
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
                [self.cdf_elements,self.atomic_reporting_unit_type,self.header_row_count,
                 self.field_name_row] = read_munger_info_from_files(self.path_to_munger_dir)
        return

    def check_against_datafile(self,raw,cols_to_munge,count_columns):
        """check that munger is compatible with datafile <raw>;
        offer user chance to correct munger"""
        checked = False
        while not checked:
            checked = True
            problems = []
            # check for unmunged rows and report

            if problems:
                checked = False
                problem_str = '\n\t'.join(problems)
                print(f'Problems found:\n{problem_str} ')
                input(f'Correct the problems by editing the files in the directory {self.path_to_munger_dir}\n'
                      f'Then hit enter to continue.')
                [self.cdf_elements,self.atomic_reporting_unit_type,self.header_row_count,
                 self.field_name_row] = read_munger_info_from_files(self.path_to_munger_dir)
        # TODO write this function
        return

    def __init__(self,dir_path):
        """<dir_path> is the directory for the munger."""
        while not os.path.isdir(dir_path):
            input(f'{dir_path} is not a directory. Please create it and hit return to continue.')
        for ff in ['cdf_elements.txt','format.txt']:
            while not os.path.isfile(os.path.join(dir_path,ff)):
                input(f'Directory \n\t{dir_path}\ndoes not contain file {ff}.\n'
                      f'Please create the file and hit return to continue')

        self.name= os.path.basename(dir_path)  # e.g., 'nc_general'
        self.path_to_munger_dir=dir_path

        [self.cdf_elements,self.atomic_reporting_unit_type,self.header_row_count,
         self.field_name_row] = read_munger_info_from_files(self.path_to_munger_dir)

        self.field_rename_suffix = '___' # NB: must not match any suffix of a cdf element name;

        # used repeatedly, so calculated once for convenience
        self.field_list = set()
        for t,r in self.cdf_elements.iterrows():
            self.field_list=self.field_list.union(r['fields'])

# TODO before processing context files into db, alert user to any duplicate names.
#  Enforce name change? Or just suggest?


def read_munger_info_from_files(dir_path):
    # read cdf_element info and add column for list of fields used in formulas
    cdf_elements = pd.read_csv(os.path.join(dir_path,'cdf_elements.txt'),sep='\t',index_col='name').fillna('')
    cdf_elements.loc[:,'fields'] = None
    for i,r in cdf_elements.iterrows():
        text_field_list,last_text = mr.text_fragments_and_fields(cdf_elements.loc[i,'raw_identifier_formula'])
        cdf_elements.loc[i,'fields'] = [f for t,f in text_field_list]

    # read formatting info
    format_info = pd.read_csv(os.path.join(dir_path,'format.txt'),sep='\t',index_col='item')
    # TODO check that format.txt file is correct
    atomic_reporting_unit_type = format_info.loc['atomic_reporting_unit_type','value']
    field_name_row = int(format_info.loc['field_name_row','value'])
    header_row_count = int(format_info.loc['header_row_count','value'])
        # TODO maybe file separator and encoding should be in format.txt?

    # TODO if cdf_elements.txt uses any cdf_element names as fields in any raw_identifiers formula,
    #   will need to rename some columns of the raw file before processing.
    return [cdf_elements, atomic_reporting_unit_type,header_row_count,field_name_row]


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
            print(f'Directory {sd_path} already exists, will be preserved')
        else:
            print(f'Directory {sd_path} created')
    ensure_context_files(juris_path,project_root)
    return


def ensure_context_files(juris_path,project_root):
    """Check that the context files are complete and consistent with one another.
    Assumes context directory exists"""

    context_dir = os.path.join(juris_path,'context')
    # ensure all files exist
    templates = os.path.join(project_root,'templates/context_templates')
    template_list = os.listdir(templates)
    context_file_list = template_list

    for context_file in context_file_list:
        cf_path = os.path.join(context_dir,f'{context_file}.txt')
        # remark
        if context_file == 'remark':
            open(cf_path,'a').close()  # creates file if it doesn't exist already
            with open(cf_path,'r') as f:
                remark = f.read()
            print(f'Current contents of remark.txt is:\n{remark}\n')
            input(
                f'In the file context/remark.txt, add or correct anything that '
                f'user should know about the jurisdiction.\n'
                f'Then hit return to continue.')
        elif context_file == 'ExternalIdentifier':
            dedupe(cf_path)
        elif context_file == 'dictionary':
            dedupe(cf_path)
        else:
            # check dependencies
            check_dependencies(context_dir,context_file)
            # run dupe check
            dedupe(cf_path)
            # check for problematic null entries
            check_nulls(context_file,cf_path,project_root)
    return


def dedupe(f_path,warning='There are duplicates'):
    # TODO allow specificaiton of unique constraints
    df = pd.read_csv(f_path,sep='\t')
    dupes = True
    while dupes:
        dupes,df = ui.find_dupes(df)
        if dupes.empty:
            dupes = False
        else:
            input(f'WARNING: {warning}\n'
                  f'Edit the file to remove the duplication, then hit return to continue')
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
    assert os.path.isfile(f_path)
    element_df = pd.read_csv(f_path,sep='\t',index_col=None)

    # Find all dependent columns
    dependent = [c for c in element_df if c in d.keys()]
    changed_elements = set()
    report = [f'In context/{element}.txt:']
    for c in dependent:
        target = d[c]
        ed = pd.read_csv(os.path.join(context_dir,f'{element}.txt'),sep='\t',header=0).loc[:,c].to_list()
        ru = list(pd.read_csv(os.path.join(context_dir,f'{target}.txt'),sep='\t').loc[:,'Name'])
        missing = [x for x in ed if x not in ru and not np.isnan(x)]
        if len(missing) == 0:
            report.append(f'Every {c} is a {target}.')
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

if __name__ == '__main__':
    print('Done (states_and_files)!')
