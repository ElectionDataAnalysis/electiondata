#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from election_anomaly import user_interface as ui
import os
import time
import pandas.io.common as pderror


def check_file_compatibility(Typetotal_df, TypetotalMod_df,fpath, fpath1):
    error = []
    # Check if the files have similar header row
    Typetotal_df_header = list(Typetotal_df.columns)
    TypetotalMod_df_header = list(TypetotalMod_df.columns)

    Typetotal_df_header.sort()
    TypetotalMod_df_header.sort()

    if Typetotal_df_header != TypetotalMod_df_header:
        error.append('The header rows of both files do not match. The files cannot be compared')

    # check if the files have data
    if len(Typetotal_df.index) == 0:
        error.append(f'Files cannot be compared to since {fpath} has no data.')

    if len(TypetotalMod_df.index) == 0:
        error.append(f'Files cannot be compared to since {fpath1} has no data.')

    # Define group columns for comparision and check if the set is unique in each file.
    grp_cols = ['Contest', 'ReportingUnit', 'Selection']
    if 'CountItemType' in Typetotal_df.columns:
        grp_cols.append('CountItemType')

    if Typetotal_df.set_index(grp_cols).index.is_unique is False:
        error.append(
            f'The files cannot be compared since columns {grp_cols} of {fpath} do not uniquely identify the rows.')

    if TypetotalMod_df.set_index(grp_cols).index.is_unique is False:
        error.append(
            f'The files cannot be compared since columns {grp_cols} of {fpath1} do not uniquely identify the rows.')
    return error

def file_comparision(Typetotal_df, TypetotalMod_df,fpath, fpath1):
    # Find rows which are different between two DataFrames.
    comparison_df = Typetotal_df.merge(TypetotalMod_df, indicator=True, how='outer')
    """
        Case 1: A [contest, reportingunit, Selection] set are in both the files and vote counts match. Both files match
        Case 2: A [contest, reportingunit, Selection] set are in both the files and vote counts do not match. Show difference in vote counts.
        Case 3: A [contest, reportingunit, Selection] pair in in result file 2 but not in result file 1
        Case 4: A [contest, reportingunit, Selection] pair in result file 1 but not in result file 2 
    """
    # case 1
    merge_types = ['left_only', 'right_only']
    diff_df = comparison_df[comparison_df._merge.isin(merge_types)]
    if len(diff_df.index) == 0:
        print(f'No discrepancies found between \n {fpath1} and \n {fpath}')
    else:
        # case 2
        # Define columns to group by and compare
        grp_cols = ['Contest', 'ReportingUnit', 'Selection']
        if 'CountItemType' in comparison_df.columns:
            grp_cols.append('CountItemType')
        differences_df = comparison_df.groupby(grp_cols).filter(lambda x: x.Count.count() > 1)

        if len(differences_df.index) == 0:
            print('Vote counts of the matching rows from both files are same. Proceed to check the unmatched rows')
        else:
            # remove duplicate rows and display one row for each contest whose counts vary

            describers = {'_merge': {'left_only': 'Vote Count in Result file 1', 'right_only': 'Vote Count in file 2'}}
            differences_df = differences_df.replace(describers)
            differences_df_top = list(differences_df.columns)
            differences_df_top.remove('Count')
            differences_df = differences_df.set_index(differences_df_top)
            differences_df = differences_df.unstack()

            count = len(differences_df.index)
            print("Vote counts do not match in {} rows".format(count))

            interact = input('Do you want to look at these rows  (y/n)?\n')
            if interact == 'y':
                print(differences_df)

            interact = input('Do you want to export the unmatched rows to a file  (y/n)?\n')
            if interact == 'y':
                p_root = os.getcwd()
                print("The default folder for the file is {}".format(p_root))
                interact2 = input('Do you want to select a different folder(y/n)?\n')
                if interact2 == 'y':
                    p_root = ui.pick_path(mode='directory')

                # Add timestap to download file name
                timestr = time.strftime("%Y%m%d_%H%M%S")
                fpath = os.path.join(p_root, 'result_differences_{}.csv'.format(timestr))
                differences_df.to_csv(fpath, index=True)

        missing_df = comparison_df.groupby(grp_cols).filter(lambda x: x.Count.count() == 1)
        describers = {'_merge': {'left_only': 'Row only in file 1', 'right_only': 'Row only in  file 2'}}
        missing_df = missing_df.replace(describers)

        # case3
        missing1_df = missing_df[missing_df['_merge'] == 'Row only in  file 2']
        if len(missing1_df.index) != 0:
            interact = input('Do you want to see the rows in file 2 missing from file 1.(y/n)?\n')
            if interact == 'y':
                print(missing1_df)
        else:
            print('All the rows in file 2 are in file 1')

        # case 4
        missing2_df = missing_df[missing_df['_merge'] == 'Row only in file 1']
        if len(missing2_df.index) != 0:
            interact = input('Do you want to see the rows in file 1 missing from file 2.(y/n)?\n')
            if interact == 'y':
                print(missing2_df)
        else:
            print('All the rows in file 1 are in file 2')

if __name__ == '__main__':

    desired_width = 320
    pd.set_option('display.width', desired_width)
    pd.set_option('display.max_columns', 10)

    # read both the result file
    fpath = ''
    fpath1 = ''
    Typetotal_df = TypetotalMod_df = pd.DataFrame

    while True:
        file1 =  ''
        try:
            print("Select result file 1 for comparision")
            fpath = ui.pick_path()
            Typetotal_df = pd.read_csv(fpath, sep='\t')
            file1 = os.path.basename(fpath)
            break
        except pderror.EmptyDataError:
            print (f'The input file {file1} is empty. Select another file')

    while True:
        file2 = ''
        try:
            print("Select result file 2 for comparision")
            fpath1 = ui.pick_path()
            TypetotalMod_df = pd.read_csv(fpath1,sep='\t')
            file2 = os.path.basename(fpath1)
            break
        except pderror.EmptyDataError:
            print(f'The input file {file2}is empty. Select another file')

    error = check_file_compatibility(Typetotal_df, TypetotalMod_df,fpath, fpath1)
    if error:
        print(*error, sep='\n')
        exit(0)

    file_comparision(Typetotal_df, TypetotalMod_df,fpath, fpath1)
    exit()










