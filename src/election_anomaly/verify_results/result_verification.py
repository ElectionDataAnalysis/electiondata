#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from election_anomaly import user_interface as ui
import os
import time



if __name__ == '__main__':

    desired_width = 320
    pd.set_option('display.width', desired_width)
    pd.set_option('display.max_columns', 10)

    #read both the result files
    print("Select result file 1 for comparision")
    fpath = ui.pick_path()
    Typetotal_df = pd.read_csv(fpath, sep='\t')

    print("Select result file 2 for comparision")
    fpath = ui.pick_path()
    TypetotalMod_df = pd.read_csv(fpath,sep='\t')



    #Find rows which are different between two DataFrames.
    comparison_df = Typetotal_df.merge(TypetotalMod_df, indicator=True, how='outer')


    """
        Case 1: A [contest, reportingunit, Selection] set in both the files and vote counts match. Both files match
        Case 2: A [contest, reportingunit, Selection] Set in both the files and count not same. Show difference in vote counts.
        Case 3: A [contest, reportingunit, Selection] pair in in result file 2 but not in result file 1
        Case 4: A [contest, reportingunit, Selection] pair in result file 1 but not in result file 2 
    """

    #case 1
    merge_types = ['left_only','right_only']
    diff_df = comparison_df[comparison_df._merge.isin(merge_types)]
    if len(diff_df.index) == 0:
        print('Both the files exactly match')

    else:

        #case 2
        #Define coulumns to group by and campare
        grp_cols = ['Contest', 'ReportingUnit', 'Selection']
        if 'CountItemType' in comparison_df.columns:
            grp_cols.append('CountItemType')
        differences_df = comparison_df.groupby(grp_cols).filter(lambda x: x.Count.count() > 1)

        if len(differences_df.index) == 0:
            print('Vote counts of all the contests match in both the files')
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
                    p_root =ui.pick_path(mode='directory')

                #Add timestap to download file name
                timestr = time.strftime("%Y%m%d_%H%M%S")
                fpath = os.path.join(p_root, 'result_differences_{}.csv'.format(timestr))
                differences_df.to_csv(fpath, index=True)

        missing_df = comparison_df.groupby(grp_cols).filter(lambda x: x.Count.count() == 1)
        describers = {'_merge': {'left_only': 'Row only in file 1', 'right_only': 'Row only in  file 2'}}
        missing_df = missing_df.replace(describers)

        #case3
        missing1_df = missing_df[missing_df['_merge'] == 'Row only in  file 2']
        if len(missing1_df.index) != 0:
            interact = input('Do you want to see the rows in file 2 missing from file 1.(y/n)?\n')
            if interact == 'y':
                print(missing1_df)


        #case 4
        missing2_df = missing_df[missing_df['_merge'] == 'Row only in file 1']
        if len(missing2_df.index) != 0:
            interact = input('Do you want to see the rows in file 1 missing from file 2.(y/n)?\n')
            if interact == 'y':
                print(missing2_df)
