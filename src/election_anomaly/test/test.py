#!usr/bin/python3
import os.path
from sqlalchemy.orm import sessionmaker
import db_routines as dbr
import user_interface as ui
import analyze_via_pandas as avp

if __name__ == '__main__':
    a = avp.contest_totals_from_rollup(
        '2018 General Election','North Carolina','county','mixed','unknown',
    '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/NC/rollups_from_cdf_db/FROMDB_NC')

    print('Done')
