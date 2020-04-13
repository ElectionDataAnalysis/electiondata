#!usr/bin/python3
import analyze_via_pandas as avp

if __name__ == '__main__':
    a = avp.contest_totals_from_rollup(
        '2018 General Election','North Carolina','county','mixed','unknown',
    '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/NC/rollups_from_cdf_db/FROMDB_NC',
    contest_group_types=['congressional','state-house','state-senate'])

    b = avp.append_total_and_pcts(a)

    c = avp.diff_from_avg(b,['congressional_pct','state-house_pct','state-senate_pct'])

    print('Done')
