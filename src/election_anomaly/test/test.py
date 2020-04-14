#!usr/bin/python3
import analyze_via_pandas as avp

if __name__ == '__main__':
    # a = avp.contest_totals_from_rollup('2018 General Election','North Carolina','county','mixed','unknown', '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/NC/rollups_from_cdf_db/FROMDB_NC',contest_group_types=['congressional','state-house','state-senate'])
    a = avp.contest_totals_from_rollup('2018general','Florida','county','total','unknown', '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/FL/rollups_from_cdf_db/FROMDB_FL',contest_group_types=['congressional','state-house','state-senate'])

    contest_list = ['Florida;Commissioner of Agriculture','Florida;US Congress Senate','Florida;Chief Financial Officer','Florida;Governor','Florida;Attorney General','congressional','state-house']
    b = avp.append_total_and_pcts(a[contest_list])

    c = avp.diff_from_avg(b,contest_list)

    print('Done')
