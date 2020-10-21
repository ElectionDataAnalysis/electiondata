import election_data_analysis as e
import pytest

# #### Tests start below #### #
# For each state, run at least 6 tests:
# 1. Presidential
# 2. One statewide chosen at random
# 3. One senate
# 4. One rep
# 5. If vote type is available, slice one of the above by vote type
# 6. If vote type is available, check that totals match vote type sums


### North Carolina Data Loading Tests ###
#constants
ok = {
    "nc16g": e.data_exists('2016 General','North Carolina', dbname=dbname),
    "nc18g": e.data_exists('2018 General','North Carolina', dbname=dbname),
    "nc20p": e.data_exists('2020 Primary','North Carolina', dbname=dbname),
    "fl16g": e.data_exists('2016 General','Florida', dbname=dbname),
    "fl18g": e.data_exists('2018 General','Florida', dbname=dbname),
    "fl20p": e.data_exists('2020 Primary','Florida', dbname=dbname),
    "pa16g": e.data_exists('2016 General','Pennsylvania', dbname=dbname),
    "pa18g": e.data_exists('2018 General','Pennsylvania', dbname=dbname),
    "pa20p": e.data_exists('2020 Primary','Pennsylvania', dbname=dbname),
    "ga16g": e.data_exists('2016 General','Georgia', dbname=dbname),
    "ga18g": e.data_exists('2018 General','Georgia', dbname=dbname),
    "ga20p": e.data_exists('2020 Primary','Georgia', dbname=dbname),
    "sc20p": e.data_exists('2020 Primary','South Carolina', dbname=dbname),
    "in16g": e.data_exists('2016 General','Indiana', dbname=dbname),
    "in18g": e.data_exists('2018 General','Indiana', dbname=dbname),
    "in20p": e.data_exists('2020 Primary','Indiana', dbname=dbname),
    "ar18g": e.data_exists('2018 General','Arkansas', dbname=dbname),
    "ar20p": e.data_exists('2018 General','Arkansas', dbname=dbname),
    "mi16g": e.data_exists('2016 General','Michigan', dbname=dbname),
    "mi18g": e.data_exists('2018 General','Michigan', dbname=dbname),
    "mi20p": e.data_exists('2020 Primary','Michigan', dbname=dbname),
    "de20p": e.data_exists('2020 Primary','Delaware', dbname=dbname),
    "oh16g": e.data_exists('2016 General','Ohio', dbname=dbname),
    "oh18g": e.data_exists('2018 General','Ohio', dbname=dbname),
    "il16g": e.data_exists('2016 General','Illinois', dbname=dbname),
    "il18g": e.data_exists('2018 General','Illinois', dbname=dbname),
    "il20p": e.data_exists('2020 Primary','Illinois', dbname=dbname),
    "ca16g": e.data_exists('2016 General','California', dbname=dbname),
    "ca18g": e.data_exists('2018 General','California', dbname=dbname),
    "ca20p": e.data_exists('2020 Primary','California', dbname=dbname),
    "co16g": e.data_exists('2016 General','Colorado', dbname=dbname),
    "co18g": e.data_exists('2018 General','Colorado', dbname=dbname),
    "co20p": e.data_exists('2020 Primary','Colorado', dbname=dbname),
}

print(ok)

### NC dataloading tests ###
#NC16 tests
@pytest.mark.skipif(not ok["nc16g"], reason="No NC 2016 General data")
def test_nc_presidential_16(dbname=None):
    assert(
            e.contest_total(
            "2016 General",
            "North Carolina",
            "US President (NC)",
            dbname=dbname,
        )
            == 4741564
    )


@pytest.mark.skipif(not ok["nc16g"], reason="No NC 2016 General data")
def test_nc_statewide_totals_16(dbname=None):
    assert(
            e.contest_total(
            "2016 General",
            "North Carolina",
            "NC Treasurer",
            dbname=dbname,
        )
            == 4502784
    )

@pytest.mark.skipif(not ok["nc16g"], reason="No NC 2016 General data")
def test_nc_senate_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "North Carolina",
            "US Senate NC",
            dbname=dbname,
        )
            == 4691133
    )

@pytest.mark.skipif(not ok["nc16g"], reason="No NC 2016 General data")
def test_nc_rep_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "North Carolina",
            "US House NC District 4",
            dbname=dbname,
        )
            == 409541
    )

@pytest.mark.skipif(not ok["nc16g"], reason="No NC 2016 General data")
def test_nc_contest_by_vote_type_16(dbname=None):
    assert (
            e.count_type_total(
            "2016 General",
            "North Carolina",
            "US House NC District 4",
            "absentee-mail",
            dbname=dbname,
        )
            == 20881
    )

@pytest.mark.skipif(not ok["nc16g"], reason="No NC 2016 General data")
def test_nc_totals_match_vote_type_16(dbname=None):
    assert e.check_totals_match_vote_types("2016 General", "North Carolina") == True


#NC18 tests
@pytest.mark.skipif(not ok["nc18g"], reason="No NC 2018 General data")
def test_nc_presidential_18(dbname=None):
    # No presidential contests in 2018
    assert True == True


@pytest.mark.skipif(not ok["nc18g"], reason="No NC 2018 General data")
def test_nc_statewide_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "North Carolina",
            "US House NC District 3",
            dbname=dbname,
        )
            == 187901
    )


@pytest.mark.skipif(not ok["nc18g"], reason="No NC 2018 General data")
def test_nc_senate_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "North Carolina",
            "NC Senate District 15",
            dbname=dbname,
        )
            == 83175
    )


@pytest.mark.skipif(not ok["nc18g"], reason="No NC 2018 General data")
def test_nc_house_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "North Carolina",
            "NC House District 1",
            dbname=dbname,
        )
            == 27775
    )


@pytest.mark.skipif(not ok["nc18g"], reason="No NC 2018 General data")
def test_nc_contest_by_vote_type_18(dbname=None):
    assert (
            e.count_type_total(
            "2018 General",
            "North Carolina",
            "US House NC District 4",
            "absentee-mail",
            dbname=dbname,
        )
            == 10778
    )

@pytest.mark.skipif(not ok["nc18g"], reason="No NC 2018 General data")
def test_nc_totals_match_vote_type_18(dbname=None):
    assert e.check_totals_match_vote_types("2018 General", "North Carolina") == True


#NC20 Tests
@pytest.mark.skipif(not ok["nc20p"], reason="No NC 2020 Primary data")
def test_nc_presidential_20(dbname=None):
    assert(
            e.contest_total(
            "2020 Primary",
            "North Carolina",
            "US President (NC) (Democratic Party)",
            dbname=dbname,
        )
            == 1331366
    )

@pytest.mark.skipif(not ok["nc20p"], reason="No NC 2020 Primary data")
def test_nc_statewide_totals_20(dbname=None):
    assert(
            e.contest_total(
            "2020 Primary",
            "North Carolina",
            "NC Governor (Democratic Party)",
            dbname=dbname,
        )
            == 1293652
    )

@pytest.mark.skipif(not ok["nc20p"], reason="No NC 2020 Primary data")
def test_nc_senate_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "North Carolina",
            "US Senate NC (Democratic Party)",
            dbname=dbname,
        )
            == 1260090
    )

@pytest.mark.skipif(not ok["nc20p"], reason="No NC 2020 Primary data")
def test_nc_rep_20_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "North Carolina",
            "US House NC District 4 (Republican Party)",
            dbname=dbname,
        )
            == 36096
    )

@pytest.mark.skipif(not ok["nc20p"], reason="No NC 2020 Primary data")
def test_nc_contest_by_vote_type_20(dbname=None):
    assert (
            e.count_type_total(
            "2020 Primary",
            "North Carolina",
            "US House NC District 4 (Republican Party)",
            "absentee-mail",
            dbname=dbname,
        )
            == 426
    )

@pytest.mark.skipif(not ok["nc20p"], reason="No NC 2020 Primary data")
def test_nc_totals_match_vote_type_20(dbname=None):
    assert e.check_totals_match_vote_types("2020 General", "North Carolina") == True



### Florida Data Loading Tests ###
#FL16 test
@pytest.mark.skipif(not ok["fl16g"], reason="No FL 2016 General data")
def test_fl_presidential(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Florida",
            "US President (FL)",
            dbname=dbname,
        )
            == 9420039
    )

@pytest.mark.skipif(not ok["fl16g"], reason="No FL 2016 General data")
def test_fl_statewide_totals(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Florida",
            "US Senate FL",
            dbname=dbname,
        )
            == 9301820
    )

@pytest.mark.skipif(not ok["fl16g"], reason="No FL 2016 General data")
def test_fl_senate_totals(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Florida",
            "FL Senate District 3",
            dbname=dbname,
        )
            == 236480
    )

@pytest.mark.skipif(not ok["fl16g"], reason="No FL 2016 General data")
def test_fl_house_totals(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Florida",
            "US House FL District 10",
            dbname=dbname,
        )
            == 305989
    )

@pytest.mark.skipif(not ok["fl16g"], reason="No FL 2016 General data")
def test_fl_contest_by_vote_type(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["fl16g"], reason="No FL 2016 General data")
def test_fl_totals_match_vote_type(dbname=None):
    # Vote type not available
    assert True == True

#FL18 test
@pytest.mark.skipif(not ok["fl18g"], reason="No FL 2018 General data")
def test_fl_presidential_18(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["fl18g"], reason="No FL 2018 General data")
def test_fl_statewide_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Florida",
            "US Senate FL",
            dbname=dbname,
        )
            == 8190005
    )

@pytest.mark.skipif(not ok["fl18g"], reason="No FL 2018 General data")
def test_fl_senate_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Florida",
            "FL Senate District 4",
            dbname=dbname,
        )
            == 235459
    )

@pytest.mark.skipif(not ok["fl18g"], reason="No FL 2018 General data")
def test_fl_house_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Florida",
            "FL House District 11",
            dbname=dbname,
        )
            == 85479
    )

@pytest.mark.skipif(not ok["fl18g"], reason="No FL 2018 General data")
def test_fl_contest_by_vote_type_18(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["fl18g"], reason="No FL 2018 General data")
def test_fl_totals_match_vote_type_18(dbname=None):
    # Vote type not available
    assert True == True

#FL20 test
@pytest.mark.skipif(not ok["fl20p"], reason="No FL 2020 Primary data")
def test_fl_presidential_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Florida",
            "US President (FL)",
            dbname=dbname,
        )
            == 5958306
    )

@pytest.mark.skipif(not ok["fl20p"], reason="No FL 2020 Primary data")
def test_fl_statewide_totals_20(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["fl20p"], reason="No FL 2020 Primary data")
def test_fl_senate_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Florida",
            "FL Senate District 21",
            dbname=dbname,
        )
            == 54988
    )

@pytest.mark.skipif(not ok["fl20p"], reason="No FL 2020 Primary data")
def test_fl_house_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Florida",
            "FL House District 11",
            dbname=dbname,
        )
            == 17445
    )

@pytest.mark.skipif(not ok["fl20p"], reason="No FL 2020 Primary data")
def test_fl_contest_by_vote_type_20(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["fl20p"], reason="No FL 2020 Primary data")
def test_fl_totals_match_vote_type_20(dbname=None):
    # Vote type not available
    assert True == True

### Pennsylvania Data Loading Tests ###
#PA16 tests
@pytest.mark.skipif(not ok["pa16g"], reason="No PA 2016 General data")
def test_pa_presidential_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Pennsylvania",
            "US President (PA)",
            dbname=dbname,
        )
            == 6115402
    )

@pytest.mark.skipif(not ok["pa16g"], reason="No PA 2016 General data")
def test_pa_statewide_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Pennsylvania",
            "PA Auditor General",
            dbname=dbname,
        )
            == 5916931
    )

@pytest.mark.skipif(not ok["pa16g"], reason="No PA 2016 General data")
def test_pa_senate_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Pennsylvania",
            "PA Senate District 41",
            dbname=dbname,
        )
            == 112283
    )

@pytest.mark.skipif(not ok["pa16g"], reason="No PA 2016 General data")
def test_pa_house_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Pennsylvania",
            "PA House District 21",
            dbname=dbname,
        )
            == 26453
    )

@pytest.mark.skipif(not ok["pa16g"], reason="No PA 2016 General data")
def test_pa_contest_by_vote_type_16(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["pa16g"], reason="No PA 2016 General data")
def test_pa_totals_match_vote_type_16(dbname=None):
    # Vote type not available
    assert True == True


#PA18 test
@pytest.mark.skipif(not ok["pa18g"], reason="No PA 2018 General data")
def test_pa_presidential_18(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["pa18g"], reason="No PA 2018 General data")
def test_pa_statewide_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Pennsylvania",
            "PA Governor",
            dbname=dbname,
        )
            == 5012555
    )

@pytest.mark.skipif(not ok["pa18g"], reason="No PA 2018 General data")
def test_pa_senate_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Pennsylvania",
            "PA Senate District 20",
            dbname=dbname,
        )
            == 81817
    )

@pytest.mark.skipif(not ok["pa18g"], reason="No PA 2018 General data")
def test_pa_house_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Pennsylvania",
            "PA House District 103",
            dbname=dbname,
        )
            == 18363
    )

@pytest.mark.skipif(not ok["pa18g"], reason="No PA 2018 General data")
def test_pa_contest_by_vote_type_18(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["pa18g"], reason="No PA 2018 General data")
def test_pa_totals_match_vote_type_18(dbname=None):
    # Vote type not available
    assert True == True

#PA20
@pytest.mark.skipif(not ok["pa20p"], reason="No PA 2020 Primary data")
def test_pa_presidential_20(dbname=None):
    assert (
            e.contest_total(
            "2020 General",
            "Pennsylvania",
            "US President (PA)",
            dbname=dbname,
        )
            == 2739007
    )

@pytest.mark.skipif(not ok["pa20p"], reason="No PA 2020 Primary data")
def test_pa_statewide_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 General",
            "Pennsylvania",
            "PA Governor",
            dbname=dbname,
        )
            == 2484582
    )

@pytest.mark.skipif(not ok["pa20p"], reason="No PA 2020 Primary data")
def test_pa_senate_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 General",
            "Pennsylvania",
            "PA Senate District 20",
            dbname=dbname,
        )
            == 67898
    )

@pytest.mark.skipif(not ok["pa20p"], reason="No PA 2020 Primary data")
def test_pa_house_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 General",
            "Pennsylvania",
            "PA House District 100",
            dbname=dbname,
        )
            == 6327
    )

@pytest.mark.skipif(not ok["pa20p"], reason="No PA 2020 Primary data")
def test_pa_contest_by_vote_type_20(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["pa20p"], reason="No PA 2020 Primary data")
def test_pa_totals_match_vote_type_20(dbname=None):
    # Vote type not available
    assert True == True


### Georgia Data Loading Tests ###
#GA16
@pytest.mark.skipif(not ok["ga16g"], reason="No GA 2016 General data")
def test_ga_presidential_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Georgia",
            "US President (GA)",
            dbname=dbname,
        )
            == 4092373
    )

@pytest.mark.skipif(not ok["ga16g"], reason="No GA 2016 General data")
def test_ga_statewide_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Georgia",
            "US Senate GA",
            dbname=dbname,
        )
            == 3897792
    )

@pytest.mark.skipif(not ok["ga16g"], reason="No GA 2016 General data")
def test_ga_senate_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Georgia",
            "GA Senate District 13",
            dbname=dbname,
        )
            == 60387
    )

@pytest.mark.skipif(not ok["ga16g"], reason="No GA 2016 General data")
def test_ga_house_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Georgia",
            "GA House District 7",
            dbname=dbname,
        )
            == 21666
    )

@pytest.mark.skipif(not ok["ga16g"], reason="No GA 2016 General data")
def test_ga_contest_by_vote_type_16(dbname=None):
    assert (
            e.count_type_total(
            "2016 General",
            "Georgia",
            "GA House District 7",
            "absentee-mail",
            dbname=dbname,
        )
            == 1244
    )

@pytest.mark.skipif(not ok["ga16g"], reason="No GA 2016 General data")
def test_ga_totals_match_vote_type_16(dbname=None):
    assert e.check_totals_match_vote_types("2016 General", "Georgia") == True

#GA18
@pytest.mark.skipif(not ok["ga18g"], reason="No GA 2018 General data")
def test_ga_presidential_18(dbname=None):
    #no presidential contests in 2018
    assert True == True

@pytest.mark.skipif(not ok["ga18g"], reason="No GA 2018 General data")
def test_ga_statewide_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Georgia",
            "GA Governor",
            dbname=dbname,
        )
            == 3939328
    )

@pytest.mark.skipif(not ok["ga18g"], reason="No GA 2018 General data")
def test_ga_senate_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Georgia",
            "GA Senate District 5",
            dbname=dbname,
        )
            == 34429
    )

@pytest.mark.skipif(not ok["ga18g"], reason="No GA 2018 General data")
def test_ga_house_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Georgia",
            "US House GA District 2",
            dbname=dbname,
        )
            == 229171
    )

@pytest.mark.skipif(not ok["ga18g"], reason="No GA 2018 General data")
def test_ga_contest_by_vote_type_18(dbname=None):
    assert (
            e.count_type_total(
            "2018 General",
            "Georgia",
            "GA Senate District 5",
            "absentee-mail",
            dbname=dbname,
        )
            == 2335
    )

@pytest.mark.skipif(not ok["ga18g"], reason="No GA 2018 General data")
def test_ga_totals_match_vote_type_18(dbname=None):
    assert e.check_totals_match_vote_types("2018 General", "Georgia") == True

#GA16
@pytest.mark.skipif(not ok["ga20p"], reason="No GA 2020 Primary data")
def test_ga_presidential_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Georgia",
            "US President (GA) (Republican Party)",
            dbname=dbname,
        )
            == 947352
    )

@pytest.mark.skipif(not ok["ga20p"], reason="No GA 2020 Primary data")
def test_ga_statewide_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Georgia",
            "US Senate GA (Republican Party)",
            dbname=dbname,
        )
            == 992555
    )

@pytest.mark.skipif(not ok["ga20p"], reason="No GA 2020 Primary data")
def test_ga_senate_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Georgia",
            "GA Senate District 8 (Democratic Party)",
            dbname=dbname,
        )
            == 9103
    )

@pytest.mark.skipif(not ok["ga20p"], reason="No GA 2020 Primary data")
def test_ga_house_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Georgia",
            "GA House District 7 (Democratic Party)",
            dbname=dbname,
        )
            == 2193
    )

@pytest.mark.skipif(not ok["ga20p"], reason="No GA 2020 Primary data")
def test_ga_contest_by_vote_type_20(dbname=None):
    assert (
            e.count_type_total(
            "2020 Primary",
            "Georgia",
            "GA House District 7 (Democratic Party)",
            "absentee-mail",
            dbname=dbname,
        )
            == 1655
    )

@pytest.mark.skipif(not ok["ga20p"], reason="No GA 2020 Primary data")
def test_ga_totals_match_vote_type_20(dbname=None):
    assert e.check_totals_match_vote_types("2020 Primary", "Georgia") == True

### South Carolina Data Loading Tests ###
#SC20 test
@pytest.mark.skipif(not ok["sc20p"], reason="No SC Primary data")
def test_sc_presidential_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "South Carolina",
            "US President (SC) (Republican Party)",
            dbname=dbname,
        )
            == 469043
    )

@pytest.mark.skipif(not ok["sc20p"], reason="No SC Primary data")
def test_sc_statewide_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "South Carolina",
            "US President SC (Republican Party)",
            dbname=dbname,
        )
            == 469043
    )

@pytest.mark.skipif(not ok["sc20p"], reason="No SC Primary data")
def test_sc_senate_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "South Carolina",
            "SC Senate District 8 (Republican Party)",
            dbname=dbname,
        )
            == 13838
    )

@pytest.mark.skipif(not ok["sc20p"], reason="No SC Primary data")
def test_sc_house_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "South Carolina",
            "SC House District 75 (Democratic Party)",
            dbname=dbname,
        )
            == 3863
    )

@pytest.mark.skipif(not ok["sc20p"], reason="No SC Primary data")
def test_sc_contest_by_vote_type_20(dbname=None):
    assert (
            e.count_type_total(
            "2020 Primary",
            "South Carolina",
            "SC House District 75 (Democratic Party)",
            "absentee-mail",
            dbname=dbname,
        )
            == 1106
    )

@pytest.mark.skipif(not ok["sc20p"], reason="No SC Primary data")
def test_sc_totals_match_vote_type_20(dbname=None):
    assert e.check_totals_match_vote_types("2020 Primary", "North Carolina") == True


### Indiana Data Loading Tests ###
#IN16 test
@pytest.mark.skipif(not ok["in16g"], reason="No IN 2016 General data")
def test_in_presidential_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Indiana",
            "US President (IN)",
            dbname=dbname,
        )
            == 2728138
    )

@pytest.mark.skipif(not ok["in16g"], reason="No IN 2016 General data")
def test_in_statewide_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Indiana",
            "IN Attorney General",
            dbname=dbname,
        )
            == 2635832
    )

@pytest.mark.skipif(not ok["in16g"], reason="No IN 2016 General data")
def test_in_senate_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Indiana",
            "IN Senate District 7",
            dbname=dbname,
        )
            == 50622
    )

@pytest.mark.skipif(not ok["in16g"], reason="No IN 2016 General data")
def test_in_house_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Indiana",
            "IN House District 13",
            dbname=dbname,
        )
            == 26712
    )

@pytest.mark.skipif(not ok["in16g"], reason="No IN 2016 General data")
def test_in_contest_by_vote_type_16(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["in16g"], reason="No IN 2016 General data")
def test_in_totals_match_vote_type_16(dbname=None):
    # Vote type not available
    assert True == True

#IN18 test
@pytest.mark.skipif(not ok["in18g"], reason="No IN 2018 General data")
def test_in_presidential_18(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["in18g"], reason="No IN 2018 General data")
def test_in_statewide_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Indiana",
            "US Senate IN",
            dbname=dbname,
        )
            == 2282565
    )

@pytest.mark.skipif(not ok["in18g"], reason="No IN 2018 General data")
def test_in_senate_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Indiana",
            "IN Senate District 14",
            dbname=dbname,
        )
            == 34542
    )

@pytest.mark.skipif(not ok["in18g"], reason="No IN 2018 General data")
def test_in_house_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Indiana",
            "IN House District 27",
            dbname=dbname,
        )
            == 12238
    )

@pytest.mark.skipif(not ok["in18g"], reason="No IN 2018 General data")
def test_in_contest_by_vote_type_18(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["in18g"], reason="No IN 2018 General data")
def test_in_totals_match_vote_type_18(dbname=None):
    # Vote type not available
    assert True == True

#IN20 test
@pytest.mark.skipif(not ok["in20p"], reason="No IN 2020 Primary data")
def test_in_presidential_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Indiana",
            "US President (IN)",
            dbname=dbname,
        )
            == 1047173
    )

@pytest.mark.skipif(not ok["in20p"], reason="No IN 2020 Primary data")
def test_in_statewide_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Indiana",
            "IN Governor",
            dbname=dbname,
        )
            == 932726
    )

@pytest.mark.skipif(not ok["in20p"], reason="No IN 2020 Primary data")
def test_in_senate_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Indiana",
            "IN Senate District 50",
            dbname=dbname,
        )
            == 6860
    )

@pytest.mark.skipif(not ok["in20p"], reason="No IN 2020 Primary data")
def test_in_house_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Indiana",
            "IN House District 3",
            dbname=dbname,
        )
            == 7975
    )

@pytest.mark.skipif(not ok["in20p"], reason="No IN 2020 Primary data")
def test_in_contest_by_vote_type_20(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["in20p"], reason="No IN 2020 Primary data")
def test_in_totals_match_vote_type_20(dbname=None):
    # Vote type not available
    assert True == True


### Arkansas Data Loading Tests ###
#AR18 test
@pytest.mark.skipif(not ok["ar18g"], reason="No AR 2018 General data")
def test_ar_presidential_18(dbname=None):
    #no presidential contests in 2018
    assert True == True

@pytest.mark.skipif(not ok["ar18g"], reason="No AR 2018 General data")
def test_ar_statewide_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Arkansas",
            "AR Governor",
            dbname=dbname,
        )
            == 891509
    )

@pytest.mark.skipif(not ok["ar18g"], reason="No AR 2018 General data")
def test_ar_senate_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Arkansas",
            "AR Senate District 5",
            dbname=dbname,
        )
            == 27047
    )

@pytest.mark.skipif(not ok["ar18g"], reason="No AR 2018 General data")
def test_ar_house_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Arkansas",
            "AR House District 19",
            dbname=dbname,
        )
            == 7927
    )

@pytest.mark.skipif(not ok["ar18g"], reason="No AR 2018 General data")
def test_ar_contest_by_vote_type_18(dbname=None):
    assert (
            e.count_type_total(
            "2018 General",
            "Arkansas",
            "AR Senate District 5",
            "absentee",
            dbname=dbname,
        )
            == 453
    )

@pytest.mark.skipif(not ok["ar18g"], reason="No AR 2018 General data")
def test_ar_totals_match_vote_type_18(dbname=None):
    assert e.check_totals_match_vote_types("2018 General", "Arkansas") == True

#AR18 test
@pytest.mark.skipif(not ok["ar20p"], reason="No AR 2020 Primary data")
def test_ar_presidential_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Arkansas",
            "US President (AR) (Republican Party)",
            dbname=dbname,
        )
            == 246037
    )

@pytest.mark.skipif(not ok["ar20p"], reason="No AR 2020 Primary data")
def test_ar_statewide_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Arkansas",
            "AR Governor",
            dbname=dbname,
        )
            == 891509
    )

@pytest.mark.skipif(not ok["ar20p"], reason="No AR 2020 Primary data")
def test_ar_senate_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Arkansas",
            "AR Senate District 5",
            dbname=dbname,
        )
            == 27047
    )

@pytest.mark.skipif(not ok["ar20p"], reason="No AR 2020 Primary data")
def test_ar_house_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Arkansas",
            "AR House District 19",
            dbname=dbname,
        )
            == 7927
    )

@pytest.mark.skipif(not ok["ar20p"], reason="No AR 2020 Primary data")
def test_ar_contest_by_vote_type_20(dbname=None):
    assert (
            e.count_type_total(
            "2020 Primary",
            "Arkansas",
            "AR Senate District 5",
            "absentee",
            dbname=dbname,
        )
            == 453
    )

@pytest.mark.skipif(not ok["ar20p"], reason="No AR 2020 Primary data")
def test_ar_totals_match_vote_type_20(dbname=None):
    assert e.check_totals_match_vote_types("2020 Primary", "Arkansas") == True


### Michigan Data Loading Tests ###
#MI16 test
@pytest.mark.skipif(not ok["mi16g"], reason="No MI 2016 General data")
def test_mi_presidential_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Michigan",
            "US President (MI)",
            dbname=dbname,
        )
            == 4799284
    )

@pytest.mark.skipif(not ok["mi16g"], reason="No MI 2016 General data")
def test_mi_statewide_totals_16(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["mi16g"], reason="No MI 2016 General data")
def test_mi_us_rep_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Michigan",
            "US House MI District 4",
            dbname=dbname,
        )
            == 315751
    )
@pytest.mark.skipif(not ok["mi16g"], reason="No MI 2016 General data")
def test_mi_house_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Michigan",
            "MI House District 8",
            dbname=dbname,
        )
            == 34742
    )

@pytest.mark.skipif(not ok["mi16g"], reason="No MI 2016 General data")
def test_mi_contest_by_vote_type_16(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["mi16g"], reason="No MI 2016 General data")
def test_mi_totals_match_vote_type_16(dbname=None):
    # Vote type not available
    assert True == True

#MI18 test
@pytest.mark.skipif(not ok["mi18g"], reason="No MI 2018 General data")
def test_mi_presidential_18(dbname=None):
    #no presidential contests in 2018
    assert True == True

@pytest.mark.skipif(not ok["mi18g"], reason="No MI 2018 General data")
def test_mi_statewide_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Michigan",
            "MI Governor",
            dbname=dbname,
        )
            == 4250585
    )

@pytest.mark.skipif(not ok["mi18g"], reason="No MI 2018 General data")
def test_mi_senate_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Michigan",
            "MI Senate District 37",
            dbname=dbname,
        )
            == 124414
    )
@pytest.mark.skipif(not ok["mi18g"], reason="No MI 2018 General data")
def test_mi_house_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Michigan",
            "MI House District 8",
            dbname=dbname,
        )
            == 28017
    )

@pytest.mark.skipif(not ok["mi18g"], reason="No MI 2018 General data")
def test_mi_contest_by_vote_type_18(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["mi18g"], reason="No MI 2018 General data")
def test_mi_totals_match_vote_type_18(dbname=None):
    # Vote type not available
    assert True == True

#MI20 test
@pytest.mark.skipif(not ok["mi20p"], reason="No MI 2020 Primary data")
def test_mi_statewide_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Michigan",
            "US Senate MI (Democratic Party)",
            dbname=dbname,
        )
            == 1180780
    )

@pytest.mark.skipif(not ok["mi20p"], reason="No MI 2020 Primary data")
def test_mi_us_rep_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Michigan",
            "US House MI District 9 (Democratic Party)",
            dbname=dbname,
        )
            == 103202
    )

@pytest.mark.skipif(not ok["mi20p"], reason="No MI 2020 Primary data")
def test_mi_state_rep_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Michigan",
            "MI House District 37 (Republican Party)",
            dbname=dbname,
        )
            == 6669
    )

@pytest.mark.skipif(not ok["mi20p"], reason="No MI 2020 Primary data")
def test_mi_contest_by_vote_type_20(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["mi20p"], reason="No MI 2020 Primary data")
def test_mi_totals_match_vote_type_20(dbname=None):
    # Vote type not available
    assert True == True


@pytest.mark.skipif(not ok["mi20p"], reason="No MI 2020 Primary data")
def test_mi_presidential_20ppp(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Michigan",
            "US President (MI) (Democratic Party)",
            dbname=dbname,
        )
            == 4250585
    )


### Delaware 2020 Primary Data Loading Tests ###
@pytest.mark.skipif(not ok["de20p"], reason="No DE 2020 Primary data")
def test_de_statewide_totals(dbname=None):
    assert (
            e.contest_total(
                "2020 Primary",
                "Delaware",
                "DE Governor (Republican Party)",
            dbname=dbname,
        )
            == 55447
    )

@pytest.mark.skipif(not ok["de20p"], reason="No DE 2020 Primary data")
def test_de_senate_totals(dbname=None):
    assert (
            e.contest_total(
                "2020 Primary",
                "Delaware",
                "DE Senate District 13 (Democratic Party)",
            dbname=dbname,
        )
            == 5940
    )

@pytest.mark.skipif(not ok["de20p"], reason="No DE 2020 Primary data")
def test_de_house_totals(dbname=None):
    assert (
            e.contest_total(
                "2020 Primary",
                "Delaware",
                "DE House District 26 (Democratic Party)",
            dbname=dbname,
        )
            == 2990
    )

@pytest.mark.skipif(not ok["de20p"], reason="No DE 2020 Primary data")
def test_de_contest_by_vote_type(dbname=None):
    assert (
            e.count_type_total(
            "2020 Primary",
            "Delaware",
            "DE Senate District 14 (Republican Party)",
            "absentee",
            dbname=dbname,
        )
            == 559
    )

@pytest.mark.skipif(not ok["de20p"], reason="No DE 2020 Primary data")
def test_de_presidential(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Delaware",
            "US President (DE) (Democratic Party)",
            dbname=dbname,
        )
            == 91682
    )

@pytest.mark.skipif(not ok["de20p"], reason="No DE 2020 Primary data")
def test_de_totals_match_vote_type(dbname=None):
    assert e.check_totals_match_vote_types("2020 Primary", "Delaware") == True


### Ohio Data Loading Tests ###
## oh 2016g tests
@pytest.mark.skipif(not ok["oh16g"], reason="No OH 2016 General data")
def test_oh_presidential_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Ohio",
            "US President (OH)",
            dbname=dbname,
        )
            == 5496487
    )

@pytest.mark.skipif(not ok["oh16g"], reason="No OH 2016 General data")
def test_oh_statewide_16(dbname=None):
    # No tracked statewide contests other than president,
    assert True == True

@pytest.mark.skipif(not ok["oh16g"], reason="No OH 2016 General data")
def test_oh_senate_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Ohio",
            "OH Senate District 16",
            dbname=dbname,
        )
            == 185531
    )


@pytest.mark.skipif(not ok["oh16g"], reason="No OH 2016 General data")
def test_oh_house_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Ohio",
            "OH House District 2",
            dbname=dbname,
        )
            == 51931
    )


@pytest.mark.skipif(not ok["oh16g"], reason="No OH 2016 General data")
def test_oh_contest_by_vote_type_16(dbname=None):
    assert (
            e.count_type_total(
            "2016 General",
            "Ohio",
            "US House OH District 5",
            "total",
            dbname=dbname,
        )
            == 344991
    )


@pytest.mark.skipif(not ok["oh16g"], reason="No OH 2016 General data")
def test_oh_totals_match_vote_type_16(dbname=None):
    assert e.check_totals_match_vote_types("2016 General", "Ohio") == True

#OH18 test
@pytest.mark.skipif(not ok["oh18g"], reason="No OH 2018 General data")
def test_oh_presidential(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["oh18g"], reason="No OH 2018 General data")
def test_oh_statewide(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Ohio",
            "OH Governor",
            dbname=dbname,
        )
            == 5496487
    )

@pytest.mark.skipif(not ok["oh18g"], reason="No OH 2018 General data")
def test_oh_senate_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Ohio",
            "OH Senate District 21",
            dbname=dbname,
        )
            == 110903
    )


@pytest.mark.skipif(not ok["oh18g"], reason="No OH 2018 General data")
def test_oh_house_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Ohio",
            "OH House District 2",
            dbname=dbname,
        )
            == 44213
    )

@pytest.mark.skipif(not ok["oh18g"], reason="No OH 2018 General data")
def test_oh_contest_by_vote_type_18(dbname=None):
    # Vote type not available
    assert True == True

@pytest.mark.skipif(not ok["oh18g"], reason="No OH 2018 General data")
def test_oh_totals_match_vote_type_18(dbname=None):
    # Vote type not available
    assert True == True

### Illinois Data Loading Tests ###
@pytest.mark.skipif(not ok["il16g"], reason="No IL 2016 General data")
def test_il_presidential_16(dbname=None):
    assert(
            e.contest_total(
            "2016 General",
            "Illinois",
            "US President (IL)",
            dbname=dbname,
        )
            == 5536424
    )

@pytest.mark.skipif(not ok["il16g"], reason="No IL 2016 General data")
def test_il_statewide_totals_16(dbname=None):
    assert(
            e.contest_total(
            "2016 General",
            "Illinois",
            "IL Comptroller",
            dbname=dbname,
        )
            == 5412543
    )

@pytest.mark.skipif(not ok["il16g"], reason="No IL 2016 General data")
def test_il_senate_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Illinois",
            "US Senate IL",
            dbname=dbname,
        )
            == 5491878
    )

@pytest.mark.skipif(not ok["il16g"], reason="No IL 2016 General data")
def test_il_rep_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Illinois",
            "IL Senate District 14",
            dbname=dbname,
        )
            == 79949
    )

@pytest.mark.skipif(not ok["il16g"], reason="No IL 2016 General data")
def test_il_contest_by_vote_type_16(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["il16g"], reason="No IL 2016 General data")
def test_il_totals_match_vote_type_16(dbname=None):
    assert True == True

#IL18 test
@pytest.mark.skipif(not ok["il18g"], reason="No IL 2018 General data")
def test_il_presidential_18(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["il18g"], reason="No IL 2018 General data")
def test_il_statewide_totals_18(dbname=None):
    assert(
            e.contest_total(
            "2018 General",
            "Illinois",
            "IL Governor",
            dbname=dbname,
        )
            == 4547657
    )

@pytest.mark.skipif(not ok["il18g"], reason="No IL 2018 General data")
def test_il_senate_totals_18(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["il18g"], reason="No IL 2018 General data")
def test_il_rep_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Illinois",
            "IL House District 10",
            dbname=dbname,
        )
            == 31649
    )

@pytest.mark.skipif(not ok["il18g"], reason="No IL 2018 General data")
def test_il_contest_by_vote_type_18(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["il18g"], reason="No IL 2018 General data")
def test_il_totals_match_vote_type_18(dbname=None):
    assert True == True

#IL20 test
@pytest.mark.skipif(not ok["il20p"], reason="No IL 2020 Primary data")
def test_il_presidential_20(dbname=None):
    assert(
            e.contest_total(
            "2020 Primary",
            "Illinois",
            "US President (IL)",
            dbname=dbname,
        )
            == 2216933
    )

@pytest.mark.skipif(not ok["il20p"], reason="No IL 2020 Primary data")
def test_il_statewide_totals_20(dbname=None):
    assert(
            e.contest_total(
            "2020 Primary",
            "Illinois",
            "US Senate IL",
            dbname=dbname,
        )
            == 1941286
    )

@pytest.mark.skipif(not ok["il20p"], reason="No IL 2020 Primary data")
def test_il_state_senate_totals_20(dbname=None):
    assert(
            e.contest_total(
            "2020 Primary",
            "Illinois",
            "IL Senate District 11",
            dbname=dbname,
        )
            == 22716
    )

@pytest.mark.skipif(not ok["il20p"], reason="No IL 2020 Primary data")
def test_il_state_rep_totals_20(dbname=None):
    assert(
            e.contest_total(
            "2020 Primary",
            "Illinois",
            "IL House District 60",
            dbname=dbname,
        )
            == 8888
    )

@pytest.mark.skipif(not ok["il20p"], reason="No IL 2020 Primary data")
def test_il_contest_by_vote_type_20(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["il20p"], reason="No IL 2020 Primary data")
def test_il_totals_match_vote_type_20(dbname=None):
    assert True == True


### California Data Loading Tests ###
#CA16 test
@pytest.mark.skipif(not ok["ca16g"], reason="No ca 2016 General data")
def test_ca_presidential_16(dbname=None):
    assert(
            e.contest_total(
            "2016 General",
            "California",
            "US President (CA)",
            dbname=dbname,
        )
            == 14181595
    )

@pytest.mark.skipif(not ok["ca16g"], reason="No CA 2016 General data")
def test_ca_statewide_totals_16(dbname=None):
    assert(
            e.contest_total(
            "2016 General",
            "California",
            "US Senate CA",
            dbname=dbname,
        )
            == 12244170
    )

@pytest.mark.skipif(not ok["ca16g"], reason="No CA 2016 General data")
def test_ca_senate_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "California",
            "CA Senate District 15",
            dbname=dbname,
        )
            == 313531
    )

@pytest.mark.skipif(not ok["ca16g"], reason="No CA 2016 General data")
def test_ca_rep_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "California",
            "CA House District 60",
            dbname=dbname,
        )
            == 142114
    )

@pytest.mark.skipif(not ok["ca16g"], reason="No CA 2016 General data")
def test_ca_contest_by_vote_type_16(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["ca16g"], reason="No CA 2016 General data")
def test_ca_totals_match_vote_type_16(dbname=None):
    assert True == True


#CA18 test
@pytest.mark.skipif(not ok["ca18g"], reason="No ca 2018 General data")
def test_ca_presidential_18(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["ca18g"], reason="No CA 2018 General data")
def test_ca_statewide_totals_18(dbname=None):
    assert(
            e.contest_total(
            "2018 General",
            "California",
            "US Senate CA",
            dbname=dbname,
        )
            == 11113364
    )

@pytest.mark.skipif(not ok["ca18g"], reason="No CA 2018 General data")
def test_ca_senate_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "California",
            "CA Senate District 12",
            dbname=dbname,
        )
            == 203077
    )

@pytest.mark.skipif(not ok["ca18g"], reason="No CA 2018 General data")
def test_ca_rep_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "California",
            "CA House District 60",
            dbname=dbname,
        )
            == 125660
    )

@pytest.mark.skipif(not ok["ca18g"], reason="No CA 2018 General data")
def test_ca_contest_by_vote_type_18(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["ca18g"], reason="No CA 2018 General data")
def test_ca_totals_match_vote_type_18(dbname=None):
    assert True == True


#CA20 test
@pytest.mark.skipif(not ok["ca20p"], reason="No ca 2020 Primary data")
def test_ca_presidential_20(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["ca20p"], reason="No CA 2020 Primary data")
def test_ca_statewide_totals_20(dbname=None):
    assert(
            e.contest_total(
            "2020 Primary",
            "California",
            "US Senate CA",
            dbname=dbname,
        )
            == 11113364
    )

@pytest.mark.skipif(not ok["ca20p"], reason="No CA 2020 Primary data")
def test_ca_senate_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "California",
            "CA Senate District 12",
            dbname=dbname,
        )
            == 203077
    )

@pytest.mark.skipif(not ok["ca20p"], reason="No CA 2020 Primary data")
def test_ca_rep_18(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "California",
            "CA House District 60",
            dbname=dbname,
        )
            == 125660
    )

@pytest.mark.skipif(not ok["ca20p"], reason="No CA 2020 Primary data")
def test_ca_contest_by_vote_type_20(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["ca20p"], reason="No CA 2020 Primary data")
def test_ca_totals_match_vote_type_20(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["ca20p"], reason="No ca 2020 Primary data")
def test_ca_presidential_20ppp(dbname=None):
    assert(
            e.contest_total(
            "2020 President Preference Primary",
            "California",
            "US President (CA)",
            dbname=dbname,
        )
            == 2780247
    )


### Colorado Data Loading Tests ###
#CO16 test
@pytest.mark.skipif(not ok["co16g"], reason="No CO 2016 General data")
def test_co_presidential_16(dbname=None):
    assert(
            e.contest_total(
            "2016 General",
            "Colorado",
            "US President (CO)",
            dbname=dbname,
        )
            == 2780247
    )

@pytest.mark.skipif(not ok["co16g"], reason="No CO 2016 General data")
def test_co_statewide_totals_16(dbname=None):
    assert(
            e.contest_total(
            "2016 General",
            "Colorado",
            "US Senate CO",
            dbname=dbname,
        )
            == 2743029
    )

@pytest.mark.skipif(not ok["co16g"], reason="No CO 2016 General data")
def test_co_senate_totals_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Colorado",
            "CO Senate District 14",
            dbname=dbname,
        )
            == 85788
    )

@pytest.mark.skipif(not ok["co16g"], reason="No CO 2016 General data")
def test_co_rep_16(dbname=None):
    assert (
            e.contest_total(
            "2016 General",
            "Colorado",
            "CO House District 60",
            dbname=dbname,
        )
            == 41303
    )

@pytest.mark.skipif(not ok["co16g"], reason="No CO 2016 General data")
def test_co_contest_by_vote_type_16(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["co16g"], reason="No CO 2016 General data")
def test_co_totals_match_vote_type_16(dbname=None):
    assert True == True

#CO18 test
@pytest.mark.skipif(not ok["co18g"], reason="No CO 2018 General data")
def test_co_presidential_18(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["co18g"], reason="No CO 2018 General data")
def test_co_statewide_totals_18(dbname=None):
    assert(
            e.contest_total(
            "2018 General",
            "Colorado",
            "CO Attorney General",
            dbname=dbname,
        )
            == 2491954
    )

@pytest.mark.skipif(not ok["co18g"], reason="No CO 2018 General data")
def test_co_senate_totals_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Colorado",
            "CO Senate District 15",
            dbname=dbname,
        )
            == 83690
    )

@pytest.mark.skipif(not ok["co18g"], reason="No CO 2018 General data")
def test_co_rep_18(dbname=None):
    assert (
            e.contest_total(
            "2018 General",
            "Colorado",
            "CO House District 60",
            dbname=dbname,
        )
            == 39237
    )

@pytest.mark.skipif(not ok["co18g"], reason="No CO 2018 General data")
def test_co_contest_by_vote_type_18(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["co18g"], reason="No CO 2018 General data")
def test_co_totals_match_vote_type_18(dbname=None):
    assert True == True

#CO20 test
@pytest.mark.skipif(not ok["co20p"], reason="No CO 2020 Primary data")
def test_co_presidential_20(dbname=None):
    assert(
            e.contest_total(
            "2020 Primary",
            "Colorado",
            "US President (CO) (Democratic Party)",
            dbname=dbname,
        )
            == 960128
    )

@pytest.mark.skipif(not ok["co20p"], reason="No CO 2020 Primary data")
def test_co_statewide_totals_20(dbname=None):
    assert(
            e.contest_total(
            "2020 Primary",
            "Colorado",
            "US Senate CO (Republican Party)",
            dbname=dbname,
        )
            == 554806
    )

@pytest.mark.skipif(not ok["co20p"], reason="No CO 2020 Primary data")
def test_co_senate_totals_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Colorado",
            "CO Senate District 21 (Republican Party)",
            dbname=dbname,
        )
            == 6320
    )

@pytest.mark.skipif(not ok["co20p"], reason="No CO 2020 Primary data")
def test_co_rep_20(dbname=None):
    assert (
            e.contest_total(
            "2020 Primary",
            "Colorado",
            "CO House District 20 (Democratic Party)",
            dbname=dbname,
        )
            == 10011
    )

@pytest.mark.skipif(not ok["co20p"], reason="No CO 2020 Primary data")
def test_co_contest_by_vote_type_18(dbname=None):
    assert True == True

@pytest.mark.skipif(not ok["co20p"], reason="No CO 2020 Primary data")
def test_co_totals_match_vote_type_18(dbname=None):
    assert True == True
