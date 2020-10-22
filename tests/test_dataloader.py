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


#constants


### NC dataloading tests ###
#NC16 tests

def test_nc_presidential_16(dbname):
    assert(not e.data_exists("2016 General", "North Carolina", dbname=dbname) or e.contest_total(
            "2016 General",
            "North Carolina",
            "US President (NC)",
            dbname=dbname,
        )
            == 4741564
    )



def test_nc_statewide_totals_16(dbname):
    assert (not e.data_exists("2016 General","North Carolina",dbname=dbname) or e.contest_total(
            "2016 General",
            "North Carolina",
            "NC Treasurer",
            dbname=dbname,
        )
            == 4502784
    )


def test_nc_senate_totals_16(dbname):
    assert (not e.data_exists("2016 General","North Carolina",dbname=dbname) or e.contest_total(
            "2016 General",
            "North Carolina",
            "US Senate NC",
            dbname=dbname,
        )
            == 4691133
    )


def test_nc_rep_16(dbname):
    assert (not e.data_exists("2016 General","North Carolina",dbname=dbname) or e.contest_total(
            "2016 General",
            "North Carolina",
            "US House NC District 4",
            dbname=dbname,
        )
            == 409541
    )


def test_nc_contest_by_vote_type_16(dbname):
    assert ( not e.data_exists("2016 General","North Carolina", dbname=dbname) or
            e.count_type_total(
            "2016 General",
            "North Carolina",
"US House NC District 4",
            "absentee-mail",
            dbname=dbname,
        )
            == 20881
    )


def test_nc_totals_match_vote_type_16(dbname):
    assert (not e.data_exists("2016 General","North Carolina", dbname=dbname) or 
            e.check_totals_match_vote_types("2016 General","North Carolina" ,dbname=dbname) == True)


#NC18 tests

#NC20 Tests

def test_nc_presidential_20(dbname):
    assert (not e.data_exists("2020 Primary","North Carolina",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "North Carolina",
            "US President (NC) (Democratic Party)",
            dbname=dbname,
        )
            == 1331366
    )


def test_nc_statewide_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","North Carolina",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "North Carolina",
            "NC Governor (Democratic Party)",
            dbname=dbname,
        )
            == 1293652
    )


def test_nc_senate_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","North Carolina",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "North Carolina",
            "US Senate NC (Democratic Party)",
            dbname=dbname,
        )
            == 1260090
    )


def test_nc_rep_20_20(dbname):
    assert (not e.data_exists("2020 Primary","North Carolina",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "North Carolina",
            "US House NC District 4 (Republican Party)",
            dbname=dbname,
        )
            == 36096
    )


def test_nc_contest_by_vote_type_20(dbname):
    assert ( not e.data_exists("2020 Primary","North Carolina", dbname=dbname) or
            e.count_type_total(
            "2016 General",
            "North Carolina",
"US House NC District 4 (Republican Party)",
            "absentee-mail",
            dbname=dbname,
        )
            == 426
    )


def test_nc_totals_match_vote_type_20(dbname):
    assert (not e.data_exists("2020 General","North Carolina", dbname=dbname) or 
            e.check_totals_match_vote_types("2020 General","North Carolina" ,dbname=dbname) == True)



### Florida Data Loading Tests ###
#FL16 test

def test_fl_presidential(dbname):
    assert (not e.data_exists("2016 General","Florida",dbname=dbname) or e.contest_total(
            "2016 General",
            "Florida",
            "US President (FL)",
            dbname=dbname,
        )
            == 9420039
    )


def test_fl_statewide_totals(dbname):
    assert (not e.data_exists("2016 General","Florida",dbname=dbname) or e.contest_total(
            "2016 General",
            "Florida",
            "US Senate FL",
            dbname=dbname,
        )
            == 9301820
    )


def test_fl_senate_totals(dbname):
    assert (not e.data_exists("2016 General","Florida",dbname=dbname) or e.contest_total(
            "2016 General",
            "Florida",
            "FL Senate District 3",
            dbname=dbname,
        )
            == 236480
    )


def test_fl_house_totals(dbname):
    assert (not e.data_exists("2016 General","Florida",dbname=dbname) or e.contest_total(
            "2016 General",
            "Florida",
            "US House FL District 10",
            dbname=dbname,
        )
            == 305989
    )


def test_fl_contest_by_vote_type(dbname):
    # Vote type not available
    assert True == True


def test_fl_totals_match_vote_type(dbname):
    # Vote type not available
    assert True == True

#FL18 test

def test_fl_presidential_18(dbname):
    assert True == True


def test_fl_statewide_totals_18(dbname):
    assert (not e.data_exists("2018 General","Florida",dbname=dbname) or e.contest_total(
            "2018 General",
            "Florida",
            "US Senate FL",
            dbname=dbname,
        )
            == 8190005
    )


def test_fl_senate_totals_18(dbname):
    assert (not e.data_exists("2018 General","Florida",dbname=dbname) or e.contest_total(
            "2018 General",
            "Florida",
            "FL Senate District 4",
            dbname=dbname,
        )
            == 235459
    )


def test_fl_house_totals_18(dbname):
    assert (not e.data_exists("2018 General","Florida",dbname=dbname) or e.contest_total(
            "2018 General",
            "Florida",
            "FL House District 11",
            dbname=dbname,
        )
            == 85479
    )


def test_fl_contest_by_vote_type_18(dbname):
    # Vote type not available
    assert True == True


def test_fl_totals_match_vote_type_18(dbname):
    # Vote type not available
    assert True == True

#FL20 test

def test_fl_presidential_20(dbname):
    assert (not e.data_exists("2020 Primary","Florida",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Florida",
            "US President (FL)",
            dbname=dbname,
        )
            == 5958306
    )


def test_fl_statewide_totals_20(dbname):
    assert True == True


def test_fl_senate_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Florida",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Florida",
            "FL Senate District 21",
            dbname=dbname,
        )
            == 54988
    )


def test_fl_house_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Florida",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Florida",
            "FL House District 11",
            dbname=dbname,
        )
            == 17445
    )


def test_fl_contest_by_vote_type_20(dbname):
    # Vote type not available
    assert True == True


def test_fl_totals_match_vote_type_20(dbname):
    # Vote type not available
    assert True == True

### Pennsylvania Data Loading Tests ###

#PA18 test

def test_pa_presidential_18(dbname):
    assert True == True


def test_pa_statewide_totals_18(dbname):
    assert (not e.data_exists("2018 General","Pennsylvania",dbname=dbname) or e.contest_total(
            "2018 General",
            "Pennsylvania",
            "PA Governor",
            dbname=dbname,
        )
            == 5012555
    )


def test_pa_senate_totals_18(dbname):
    assert (not e.data_exists("2018 General","Pennsylvania",dbname=dbname) or e.contest_total(
            "2018 General",
            "Pennsylvania",
            "PA Senate District 20",
            dbname=dbname,
        )
            == 81817
    )


def test_pa_house_totals_18(dbname):
    assert (not e.data_exists("2018 General","Pennsylvania",dbname=dbname) or e.contest_total(
            "2018 General",
            "Pennsylvania",
            "PA House District 103",
            dbname=dbname,
        )
            == 18363
    )


def test_pa_contest_by_vote_type_18(dbname):
    # Vote type not available
    assert True == True


def test_pa_totals_match_vote_type_18(dbname):
    # Vote type not available
    assert True == True

#PA20

def test_pa_presidential_20(dbname):
    assert (not e.data_exists("2020 General","Pennsylvania",dbname=dbname) or e.contest_total(
            "2020 General",
            "Pennsylvania",
            "US President (PA)",
            dbname=dbname,
        )
            == 2739007
    )


def test_pa_statewide_totals_20(dbname):
    assert (not e.data_exists("2020 General","Pennsylvania",dbname=dbname) or e.contest_total(
            "2020 General",
            "Pennsylvania",
            "PA Governor",
            dbname=dbname,
        )
            == 2484582
    )


def test_pa_senate_totals_20(dbname):
    assert (not e.data_exists("2020 General","Pennsylvania",dbname=dbname) or e.contest_total(
            "2020 General",
            "Pennsylvania",
            "PA Senate District 20",
            dbname=dbname,
        )
            == 67898
    )


def test_pa_house_totals_20(dbname):
    assert (not e.data_exists("2020 General","Pennsylvania",dbname=dbname) or e.contest_total(
            "2020 General",
            "Pennsylvania",
            "PA House District 100",
            dbname=dbname,
        )
            == 6327
    )


def test_pa_contest_by_vote_type_20(dbname):
    # Vote type not available
    assert True == True


def test_pa_totals_match_vote_type_20(dbname):
    # Vote type not available
    assert True == True


### Georgia Data Loading Tests ###
#GA16

def test_ga_presidential_16(dbname):
    assert (not e.data_exists("2016 General","Georgia",dbname=dbname) or e.contest_total(
            "2016 General",
            "Georgia",
            "US President (GA)",
            dbname=dbname,
        )
            == 4092373
    )


def test_ga_statewide_totals_16(dbname):
    assert (not e.data_exists("2016 General","Georgia",dbname=dbname) or e.contest_total(
            "2016 General",
            "Georgia",
            "US Senate GA",
            dbname=dbname,
        )
            == 3897792
    )


def test_ga_senate_totals_16(dbname):
    assert (not e.data_exists("2016 General","Georgia",dbname=dbname) or e.contest_total(
            "2016 General",
            "Georgia",
            "GA Senate District 13",
            dbname=dbname,
        )
            == 60387
    )


def test_ga_house_totals_16(dbname):
    assert (not e.data_exists("2016 General","Georgia",dbname=dbname) or e.contest_total(
            "2016 General",
            "Georgia",
            "GA House District 7",
            dbname=dbname,
        )
            == 21666
    )


def test_ga_contest_by_vote_type_16(dbname):
    assert ( not e.data_exists("2016 General","Georgia", dbname=dbname) or
            e.count_type_total(
            "2016 General",
            "North Carolina",
"GA House District 7",
            "absentee-mail",
            dbname=dbname,
        )
            == 1244
    )


def test_ga_totals_match_vote_type_16(dbname):
    assert (not e.data_exists("2016 General","Georgia", dbname=dbname) or 
            e.check_totals_match_vote_types("2016 General","Georgia" ,dbname=dbname) == True)

#GA18

def test_ga_presidential_18(dbname):
    #no presidential contests in 2018
    assert True == True


def test_ga_statewide_totals_18(dbname):
    assert (not e.data_exists("2018 General","Georgia",dbname=dbname) or e.contest_total(
            "2018 General",
            "Georgia",
            "GA Governor",
            dbname=dbname,
        )
            == 3939328
    )


def test_ga_senate_totals_18(dbname):
    assert (not e.data_exists("2018 General","Georgia",dbname=dbname) or e.contest_total(
            "2018 General",
            "Georgia",
            "GA Senate District 5",
            dbname=dbname,
        )
            == 34429
    )


def test_ga_house_totals_18(dbname):
    assert (not e.data_exists("2018 General","Georgia",dbname=dbname) or e.contest_total(
            "2018 General",
            "Georgia",
            "US House GA District 2",
            dbname=dbname,
        )
            == 229171
    )


def test_ga_contest_by_vote_type_18(dbname):
    assert ( not e.data_exists("2018 General","Georgia", dbname=dbname) or
            e.count_type_total(
            "2016 General",
            "North Carolina",
"GA Senate District 5",
            "absentee-mail",
            dbname=dbname,
        )
            == 2335
    )


def test_ga_totals_match_vote_type_18(dbname):
    assert (not e.data_exists("2018 General","Georgia", dbname=dbname) or 
            e.check_totals_match_vote_types("2018 General","Georgia" ,dbname=dbname) == True)

#GA16

def test_ga_presidential_20(dbname):
    assert (not e.data_exists("2020 Primary","Georgia",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Georgia",
            "US President (GA) (Republican Party)",
            dbname=dbname,
        )
            == 947352
    )


def test_ga_statewide_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Georgia",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Georgia",
            "US Senate GA (Republican Party)",
            dbname=dbname,
        )
            == 992555
    )


def test_ga_senate_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Georgia",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Georgia",
            "GA Senate District 8 (Democratic Party)",
            dbname=dbname,
        )
            == 9103
    )


def test_ga_house_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Georgia",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Georgia",
            "GA House District 7 (Democratic Party)",
            dbname=dbname,
        )
            == 2193
    )


def test_ga_contest_by_vote_type_20(dbname):
    assert ( not e.data_exists("2020 Primary","Georgia", dbname=dbname) or
            e.count_type_total(
            "2016 General",
            "North Carolina",
"GA House District 7 (Democratic Party)",
            "absentee-mail",
            dbname=dbname,
        )
            == 1655
    )


def test_ga_totals_match_vote_type_20(dbname):
    assert (not e.data_exists("2020 Primary","Georgia", dbname=dbname) or 
            e.check_totals_match_vote_types("2020 Primary","Georgia" ,dbname=dbname) == True)

### South Carolina Data Loading Tests ###
#SC20 test

def test_sc_presidential_20(dbname):
    assert (not e.data_exists("2020 Primary","South Carolina",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "South Carolina",
            "US President (SC) (Republican Party)",
            dbname=dbname,
        )
            == 469043
    )


def test_sc_statewide_20(dbname):
    assert (not e.data_exists("2020 Primary","South Carolina",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "South Carolina",
            "US President SC (Republican Party)",
            dbname=dbname,
        )
            == 469043
    )


def test_sc_senate_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","South Carolina",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "South Carolina",
            "SC Senate District 8 (Republican Party)",
            dbname=dbname,
        )
            == 13838
    )


def test_sc_house_totals_20(dbname):
    assert (not e.data_exists("2016 General","South Carolina",dbname=dbname) or e.contest_total(
            "2016 General",
            "South Carolina",
            "SC House District 75 (Democratic Party)",
            dbname=dbname,
        )
            == 3863
    )


def test_sc_contest_by_vote_type_20(dbname):
    assert ( not e.data_exists("2020 Primary","South Carolina", dbname=dbname) or
            e.count_type_total(
            "2016 General",
            "North Carolina",
"SC House District 75 (Democratic Party)",
            "absentee-mail",
            dbname=dbname,
        )
            == 1106
    )


def test_sc_totals_match_vote_type_20(dbname):
    assert (not e.data_exists("2020 Primary","North Carolina", dbname=dbname) or 
            e.check_totals_match_vote_types("2020 Primary","North Carolina" ,dbname=dbname) == True)


### Indiana Data Loading Tests ###
#IN16 test

def test_in_presidential_16(dbname):
    assert (not e.data_exists("2016 General","Indiana",dbname=dbname) or e.contest_total(
            "2016 General",
            "Indiana",
            "US President (IN)",
            dbname=dbname,
        )
            == 2728138
    )


def test_in_statewide_totals_16(dbname):
    assert (not e.data_exists("2016 General","Indiana",dbname=dbname) or e.contest_total(
            "2016 General",
            "Indiana",
            "IN Attorney General",
            dbname=dbname,
        )
            == 2635832
    )


def test_in_senate_totals_16(dbname):
    assert (not e.data_exists("2016 General","Indiana",dbname=dbname) or e.contest_total(
            "2016 General",
            "Indiana",
            "IN Senate District 7",
            dbname=dbname,
        )
            == 50622
    )


def test_in_house_totals_16(dbname):
    assert (not e.data_exists("2016 General","Indiana",dbname=dbname) or e.contest_total(
            "2016 General",
            "Indiana",
            "IN House District 13",
            dbname=dbname,
        )
            == 26712
    )


def test_in_contest_by_vote_type_16(dbname):
    # Vote type not available
    assert True == True


def test_in_totals_match_vote_type_16(dbname):
    # Vote type not available
    assert True == True

#IN18 test

def test_in_presidential_18(dbname):
    assert True == True


def test_in_statewide_totals_18(dbname):
    assert (not e.data_exists("2018 General","Indiana",dbname=dbname) or e.contest_total(
            "2018 General",
            "Indiana",
            "US Senate IN",
            dbname=dbname,
        )
            == 2282565
    )


def test_in_senate_totals_18(dbname):
    assert (not e.data_exists("2018 General","Indiana",dbname=dbname) or e.contest_total(
            "2018 General",
            "Indiana",
            "IN Senate District 14",
            dbname=dbname,
        )
            == 34542
    )


def test_in_house_totals_18(dbname):
    assert (not e.data_exists("2018 General","Indiana",dbname=dbname) or e.contest_total(
            "2018 General",
            "Indiana",
            "IN House District 27",
            dbname=dbname,
        )
            == 12238
    )


def test_in_contest_by_vote_type_18(dbname):
    # Vote type not available
    assert True == True


def test_in_totals_match_vote_type_18(dbname):
    # Vote type not available
    assert True == True

#IN20 test

def test_in_presidential_20(dbname):
    assert (not e.data_exists("2020 Primary","Indiana",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Indiana",
            "US President (IN)",
            dbname=dbname,
        )
            == 1047173
    )


def test_in_statewide_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Indiana",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Indiana",
            "IN Governor",
            dbname=dbname,
        )
            == 932726
    )


def test_in_senate_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Indiana",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Indiana",
            "IN Senate District 50",
            dbname=dbname,
        )
            == 6860
    )


def test_in_house_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Indiana",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Indiana",
            "IN House District 3",
            dbname=dbname,
        )
            == 7975
    )


def test_in_contest_by_vote_type_20(dbname):
    # Vote type not available
    assert True == True


def test_in_totals_match_vote_type_20(dbname):
    # Vote type not available
    assert True == True


### Arkansas Data Loading Tests ###
#AR18 test

def test_ar_presidential_18(dbname):
    #no presidential contests in 2018
    assert True == True


def test_ar_statewide_totals_18(dbname):
    assert (not e.data_exists("2018 General","Arkansas",dbname=dbname) or e.contest_total(
            "2018 General",
            "Arkansas",
            "AR Governor",
            dbname=dbname,
        )
            == 891509
    )


def test_ar_senate_totals_18(dbname):
    assert (not e.data_exists("2018 General","Arkansas",dbname=dbname) or e.contest_total(
            "2018 General",
            "Arkansas",
            "AR Senate District 5",
            dbname=dbname,
        )
            == 27047
    )


def test_ar_house_totals_18(dbname):
    assert (not e.data_exists("2018 General","Arkansas",dbname=dbname) or e.contest_total(
            "2018 General",
            "Arkansas",
            "AR House District 19",
            dbname=dbname,
        )
            == 7927
    )


def test_ar_contest_by_vote_type_18(dbname):
    assert ( not e.data_exists("2018 General","Arkansas", dbname=dbname) or
            e.count_type_total(
            "2016 General",
            "North Carolina",
"AR Senate District 5",
            "absentee",
            dbname=dbname,
        )
            == 453
    )


def test_ar_totals_match_vote_type_18(dbname):
    assert (not e.data_exists("2018 General","Arkansas", dbname=dbname) or 
            e.check_totals_match_vote_types("2018 General","Arkansas" ,dbname=dbname) == True)

#AR18 test

def test_ar_presidential_20(dbname):
    assert (not e.data_exists("2020 Primary","Arkansas",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Arkansas",
            "US President (AR) (Republican Party)",
            dbname=dbname,
        )
            == 246037
    )


def test_ar_statewide_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Arkansas",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Arkansas",
            "AR Governor",
            dbname=dbname,
        )
            == 891509
    )


def test_ar_senate_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Arkansas",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Arkansas",
            "AR Senate District 5",
            dbname=dbname,
        )
            == 27047
    )


def test_ar_house_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Arkansas",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Arkansas",
            "AR House District 19",
            dbname=dbname,
        )
            == 7927
    )


def test_ar_contest_by_vote_type_20(dbname):
    assert ( not e.data_exists("2020 Primary","Arkansas", dbname=dbname) or
            e.count_type_total(
            "2016 General",
            "North Carolina",
"AR Senate District 5",
            "absentee",
            dbname=dbname,
        )
            == 453
    )


def test_ar_totals_match_vote_type_20(dbname):
    assert (not e.data_exists("2020 Primary","Arkansas", dbname=dbname) or 
            e.check_totals_match_vote_types("2020 Primary","Arkansas" ,dbname=dbname) == True)


### Michigan Data Loading Tests ###
#MI16 test

def test_mi_presidential_16(dbname):
    assert (not e.data_exists("2016 General","Michigan",dbname=dbname) or e.contest_total(
            "2016 General",
            "Michigan",
            "US President (MI)",
            dbname=dbname,
        )
            == 4799284
    )


def test_mi_statewide_totals_16(dbname):
    assert True == True


def test_mi_us_rep_totals_16(dbname):
    assert (not e.data_exists("2016 General","Michigan",dbname=dbname) or e.contest_total(
            "2016 General",
            "Michigan",
            "US House MI District 4",
            dbname=dbname,
        )
            == 315751
    )

def test_mi_house_totals_16(dbname):
    assert (not e.data_exists("2016 General","Michigan",dbname=dbname) or e.contest_total(
            "2016 General",
            "Michigan",
            "MI House District 8",
            dbname=dbname,
        )
            == 34742
    )


def test_mi_contest_by_vote_type_16(dbname):
    # Vote type not available
    assert True == True


def test_mi_totals_match_vote_type_16(dbname):
    # Vote type not available
    assert True == True

#MI18 test

def test_mi_presidential_18(dbname):
    #no presidential contests in 2018
    assert True == True


def test_mi_statewide_totals_18(dbname):
    assert (not e.data_exists("2018 General","Michigan",dbname=dbname) or e.contest_total(
            "2018 General",
            "Michigan",
            "MI Governor",
            dbname=dbname,
        )
            == 4250585
    )


def test_mi_senate_totals_18(dbname):
    assert (not e.data_exists("2018 General","Michigan",dbname=dbname) or e.contest_total(
            "2018 General",
            "Michigan",
            "MI Senate District 37",
            dbname=dbname,
        )
            == 124414
    )

def test_mi_house_totals_18(dbname):
    assert (not e.data_exists("2018 General","Michigan",dbname=dbname) or e.contest_total(
            "2018 General",
            "Michigan",
            "MI House District 8",
            dbname=dbname,
        )
            == 28017
    )


def test_mi_contest_by_vote_type_18(dbname):
    # Vote type not available
    assert True == True


def test_mi_totals_match_vote_type_18(dbname):
    # Vote type not available
    assert True == True

#MI20 test

def test_mi_statewide_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Michigan",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Michigan",
            "US Senate MI (Democratic Party)",
            dbname=dbname,
        )
            == 1180780
    )


def test_mi_us_rep_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Michigan",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Michigan",
            "US House MI District 9 (Democratic Party)",
            dbname=dbname,
        )
            == 103202
    )


def test_mi_state_rep_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Michigan",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Michigan",
            "MI House District 37 (Republican Party)",
            dbname=dbname,
        )
            == 6669
    )


def test_mi_contest_by_vote_type_20(dbname):
    # Vote type not available
    assert True == True


def test_mi_totals_match_vote_type_20(dbname):
    # Vote type not available
    assert True == True



def test_mi_presidential_20ppp(dbname):
    assert (not e.data_exists("2020 Primary","Michigan",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Michigan",
            "US President (MI) (Democratic Party)",
            dbname=dbname,
        )
            == 4250585
    )


### Delaware 2020 Primary Data Loading Tests ###

def test_de_statewide_totals(dbname):
    assert (not e.data_exists("2020 Primary","Delaware",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Delaware",
                "DE Governor (Republican Party)",
            dbname=dbname,
        )
            == 55447
    )


def test_de_senate_totals(dbname):
    assert (not e.data_exists("2020 Primary","Delaware",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Delaware",
                "DE Senate District 13 (Democratic Party)",
            dbname=dbname,
        )
            == 5940
    )


def test_de_house_totals(dbname):
    assert (not e.data_exists("2020 Primary","Delaware",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Delaware",
                "DE House District 26 (Democratic Party)",
            dbname=dbname,
        )
            == 2990
    )


def test_de_contest_by_vote_type(dbname):
    assert ( not e.data_exists("2020 Primary","Delaware", dbname=dbname) or
            e.count_type_total(
            "2016 General",
            "North Carolina",
"DE Senate District 14 (Republican Party)",
            "absentee",
            dbname=dbname,
        )
            == 559
    )


def test_de_presidential(dbname):
    assert (not e.data_exists("2020 Primary","Delaware",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Delaware",
            "US President (DE) (Democratic Party)",
            dbname=dbname,
        )
            == 91682
    )


def test_de_totals_match_vote_type(dbname):
    assert (not e.data_exists("2020 Primary","Delaware", dbname=dbname) or 
            e.check_totals_match_vote_types("2020 Primary","Delaware" ,dbname=dbname) == True)


### Ohio Data Loading Tests ###
## oh 2016g tests

def test_oh_presidential_16(dbname):
    assert (not e.data_exists("2016 General","Ohio",dbname=dbname) or e.contest_total(
            "2016 General",
            "Ohio",
            "US President (OH)",
            dbname=dbname,
        )
            == 5496487
    )


def test_oh_statewide_16(dbname):
    # No tracked statewide contests other than president,
    assert True == True


def test_oh_senate_totals_16(dbname):
    assert (not e.data_exists("2016 General","Ohio",dbname=dbname) or e.contest_total(
            "2016 General",
            "Ohio",
            "OH Senate District 16",
            dbname=dbname,
        )
            == 185531
    )



def test_oh_house_totals_16(dbname):
    assert (not e.data_exists("2016 General","Ohio",dbname=dbname) or e.contest_total(
            "2016 General",
            "Ohio",
            "OH House District 2",
            dbname=dbname,
        )
            == 51931
    )



def test_oh_contest_by_vote_type_16(dbname):
    assert ( not e.data_exists("2016 General","Ohio", dbname=dbname) or
            e.count_type_total(
            "2016 General",
            "North Carolina",
"US House OH District 5",
            "total",
            dbname=dbname,
        )
            == 344991
    )



def test_oh_totals_match_vote_type_16(dbname):
    assert (not e.data_exists("2016 General","Ohio", dbname=dbname) or 
            e.check_totals_match_vote_types("2016 General","Ohio" ,dbname=dbname) == True)

#OH18 test

def test_oh_presidential(dbname):
    assert True == True


def test_oh_statewide(dbname):
    assert (not e.data_exists("2018 General","Ohio",dbname=dbname) or e.contest_total(
            "2018 General",
            "Ohio",
            "OH Governor",
            dbname=dbname,
        )
            == 5496487
    )


def test_oh_senate_totals_18(dbname):
    assert (not e.data_exists("2018 General","Ohio",dbname=dbname) or e.contest_total(
            "2018 General",
            "Ohio",
            "OH Senate District 21",
            dbname=dbname,
        )
            == 110903
    )



def test_oh_house_totals_18(dbname):
    assert (not e.data_exists("2018 General","Ohio",dbname=dbname) or e.contest_total(
            "2018 General",
            "Ohio",
            "OH House District 2",
            dbname=dbname,
        )
            == 44213
    )


def test_oh_contest_by_vote_type_18(dbname):
    # Vote type not available
    assert True == True


def test_oh_totals_match_vote_type_18(dbname):
    # Vote type not available
    assert True == True

### Illinois Data Loading Tests ###

def test_il_presidential_16(dbname):
    assert (not e.data_exists("2016 General","Illinois",dbname=dbname) or e.contest_total(
            "2016 General",
            "Illinois",
            "US President (IL)",
            dbname=dbname,
        )
            == 5536424
    )


def test_il_statewide_totals_16(dbname):
    assert (not e.data_exists("2016 General","Illinois",dbname=dbname) or e.contest_total(
            "2016 General",
            "Illinois",
            "IL Comptroller",
            dbname=dbname,
        )
            == 5412543
    )


def test_il_senate_totals_16(dbname):
    assert (not e.data_exists("2016 General","Illinois",dbname=dbname) or e.contest_total(
            "2016 General",
            "Illinois",
            "US Senate IL",
            dbname=dbname,
        )
            == 5491878
    )


def test_il_rep_16(dbname):
    assert (not e.data_exists("2016 General","Illinois",dbname=dbname) or e.contest_total(
            "2016 General",
            "Illinois",
            "IL Senate District 14",
            dbname=dbname,
        )
            == 79949
    )


def test_il_contest_by_vote_type_16(dbname):
    assert True == True


def test_il_totals_match_vote_type_16(dbname):
    assert True == True

#IL18 test

def test_il_presidential_18(dbname):
    assert True == True


def test_il_statewide_totals_18(dbname):
    assert (not e.data_exists("2018 General","Illinois",dbname=dbname) or e.contest_total(
            "2018 General",
            "Illinois",
            "IL Governor",
            dbname=dbname,
        )
            == 4547657
    )


def test_il_senate_totals_18(dbname):
    assert True == True


def test_il_rep_18(dbname):
    assert (not e.data_exists("2018 General","Illinois",dbname=dbname) or e.contest_total(
            "2018 General",
            "Illinois",
            "IL House District 10",
            dbname=dbname,
        )
            == 31649
    )


def test_il_contest_by_vote_type_18(dbname):
    assert True == True


def test_il_totals_match_vote_type_18(dbname):
    assert True == True

#IL20 test

def test_il_presidential_20(dbname):
    assert (not e.data_exists("2020 Primary","Illinois",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Illinois",
            "US President (IL)",
            dbname=dbname,
        )
            == 2216933
    )


def test_il_statewide_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Illinois",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Illinois",
            "US Senate IL",
            dbname=dbname,
        )
            == 1941286
    )


def test_il_state_senate_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Illinois",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Illinois",
            "IL Senate District 11",
            dbname=dbname,
        )
            == 22716
    )


def test_il_state_rep_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Illinois",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Illinois",
            "IL House District 60",
            dbname=dbname,
        )
            == 8888
    )


def test_il_contest_by_vote_type_20(dbname):
    assert True == True


def test_il_totals_match_vote_type_20(dbname):
    assert True == True


### California Data Loading Tests ###
#CA16 test

def test_ca_presidential_16(dbname):
    assert (not e.data_exists("2016 General","California",dbname=dbname) or e.contest_total(
            "2016 General",
            "California",
            "US President (CA)",
            dbname=dbname,
        )
            == 14181595
    )


def test_ca_statewide_totals_16(dbname):
    assert (not e.data_exists("2016 General","California",dbname=dbname) or e.contest_total(
            "2016 General",
            "California",
            "US Senate CA",
            dbname=dbname,
        )
            == 12244170
    )


def test_ca_senate_totals_16(dbname):
    assert (not e.data_exists("2016 General","California",dbname=dbname) or e.contest_total(
            "2016 General",
            "California",
            "CA Senate District 15",
            dbname=dbname,
        )
            == 313531
    )


def test_ca_rep_16(dbname):
    assert (not e.data_exists("2016 General","California",dbname=dbname) or e.contest_total(
            "2016 General",
            "California",
            "CA House District 60",
            dbname=dbname,
        )
            == 142114
    )


def test_ca_contest_by_vote_type_16(dbname):
    assert True == True


def test_ca_totals_match_vote_type_16(dbname):
    assert True == True


#CA18 test

def test_ca_presidential_18(dbname):
    assert True == True


def test_ca_statewide_totals_18(dbname):
    assert (not e.data_exists("2018 General","California",dbname=dbname) or e.contest_total(
            "2018 General",
            "California",
            "US Senate CA",
            dbname=dbname,
        )
            == 11113364
    )


def test_ca_senate_totals_18(dbname):
    assert (not e.data_exists("2018 General","California",dbname=dbname) or e.contest_total(
            "2018 General",
            "California",
            "CA Senate District 12",
            dbname=dbname,
        )
            == 203077
    )


def test_ca_rep_18(dbname):
    assert (not e.data_exists("2018 General","California",dbname=dbname) or e.contest_total(
            "2018 General",
            "California",
            "CA House District 60",
            dbname=dbname,
        )
            == 125660
    )


def test_ca_contest_by_vote_type_18(dbname):
    assert True == True


def test_ca_totals_match_vote_type_18(dbname):
    assert True == True


#CA20 test

def test_ca_presidential_20(dbname):
    assert True == True


def test_ca_statewide_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","California",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "California",
            "US Senate CA",
            dbname=dbname,
        )
            == 11113364
    )


def test_ca_senate_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","California",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "California",
            "CA Senate District 12",
            dbname=dbname,
        )
            == 203077
    )


def test_ca_rep_18(dbname):
    assert (not e.data_exists("2020 Primary","California",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "California",
            "CA House District 60",
            dbname=dbname,
        )
            == 125660
    )


def test_ca_contest_by_vote_type_20(dbname):
    assert True == True


def test_ca_totals_match_vote_type_20(dbname):
    assert True == True


def test_ca_presidential_20ppp(dbname):
    assert (not e.data_exists("2020 President Preference Primary","California",dbname=dbname) or e.contest_total(
            "2020 President Preference Primary",
            "California",
            "US President (CA)",
            dbname=dbname,
        )
            == 2780247
    )


### Colorado Data Loading Tests ###
#CO16 test

def test_co_presidential_16(dbname):
    assert (not e.data_exists("2016 General","Colorado",dbname=dbname) or e.contest_total(
            "2016 General",
            "Colorado",
            "US President (CO)",
            dbname=dbname,
        )
            == 2780247
    )


def test_co_statewide_totals_16(dbname):
    assert (not e.data_exists("2016 General","Colorado",dbname=dbname) or e.contest_total(
            "2016 General",
            "Colorado",
            "US Senate CO",
            dbname=dbname,
        )
            == 2743029
    )


def test_co_senate_totals_16(dbname):
    assert (not e.data_exists("2016 General","Colorado",dbname=dbname) or e.contest_total(
            "2016 General",
            "Colorado",
            "CO Senate District 14",
            dbname=dbname,
        )
            == 85788
    )


def test_co_rep_16(dbname):
    assert (not e.data_exists("2016 General","Colorado",dbname=dbname) or e.contest_total(
            "2016 General",
            "Colorado",
            "CO House District 60",
            dbname=dbname,
        )
            == 41303
    )


def test_co_contest_by_vote_type_16(dbname):
    assert True == True


def test_co_totals_match_vote_type_16(dbname):
    assert True == True

#CO18 test

def test_co_presidential_18(dbname):
    assert True == True


def test_co_statewide_totals_18(dbname):
    assert (not e.data_exists("2018 General","Colorado",dbname=dbname) or e.contest_total(
            "2018 General",
            "Colorado",
            "CO Attorney General",
            dbname=dbname,
        )
            == 2491954
    )


def test_co_senate_totals_18(dbname):
    assert (not e.data_exists("2018 General","Colorado",dbname=dbname) or e.contest_total(
            "2018 General",
            "Colorado",
            "CO Senate District 15",
            dbname=dbname,
        )
            == 83690
    )


def test_co_rep_18(dbname):
    assert (not e.data_exists("2018 General","Colorado",dbname=dbname) or e.contest_total(
            "2018 General",
            "Colorado",
            "CO House District 60",
            dbname=dbname,
        )
            == 39237
    )


def test_co_contest_by_vote_type_18(dbname):
    assert True == True


def test_co_totals_match_vote_type_18(dbname):
    assert True == True

#CO20 test

def test_co_presidential_20(dbname):
    assert (not e.data_exists("2020 Primary","Colorado",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Colorado",
            "US President (CO) (Democratic Party)",
            dbname=dbname,
        )
            == 960128
    )


def test_co_statewide_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Colorado",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Colorado",
            "US Senate CO (Republican Party)",
            dbname=dbname,
        )
            == 554806
    )


def test_co_senate_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Colorado",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Colorado",
            "CO Senate District 21 (Republican Party)",
            dbname=dbname,
        )
            == 6320
    )


def test_co_rep_20(dbname):
    assert (not e.data_exists("2020 Primary","Colorado",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Colorado",
            "CO House District 20 (Democratic Party)",
            dbname=dbname,
        )
            == 10011
    )


def test_co_contest_by_vote_type_18(dbname):
    assert True == True


def test_co_totals_match_vote_type_18(dbname):
    assert True == True
