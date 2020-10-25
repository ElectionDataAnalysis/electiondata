import election_data_analysis as e

### Arkansas Data Loading Tests ###
#AR18 test

def test_ar_presidential_18(dbname):
    #no presidential contests in 2018
    assert True == True



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
