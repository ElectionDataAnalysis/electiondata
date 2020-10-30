import election_data_analysis as e

### Arkansas Data Loading Tests ###
#AR18 test

def test_mi_statewide_totals_20(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "Michigan",
            "US Senate MI (Democratic Party)",
            dbname=dbname,
        )
            == 1180780
    )


def test_mi_us_rep_totals_20(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "Michigan",
            "US House MI District 9 (Democratic Party)",
            dbname=dbname,
        )
            == 103202
    )


def test_mi_state_rep_totals_20(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "Michigan",
            "MI House District 37 (Republican Party)",
            dbname=dbname,
        )
            == 6669
    )



def test_mi_presidential_20ppp(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "Michigan",
            "US President (MI) (Democratic Party)",
            dbname=dbname,
        )
            == 4250585
    )


### Delaware 2020 Primary Data Loading Tests ###

def test_de_statewide_totals(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "Delaware",
                "DE Governor (Republican Party)",
            dbname=dbname,
        )
            == 55447
    )


def test_de_senate_totals(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "Delaware",
                "DE Senate District 13 (Democratic Party)",
            dbname=dbname,
        )
            == 5940
    )


def test_de_house_totals(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "Delaware",
                "DE House District 26 (Democratic Party)",
            dbname=dbname,
        )
            == 2990
    )


def test_de_contest_by_vote_type(dbname):
    assert ( 
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
    assert ( e.contest_total(
            "2020 Primary",
            "Delaware",
            "US President (DE) (Democratic Party)",
            dbname=dbname,
        )
            == 91682
    )


def test_de_totals_match_vote_type(dbname):
    assert ( 
            e.check_totals_match_vote_types("2020 Primary","Delaware" ,dbname=dbname) == True)


### Illinois Data Loading Tests ###

def test_il_presidential_16(dbname):
    assert ( e.contest_total(
            "2016 General",
            "Illinois",
            "US President (IL)",
            dbname=dbname,
        )
            == 5536424
    )


def test_il_statewide_totals_16(dbname):
    assert ( e.contest_total(
            "2016 General",
            "Illinois",
            "IL Comptroller",
            dbname=dbname,
        )
            == 5412543
    )


def test_il_senate_totals_16(dbname):
    assert ( e.contest_total(
            "2016 General",
            "Illinois",
            "US Senate IL",
            dbname=dbname,
        )
            == 5491878
    )


def test_il_rep_16(dbname):
    assert ( e.contest_total(
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
    assert ( e.contest_total(
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
    assert ( e.contest_total(
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
    assert ( e.contest_total(
            "2020 Primary",
            "Illinois",
            "US President (IL)",
            dbname=dbname,
        )
            == 2216933
    )


def test_il_statewide_totals_20(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "Illinois",
            "US Senate IL",
            dbname=dbname,
        )
            == 1941286
    )


def test_il_state_senate_totals_20(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "Illinois",
            "IL Senate District 11",
            dbname=dbname,
        )
            == 22716
    )


def test_il_state_rep_totals_20(dbname):
    assert ( e.contest_total(
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

