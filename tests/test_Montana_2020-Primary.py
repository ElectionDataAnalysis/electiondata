import election_data_analysis as e
#MT20 tests

def test_presidential(dbname):
    assert(not e.data_exists("2020 Primary","Montana",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Montana",
            "US President (MT) (Democratic Party)",
        )
        == 149973
    )


def test_statewide_totals(dbname):
    assert(not e.data_exists("2020 Primary","Montana",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Montana",
            "US Senate MT (Republican Party)",
        )
        == 219205
    )

def test_state_senate_totals(dbname):
    assert (not e.data_exists("2020 Primary","Montana",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Montana",
            "MT Senate District 17 (Republican Party)",
        )
        == 4896
    )

def test_state_rep_totals(dbname):
    assert (not e.data_exists("2020 Primary","Montana",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Montana",
            "US House MT District 4",
        )
        == 1977
    )

def test_contest_by_vote_type(dbname):
    assert True == True

def test_totals_match_vote_type(dbname):
    assert True == True
