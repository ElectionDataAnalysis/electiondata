import election_data_analysis as e
#IA18 tests


def test_presidential():
    assert True == True

def test_statewide_totals():
    assert(not e.data_exists("2018 General","Iowa",dbname=dbname) or e.contest_total(
            "2018 General",
            "Iowa",
            "IA Governor",
        )
        == 1327638
    )

def test_state_senate_totals():
    assert (not e.data_exists("2018 General","Iowa",dbname=dbname) or e.contest_total(
            "2018 General",
            "Iowa",
            "IA Senate District 13",
        )
        == 30787
    )

def test_state_rep_totals():
    assert (not e.data_exists("2018 General","Iowa",dbname=dbname) or e.contest_total(
            "2018 General",
            "Iowa",
            "US House IA District 14",
        )
        == 1382
    )

def test_contest_by_vote_type():
    assert True = True


def test_totals_match_vote_type():
    assert True == True
