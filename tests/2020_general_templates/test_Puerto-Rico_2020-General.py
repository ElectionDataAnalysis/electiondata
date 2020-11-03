import election_data_analysis as e

#PR20g test

def data_exists(dbname):
    assert e.data_exists("2020 General","Puerto Rico",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "Puerto Rico",
        "US President (PR)",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Puerto Rico",
        "congressional",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Puerto Rico",
        "state-senate",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Puerto Rico",
        "state-house",
        dbname=dbname,
        )
        == -1
    )

