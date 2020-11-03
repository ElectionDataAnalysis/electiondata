import election_data_analysis as e

#NA20g test

def data_exists(dbname):
    assert e.data_exists("2020 General","National",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "National",
        "US President (NA)",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "National",
        "congressional",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "National",
        "state-senate",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "National",
        "state-house",
        dbname=dbname,
        )
        == -1
    )

