import election_data_analysis as e

#NH20g test

def data_exists(dbname):
    assert e.data_exists("2020 General","New Hampshire",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "New Hampshire",
        "US President (NH)",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "New Hampshire",
        "congressional",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "New Hampshire",
        "state-senate",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "New Hampshire",
        "state-house",
        dbname=dbname,
        )
        == -1
    )

