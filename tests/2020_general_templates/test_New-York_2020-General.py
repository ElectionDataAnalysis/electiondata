import election_data_analysis as e

#NY20g test

def data_exists(dbname):
    assert e.data_exists("2020 General","New York",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "New York",
        "US President (NY)",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "New York",
        "congressional",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "New York",
        "state-senate",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "New York",
        "state-house",
        dbname=dbname,
        )
        == -1
    )

