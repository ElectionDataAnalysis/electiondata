import election_data_analysis as e

#WI20g test

def data_exists(dbname):
    assert e.data_exists("2020 General","Wisconsin",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "Wisconsin",
        "US President (WI)",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Wisconsin",
        "congressional",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Wisconsin",
        "state-senate",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Wisconsin",
        "state-house",
        dbname=dbname,
        )
        == -1
    )

