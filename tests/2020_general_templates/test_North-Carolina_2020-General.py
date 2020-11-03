import election_data_analysis as e

#NC20g test

def data_exists(dbname):
    assert e.data_exists("2020 General","North Carolina",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "North Carolina",
        "US President (NC)",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "North Carolina",
        "congressional",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "North Carolina",
        "state-senate",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "North Carolina",
        "state-house",
        dbname=dbname,
        )
        == -1
    )

