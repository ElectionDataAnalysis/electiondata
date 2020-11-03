import election_data_analysis as e

#DC20g test

def data_exists(dbname):
    assert e.data_exists("2020 General","District of Columbia",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "District of Columbia",
        "US President (DC)",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "District of Columbia",
        "congressional",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "District of Columbia",
        "state-senate",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "District of Columbia",
        "state-house",
        dbname=dbname,
        )
        == -1
    )

