import election_data_analysis as e

#NM20g test

def data_exists(dbname):
    assert e.data_exists("2020 General","New Mexico",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "New Mexico",
        "US President (NM)",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "New Mexico",
        "congressional",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "New Mexico",
        "state-senate",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "New Mexico",
        "state-house",
        dbname=dbname,
        )
        == -1
    )

