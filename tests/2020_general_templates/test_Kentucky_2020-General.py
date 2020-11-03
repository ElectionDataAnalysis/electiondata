import election_data_analysis as e

#KY20g test

def data_exists(dbname):
    assert e.data_exists("2020 General","Kentucky",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "Kentucky",
        "US President (KY)",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Kentucky",
        "congressional",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Kentucky",
        "state-senate",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Kentucky",
        "state-house",
        dbname=dbname,
        )
        == -1
    )

