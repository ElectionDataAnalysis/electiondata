import election_data_analysis as e

#OK20g test

def data_exists(dbname):
    assert e.data_exists("2020 General","Oklahoma",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "Oklahoma",
        "US President (OK)",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Oklahoma",
        "congressional",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Oklahoma",
        "state-senate",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Oklahoma",
        "state-house",
        dbname=dbname,
        )
        == -1
    )

