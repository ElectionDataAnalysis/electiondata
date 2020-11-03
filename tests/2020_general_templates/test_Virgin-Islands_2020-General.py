import election_data_analysis as e

#VI20g test

def data_exists(dbname):
    assert e.data_exists("2020 General","Virgin Islands",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "Virgin Islands",
        "US President (VI)",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Virgin Islands",
        "congressional",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Virgin Islands",
        "state-senate",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Virgin Islands",
        "state-house",
        dbname=dbname,
        )
        == -1
    )

