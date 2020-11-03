import election_data_analysis as e

#MP20g test

def data_exists(dbname):
    assert e.data_exists("2020 General","Northern Mariana Islands",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "Northern Mariana Islands",
        "US President (MP)",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Northern Mariana Islands",
        "congressional",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Northern Mariana Islands",
        "state-senate",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Northern Mariana Islands",
        "state-house",
        dbname=dbname,
        )
        == -1
    )

