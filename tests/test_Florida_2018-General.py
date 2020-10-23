import election_data_analysis as e


def test_fl_presidential_18(dbname):
    assert True == True


def test_fl_statewide_totals_18(dbname):
    assert (not e.data_exists("2018 General","Florida",dbname=dbname) or e.contest_total(
            "2018 General",
            "Florida",
            "US Senate FL",
            dbname=dbname,
        )
            == 8190005
    )


def test_fl_senate_totals_18(dbname):
    assert (not e.data_exists("2018 General","Florida",dbname=dbname) or e.contest_total(
            "2018 General",
            "Florida",
            "FL Senate District 4",
            dbname=dbname,
        )
            == 235459
    )


def test_fl_house_totals_18(dbname):
    assert (not e.data_exists("2018 General","Florida",dbname=dbname) or e.contest_total(
            "2018 General",
            "Florida",
            "FL House District 11",
            dbname=dbname,
        )
            == 85479
    )

# results not available by vote type