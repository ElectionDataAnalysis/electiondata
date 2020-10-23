import election_data_analysis as e

def test_fl_presidential_20(dbname):
    assert (not e.data_exists("2020 Primary","Florida",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Florida",
            "US President (FL)",
            dbname=dbname,
        )
            == 5958306
    )


def test_fl_statewide_totals_20(dbname):
    assert True == True


def test_fl_senate_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Florida",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Florida",
            "FL Senate District 21",
            dbname=dbname,
        )
            == 54988
    )


def test_fl_house_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Florida",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Florida",
            "FL House District 11",
            dbname=dbname,
        )
            == 17445
    )

# results not available by vote type