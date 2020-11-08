import election_data_analysis as e


def test_data_exists(dbname):
    assert e.data_exists("2016 General", "Colorado", dbname=dbname)


def test_ak_presidential_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Colorado",
            "US President (CO)",
            dbname=dbname,
        )
        == 2780247
    )


def test_ak_statewide_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Colorado",
            "US Senate CO",
            dbname=dbname,
        )
        == 2743029
    )


def test_ak_senate_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Colorado",
            "CO Senate District 14",
            dbname=dbname,
        )
        == 85788
    )


def test_ak_rep_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Colorado",
            "CO House District 60",
            dbname=dbname,
        )
        == 41303
    )
