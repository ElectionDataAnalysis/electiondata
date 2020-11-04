import election_data_analysis as e


def test_data_exists(dbname):
    assert e.data_exists("2016 General", "Wisconsin", dbname=dbname)


def test_wi_presidential(dbname):
    assert (e.contest_total(
            "2016 General",
            "Wisconsin",
            "US President (WI)",
            dbname=dbname,
        )
        == 2976150
    )


def test_wi_statewide_totals(dbname):
    assert (e.contest_total(
            "2016 General",
            "Wisconsin",
            "US Senate WI",
            dbname=dbname,
        )
        == 2948741
    )


# results not availabe for state senate this election


def test_us_house_totals(dbname):
    assert (e.contest_total(
            "2016 General",
            "Wisconsin",
            "US House WI District 5",
            dbname=dbname,
        )
        == 390844
    )


# results not available by vote type
