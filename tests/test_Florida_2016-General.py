import election_data_analysis as e


def test_fl_presidential(dbname):
    assert (
        not e.data_exists("2016 General", "Florida", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Florida",
            "US President (FL)",
            dbname=dbname,
        )
        == 9420039
    )


def test_fl_statewide_totals(dbname):
    assert (
        not e.data_exists("2016 General", "Florida", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Florida",
            "US Senate FL",
            dbname=dbname,
        )
        == 9301820
    )


def test_fl_senate_totals(dbname):
    assert (
        not e.data_exists("2016 General", "Florida", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Florida",
            "FL Senate District 3",
            dbname=dbname,
        )
        == 236480
    )


def test_fl_house_totals(dbname):
    assert (
        not e.data_exists("2016 General", "Florida", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Florida",
            "US House FL District 10",
            dbname=dbname,
        )
        == 305989
    )


# results not available by vote type
