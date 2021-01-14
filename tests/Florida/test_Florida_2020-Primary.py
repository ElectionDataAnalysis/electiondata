import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "Florida"


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Florida", dbname=dbname)


def test_fl_presidential_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Florida",
            "US President (FL) (Democratic Party)",
            dbname=dbname,
        )
        == 3478428 / 2
    )


def test_fl_senate_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Florida",
            "FL Senate District 5 (Republican Party)",
            dbname=dbname,
        )
        == 80339
    )


def test_fl_house_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Florida",
            "FL House District 31 (Democratic Party)",
            dbname=dbname,
        )
        == 12084
    )


def test_us_house_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Florida",
            "US House FL District 5 (Republican Party)",
            dbname=dbname,
        )
        == 33445
    )


# results not available by vote type


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
