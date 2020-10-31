import election_data_analysis as e


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Michigan", dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Michigan",
            "US President (MI) (Democratic Party)",
            dbname=dbname,
        )
        == 1587679
    )


def test_senate(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Michigan",
            "US Senate MI (Democratic Party)",
            dbname=dbname,
        )
        == 1180780
    )


def test_us_rep_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Michigan",
            "US House MI District 5 (Republican Party)",
            dbname=dbname,
        )
        == 47367
    )


def test_house_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Michigan",
            "MI House District 8 (Republican Party)",
            dbname=dbname,
        )
        == 238
    )
