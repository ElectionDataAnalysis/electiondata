import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "New Hampshire"


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "New Hampshire", dbname=dbname)


def test_nh_presidential_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "New Hampshire",
            "US President (NH) (Democratic Party)",
            dbname=dbname,
        )
        == 298377
    )


def test_nh_presidential_20_rep(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "New Hampshire",
            "US President (NH) (Republican Party)",
            dbname=dbname,
        )
        == 153711
    )


def test_nh_statewide_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "New Hampshire",
            "US Senate NH (Republican Party)",
            dbname=dbname,
        )
        == 139117
    )


def test_nh_statewide_totals_20_dem(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "New Hampshire",
            "US Senate NH (Democratic Party)",
            dbname=dbname,
        )
        == 151405
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
