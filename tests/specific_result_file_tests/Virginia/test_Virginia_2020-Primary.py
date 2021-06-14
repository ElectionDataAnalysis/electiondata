import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "Virginia"
# VA20 tests
def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Virginia", dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Virginia",
            "US President (VA) (Democratic Party)",
            dbname=dbname,
        )
        == 1323693
    )


def test_statewide_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Virginia",
            "US Senator VA (Republican Party)",
            dbname=dbname,
        )
        == 309804
    )


def test_house_totals_dem(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Virginia",
            "US House VA District 2 (Republican Primary)",
            dbname=dbname,
        )
        == 52514
    )


def test_house_totals_dem(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Virginia",
            "US House VA District 5 (Democratic Primary)",
            dbname=dbname,
        )
        == 54037
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
