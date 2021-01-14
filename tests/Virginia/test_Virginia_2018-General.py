import election_data_analysis as e

election = "2018 General"
jurisdiction = "Virginia"
# VA18 tests
def test_data_exists(dbname):
    assert e.data_exists("2018 General", "Virginia", dbname=dbname)


def test_statewide_totals(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Virginia",
            "US Senator VA",
            dbname=dbname,
        )
        == 3351757
    )


def test_house_totals(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Virginia",
            "US House VA District 1",
            dbname=dbname,
        )
        == 273029
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
