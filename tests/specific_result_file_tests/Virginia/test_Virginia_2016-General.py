import election_data_analysis as e

election = "2016 General"
jurisdiction = "Virginia"
# VA16 tests
def test_data_exists(dbname):
    assert e.data_exists("2016 General", "Virginia", dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Virginia",
            "US President (VA)",
            dbname=dbname,
        )
        == 3984631
    )


def test_house_totals(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Virginia",
            "US House VA District 2",
            dbname=dbname,
        )
        == 309915
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
