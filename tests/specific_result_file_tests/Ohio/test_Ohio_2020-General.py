import election_data_analysis as e

election = "2020 General"
jurisdiction = "Ohio"


def test_data_exists(dbname):
    assert e.data_exists("2020 General", "Ohio", dbname=dbname)


def test_oh_presidential_20(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Ohio",
            "US President (OH)",
            dbname=dbname,
        )
        == 5762920
    )


def test_oh_house_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Ohio",
            "OH House District 2",
            dbname=dbname,
        )
        == 56269
    )


def test_congressional_house_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Ohio",
            "US House OH District 1",
            dbname=dbname,
        )
        == 372747
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)
