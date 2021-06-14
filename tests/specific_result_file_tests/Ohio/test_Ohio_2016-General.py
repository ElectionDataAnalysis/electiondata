import election_data_analysis as e

election = "2016 General"
jurisdiction = "Ohio"


def test_data_exists(dbname):
    assert e.data_exists("2016 General", "Ohio", dbname=dbname)


def test_oh_presidential_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Ohio",
            "US President (OH)",
            dbname=dbname,
        )
        == 5496487
    )


def test_oh_senate_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Ohio",
            "OH Senate District 16",
            dbname=dbname,
        )
        == 185531
    )


def test_oh_house_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Ohio",
            "OH House District 2",
            dbname=dbname,
        )
        == 51931
    )


def test_oh_contest_by_vote_type_16(dbname):
    assert (
        e.count_type_total(
            "2016 General",
            "Ohio",
            "US House OH District 5",
            "total",
            dbname=dbname,
        )
        == 344991
    )


def test_oh_totals_match_vote_type_16(dbname):
    assert (
        e.check_totals_match_vote_types("2016 General", "Ohio", dbname=dbname) == True
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
