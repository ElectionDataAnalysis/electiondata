import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "Ohio"


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Ohio", dbname=dbname)


def test_oh_presidential_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Ohio",
            "US President (OH) (Republican Party)",
            dbname=dbname,
        )
        == 713546
    )


# no statewide state executive offices in 2020


def test_oh_house_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Ohio",
            "OH House District 43 (Democratic Party)",
            dbname=dbname,
        )
        == 6664
    )


def test_oh_rep_20_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Ohio",
            "US House OH District 4 (Republican Party)",
            dbname=dbname,
        )
        == 64695
    )


def test_oh_rep_20_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Ohio",
            "US House OH District 4 (Libertarian Party)",
            dbname=dbname,
        )
        == 214
    )


def test_oh_contest_by_vote_type_20(dbname):
    assert (
        e.count_type_total(
            "2020 Primary",
            "Ohio",
            "US House OH District 3 (Republican Party)",
            "total",
            dbname=dbname,
        )
        == 13248
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
