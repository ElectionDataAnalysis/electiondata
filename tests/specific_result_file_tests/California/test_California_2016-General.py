import election_data_analysis as e

election = "2016 General"
jurisdiction = "California"


def test_data_exists(dbname):
    assert e.data_exists("2016 General", "California", dbname=dbname)


def test_ca_presidential_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "California",
            "US President (CA)",
            dbname=dbname,
        )
        == 14181595
    )


def test_ca_statewide_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "California",
            "US Senate CA",
            dbname=dbname,
        )
        == 12244170
    )


def test_ca_senate_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "California",
            "CA Senate District 15",
            dbname=dbname,
        )
        == 313531
    )


def test_ca_rep_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "California",
            "CA House District 60",
            dbname=dbname,
        )
        == 142114
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
