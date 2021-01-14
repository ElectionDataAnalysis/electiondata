import election_data_analysis as e

election = "2016 General"
jurisdiction = "Texas"
# IA18 tests


def test_data_exists(dbname):
    assert e.data_exists("2016 General", "Texas", dbname=dbname)


def test_tx_presidential_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Texas",
            "US President (TX)",
            dbname=dbname,
        )
        == 8969226
    )


def test_state_railroad_totals(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Texas",
            "Railroad Commissioner",
            dbname=dbname,
        )
        == 8760238
    )


def test_state_contest_totals(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Texas",
            "US House TX District 10",
            dbname=dbname,
        )
        == 312600
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Texas",
            "TX Senate District 11",
            dbname=dbname,
        )
        == 218201
    )


def test_state_rep_totals(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Texas",
            "TX House District 115",
            dbname=dbname,
        )
        == 58926
    )


# # no tests by vote type


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
