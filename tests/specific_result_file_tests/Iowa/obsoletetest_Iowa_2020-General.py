import election_data_analysis as e

election = "2020 General"
jurisdiction = "Iowa"
# IA20g test
# Instructions:
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`


def test_data_exists(dbname):
    assert e.data_exists("2020 General", "Iowa", dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Iowa",
            "US President (IA)",
            dbname=dbname,
        )
        == 493433
        + 402853
        + 193211
        + 564369
        + 508
        + 573
        + 657
        + 1047
        + 214
        + 336
        + 1387
        + 1680
        + 11200
        + 8346
        + 247
        + 302
        + 1815
        + 1387
        + 1887
        + 2431
    )


def test_senate_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Iowa",
            "US Senate IA",
            dbname=dbname,
        )
        == 863670 + 753314 + 36821 + 13762 + 1203
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Iowa",
            "US House IA District 3",
            dbname=dbname,
        )
        == 212727 + 218968 + 15338 + 383
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Iowa",
            "IA Senate District 2",
            dbname=dbname,
        )
        == 26372 + 176
    )


def test_state_house_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Iowa",
            "IA House District 4",
            dbname=dbname,
        )
        == 13141 + 2868 + 43
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
