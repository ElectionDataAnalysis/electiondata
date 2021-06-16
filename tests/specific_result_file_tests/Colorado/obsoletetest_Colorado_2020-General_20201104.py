import election_data_analysis as e

election = "2020 General"
jurisdiction = "Colorado"
# CO20g test
# Instructions:
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`


def test_data_exists(dbname):
    assert e.data_exists("2020 General", "Colorado", dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Colorado",
            "US President (CO)",
            dbname=dbname,
        )
        == 1634546
        + 1224304
        + 4484
        + 2333
        + 7323
        + 298
        + 42723
        + 2090
        + 1740
        + 483
        + 532
        + 312
        + 371
        + 288
        + 160
        + 665
        + 778
        + 541
        + 483
        + 143
        + 6295
    )


def test_senate_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Colorado",
            "US Senate CO",
            dbname=dbname,
        )
        == 1569209 + 1283954 + 8429 + 7105 + 45914
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Colorado",
            "US House CO District 1",
            dbname=dbname,
        )
        == 79979 + 265478 + 1578 + 969 + 5524
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Colorado",
            "CO Senate District 4",
            dbname=dbname,
        )
        == 70311 + 40381 + 2957
    )


def test_state_house_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Colorado",
            "CO House District 56",
            dbname=dbname,
        )
        == 32937 + 22752 + 2264
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
