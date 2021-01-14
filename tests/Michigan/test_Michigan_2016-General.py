import election_data_analysis as e

election = "2016 General"
jurisdiction = "Michigan"


def test_data_exists(dbname):
    assert e.data_exists("2016 General", "Michigan", dbname=dbname)


def test_mi_presidential_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Michigan",
            "US President (MI)",
            dbname=dbname,
        )
        == 4799284
    )


def test_mi_us_rep_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Michigan",
            "US House MI District 4",
            dbname=dbname,
        )
        == 315751
    )


def test_mi_house_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Michigan",
            "MI House District 8",
            dbname=dbname,
        )
        == 34742
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
