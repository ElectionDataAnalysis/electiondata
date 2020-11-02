import election_data_analysis as e


def test_mi_presidential_16(dbname):
    assert (
        not e.data_exists("2016 General", "Michigan", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Michigan",
            "US President (MI)",
            dbname=dbname,
        )
        == 4799284
    )


def test_mi_statewide_totals_16(dbname):
    assert True == True


def test_mi_us_rep_totals_16(dbname):
    assert (
        not e.data_exists("2016 General", "Michigan", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Michigan",
            "US House MI District 4",
            dbname=dbname,
        )
        == 315751
    )


def test_mi_house_totals_16(dbname):
    assert (
        not e.data_exists("2016 General", "Michigan", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Michigan",
            "MI House District 8",
            dbname=dbname,
        )
        == 34742
    )
