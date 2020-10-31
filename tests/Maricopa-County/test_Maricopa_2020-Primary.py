import election_data_analysis as e
election = "2020 Primary"
jurisdiction = "Maricopa County"

def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_presidential_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "US President (AZ) (Democratic Party) (Maricopa County)",
            dbname=dbname,
        )
        == 357375
    )

def test_ussenate_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "US Senate AZ Partial Term (Republican Party) (Maricopa County)",
            dbname=dbname,
        )
        == 439933
    )


def test_senate_totals_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "AZ Senate District 13 (Libertarian Party)",
            dbname=dbname,
        )
        == 37
    )


def test_rep_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "AZ House District 30 (Republican Party)",
            dbname=dbname,
        )
        == 1306
    )


def test_congress_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "US House AZ District 6 (Democratic Party)",
            dbname=dbname,
        )
        == 80286
    )
