import election_data_analysis as e
election = "2016 General"
jurisdiction = "Maricopa County"

def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_presidential_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "US President (AZ) (Maricopa County)",
            dbname=dbname,
        )
        == 1567834
    )

def test_ussenate_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "US Senate AZ Partial Term (Republican Party) (Maricopa County)",
            dbname=dbname,
        )
        == 1534633
    )


def test_senate_totals_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "AZ Senate District 13 (Maricopa County)",
            dbname=dbname,
        )
        == 44317
    )


def test_rep_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "AZ House District 30 (Maricopa County)",
            dbname=dbname,
        )
        == 60755
    )


def test_congress_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "US House AZ District 6 (Maricopa County)",
            dbname=dbname,
        )
        == 325126
    )
