import election_data_analysis as e

def test_data_exists(dbname):
    assert e.data_exists("2018 General", "Ohio", dbname=dbname)

def test_oh_statewide(dbname):
    assert ( e.contest_total(
            "2018 General",
            "Ohio",
            "OH Governor",
            dbname=dbname,
        )
            == 4429582
    )


def test_oh_senate_totals_18(dbname):
    assert ( e.contest_total(
            "2018 General",
            "Ohio",
            "OH Senate District 21",
            dbname=dbname,
        )
            == 110903
    )



def test_oh_house_totals_18(dbname):
    assert ( e.contest_total(
            "2018 General",
            "Ohio",
            "OH House District 2",
            dbname=dbname,
        )
            == 44213
    )

