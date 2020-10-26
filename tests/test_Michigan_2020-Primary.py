import election_data_analysis as e

def test_mi_presidential(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Michigan",
            "US President (MI) (Democratic Party)",
            dbname=dbname,
        )
            == 4799284
    )

def test_mi_senate(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Michigan",
            "US Senate MI (Democratic Party)",
            dbname=dbname,
        )
            == 1180780
    )



def test_senate_totals(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Michigan",
            "MI Senate District 37 (Republican Party)",
            dbname=dbname,
        )
            == 124414
    )

def test_house_totals(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Michigan",
            "MI House District 8 (Republican Party)",
            dbname=dbname,
        )
            == 28017
    )
