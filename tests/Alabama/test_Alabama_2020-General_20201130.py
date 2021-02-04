import election_data_analysis as e

election = "2020 General"
jurisdiction = "Alabama"
# Instructions:
#   Add in the Jurisdiction and abbreviation
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

# source for official result counts in this file: # source:
#  https://www.sos.alabama.gov/sites/default/files/election-2020/Final%20Canvass%20of%20Results-Merged.pdf

jurisdiction = "Alabama"
abbr = "AL"
single_county = "Alabama;Choctaw County"
county_or_other = "county"
pres_votes_county = 3127 + 4296 + 38 + 3


def test_data_exists(dbname):
    assert e.data_exists("2020 General", f"{jurisdiction}", dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            "2020 General",
            f"{jurisdiction}",
            f"US President ({abbr})",
            dbname=dbname,
        )
        == 849624 + 1441170 + 25176 + 7312
    )


def test_senate_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            f"{jurisdiction}",
            f"US Senate {abbr}",
            dbname=dbname,
        )
        == 920478 + 1392076 + 3891
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            f"{jurisdiction}",
            f"US House {abbr} District 1",
            dbname=dbname,
        )
        == 116949 + 211825 + 301
    )


# No state legislature contests, since state legislature elections for Alabama
# occure during midterm years, e.g., 2010, 2014, 2018, ...


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)



def test_county_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            county=single_county,
            sub_unit_type=county_or_other,
        )
        == pres_votes_county
    )



