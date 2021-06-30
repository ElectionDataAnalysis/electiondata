import election_data_analysis as e

# Instructions:
#   Copy this template and rename, including your jurisdiction and the timestamp of the results file
#       following this model: test_North-Carolina_2020_General_20201113_1645.py
#           * start with test_ so pytest will find the file
#           * get the underscores and hyphens right, so the system will find the file
#           * timestamp is YYYYMMDD_xxxx, where xxxx is the military-time Pacific Standard Time
#           * (timestamp is crucial for files collected during recounts and evolving canvass counts)
#   Change the constants to values from your file
#   "triple-quote" out any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

# # # constants - CHANGE THESE!! - use internal db names
# NB: write-ins are not counted in totals given on website
election = "2020 General"
jurisdiction = "Vermont"
jurisdiction_type = "state"
abbr = "VT"
total_pres_votes = (
    370968 - 278 - 3262 - 1942
)  # total of all votes for US President, not including write-ins
cd = 1  # US House congressional district
total_cd_votes = (
    370968 - 383 - 15748 - 542
)  # total votes in that US House contest in the chosen cd (not including write-ins
shd = "Bennington 2-2"  # state house district
total_shd_votes = 7154 - 1511 - 2 - 21  # total votes in that State House contest
ssd = "Grand Isle"  # state senate district
total_ssd_votes = 13516 - 2018 - 1 - 256  # total votes in that State Senate contest
single_vote_type = "total"  # pick any one with corresponding data in your file, but use internal db name
pres_votes_vote_type = (
    total_pres_votes  # total votes for US President of that vote type
)
county_or_other = "town"  # Change this only if results are subdivided by something other than counties
#  e.g., 'parish' in LA, 'state-house' in Alaska, 'ward' in Philadelphia
single_county = (
    "Vermont;Isle La Motte"  # pick any one from your file, but use internal db name
)
pres_votes_county = 356 - 7 - 4  # total votes for US President in that county


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            sub_unit_type=jurisdiction_type,
            dbname=dbname,
        )
        == total_pres_votes
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House {abbr} District {cd}",
            sub_unit_type=jurisdiction_type,
            dbname=dbname,
        )
        == total_cd_votes
    )


# June 2021, trouble reading the xlsx files automatically via xlrd and openpyxl,
# so transformed a few to txt, but not state legislature
"""def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} Senate District {ssd}",
            sub_unit_type=jurisdiction_type,
            dbname=dbname,
        )
        == total_ssd_votes
    )


def test_state_house_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} House District {shd}",
            sub_unit_type=jurisdiction_type,
            dbname=dbname,
        )
        == total_shd_votes
    )

"""


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )


"""
def test_count_type_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            sub_unit_type=county_or_other,
            vote_type=single_vote_type,
        )
        == pres_votes_vote_type
    )
"""


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
