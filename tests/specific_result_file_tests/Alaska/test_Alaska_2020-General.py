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
## NOte: roll-up doesn't find Absentee FWAB votes, as these are statewide. So had to change some roll-ups to "state"
election = "2020 General"
jurisdiction = "Alaska"
abbr = "AK"
total_pres_votes = (
    359530 - 1961
)  # total of all votes for US President, less unresolved write-ins
cd = 1  # US House congressional district
total_cd_votes = 353165 - 1183  # total votes in that US House contest in the chosen cd
shd = 1  # state house district
total_shd_votes = 6815 - 19  # total votes in that State House contest
ssd = "B"  # state senate district
total_ssd_votes = 17879 - 67  # total votes in that State Senate contest
single_vote_type = (
    0  # pick any one with corresponding data in your file, but use internal db name
)
pres_votes_vote_type = 0  # total votes for US President of that vote type
county_or_other = "state-house"  # Change this only if results are subdivided by something other than counties
#  e.g., 'parish' in LA, 'state-house' in Alaska, 'ward' in Philadelphia
single_county = "Alaska;AK House District 1"  # pick any one from your file, but use internal db name
pres_votes_county = (
    3511 + 3477 + 216 + 50 + 34 + 24 + 2
)  # total votes for US President in that county
# from https://www.elections.alaska.gov/results/20GENR/Map/


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            sub_unit_type="state",
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
            sub_unit_type="state",
            dbname=dbname,
        )
        == total_cd_votes
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} Senate District {ssd}",
            sub_unit_type=county_or_other,
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
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_shd_votes
    )


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )


def test_count_type_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            vote_type=single_vote_type,
        )
        == pres_votes_vote_type
    )


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
