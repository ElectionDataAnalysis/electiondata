import elections as e

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
#   Move this testing file to the correct jurisdiction folder in `elections/tests`

# # # constants - CHANGE THESE!! - use internal db names
election = "2020 General"
jurisdiction = "Kansas"
abbr = "KS"
total_pres_votes = (
    1457960  # total from official file, does not match total from website
)
# 771406 + 570323 + 30574  # total of all votes for US President per website
cd = 3  # US House congressional district
cd_3_Wyandotte = 16788 + 37552 + 1914  # from official results file
cd_3_Johnson = 157148 + 180329 + 9474  # from official results file
cd_3_all_others = 7213  # from official results file
total_cd_votes = cd_3_Wyandotte + cd_3_Johnson + cd_3_all_others
# same as from website: 220049 + 178773 + 11596  # total votes in that US House contest in the chosen cd
shd = 2  # state house district
total_shd_votes = 6759 + 4115  # total votes in that State House contest
ssd = 15  # state senate district
total_ssd_votes = 23043  # total votes in that State Senate contest
single_vote_type = "early"  # pick any one with corresponding data in your file, but use internal db name
pres_votes_vote_type = -1  # total votes for US President of that vote type
county_or_other = "county"  # Change this only if results are subdivided by something other than counties
#  e.g., 'parish' in LA, 'state-house' in Alaska, 'ward' in Philadelphia
single_county = (
    "Kansas;Bourbon County"  # pick any one from your file, but use internal db name
)
pres_votes_county = 6676  # total votes for US President in that county


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            sub_unit_type=county_or_other,
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
            sub_unit_type=county_or_other,
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


"""def test_count_type_subtotal(dbname):
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


def test_wyandotte_subtotal_pres(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            county="Kansas;Wyandotte County",
            sub_unit_type=county_or_other,
        )
        == 36788 + 1063 + 18934
    )


def test_wyandotte_subtotal_cd_3(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House KS District 3",
            dbname=dbname,
            county="Kansas;Wyandotte County",
            sub_unit_type=county_or_other,
        )
        == 56254
    )


# because of duplicate info from Wyandotte in different files, couldn't resolve this.
"""def test_wyandotte_subtotal_state_senate_4(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"KS Senate District 4",
            dbname=dbname,
            county="Kansas;Wyandotte County",
            sub_unit_type=county_or_other,
        )
        == 17319 + 4715
    )
"""


def test_johnson_subtotal_cd_3(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House KS District 3",
            dbname=dbname,
            county="Kansas;Johnson County",
            sub_unit_type=county_or_other,
        )
        == 346951
    )
