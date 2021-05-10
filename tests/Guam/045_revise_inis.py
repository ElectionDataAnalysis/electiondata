import os
import re

results_dir = "/Users/singer3/PycharmProjects/election_data_analysis/tests/TestingData"
state = "Vermont"
abbr = "VT"
jurisdiction_dir = "/Users/singer3/PycharmProjects/election_data_analysis/src/jurisdictions"
ini_dir = "/Users/singer3/PycharmProjects/election_data_analysis/src/ini_files_for_results"
ext = ".xlsx"
state_results_dir = os.path.join(results_dir, state)


for state in os.listdir(ini_dir):
    state_ini_dir = os.path.join(ini_dir, state)
    if os.path.isdir(state_ini_dir):
        for ini in os.listdir(state_ini_dir):
            if ini[-4:] == ".ini":




p = {
    "senate": re.compile("Senate_(.*)_11_03_2020.*"),
    "house": re.compile("House_(.*)_11_03_2020.*"),
    "statewide": re.compile("StateWide_(.*)_11_03_2020.*"),
    "federal": re.compile("Federal(.*)_11_03_2020.*"),
}

generic_name = {
    "senate": f"{abbr} Senate District ",
    "house": f"{abbr} House District ",
    "statewide": f"{abbr} ",
    "federal": f"US ",
}

ru_type = {
    "senate": f"state-senate",
    "house": f"state-house",
    "statewide": f"{abbr} ",
}


def ini_contents(f, contest_raw, contest_internal):
    content_str = f"""[election_data_analysis]
results_file={state}/{f}
jurisdiction_directory={state}
munger_name=vt_pres
top_reporting_unit={state}
election=2020 General
results_short_name={abbr}_{contest_raw}.replace(" ","-")
results_download_date=2020-11-17
results_source=https://electionresults.vermont.gov/Index.html#/
results_note=
is_preliminary=False
CountItemType=total
CandidateContest={contest_internal}"""
    return content_str


for f in os.listdir(state_results_dir):
    if f[-len(ext):] == ext:
        for contest_type in ["senate", "house", "statewide"]:
            contest_raw_list = p[contest_type].findall(f)
            for c in contest_raw_list:
                contest_raw = c.title()
                contest_internal = f"{generic_name[contest_type]}{contest_raw}"
                # create ini file
                ini_file_name = f"{abbr}_20g_{contest_type}_{contest_raw}.ini".replace(" ","-")
                with open(os.path.join(state_ini_dir, ini_file_name),"w") as g:
                    g.write(ini_contents(f, contest_raw, contest_internal))
                # add contest to CandidateContest.txt, Office.txt, ReportingUnit.txt
                cc_str = f"\n{contest_internal}\t1\t{contest_internal}\t"
                if contest_type == "statewide":
                    office_str = f"\n{contest_internal}\t{state}"
                else:
                    office_str = f"\n{contest_internal}\t{state};{contest_internal}"
                with open(os.path.join(jurisdiction_dir,state,"CandidateContest.txt"),"a") as g:
                    g.write(cc_str)
                with open(os.path.join(jurisdiction_dir,state,"Office.txt"),"a") as g:
                    g.write(office_str)
                if contest_type != "statewide":
                    ru_str = f"\n{state};{contest_internal}\t{ru_type[contest_type]}"
                    with open(os.path.join(jurisdiction_dir, state, "ReportingUnit.txt"), "a") as g:
                        g.write(ru_str)

exit()
