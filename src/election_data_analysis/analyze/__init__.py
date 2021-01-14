import os.path

import pandas as pd
from election_data_analysis import user_interface as ui
from election_data_analysis import munge as m
import datetime
import os
import numpy as np
from pathlib import Path
from election_data_analysis import database as db
import scipy.spatial.distance as dist
from scipy import stats
import json


def child_rus_by_id(session, parents, ru_type=None):
    """Given a list <parents> of parent ids (or just a single parent_id), return
    list containing all children of those parents.
    (By convention, a ReportingUnit counts as one of its own 'parents',)
    If (ReportingUnitType_Id,OtherReportingUnit) pair <rutype> is given,
    restrict children to that ReportingUnitType"""
    cruj = pd.read_sql_table("ComposingReportingUnitJoin", session.bind)
    children = list(
        cruj[cruj.ParentReportingUnit_Id.isin(parents)].ChildReportingUnit_Id.unique()
    )
    if ru_type:
        assert len(ru_type) == 2, f"argument {ru_type} does not have exactly 2 elements"
        ru = pd.read_sql_table("ReportingUnit", session.bind, index_col="Id")
        right_type_ru = ru[
            (ru.ReportingUnitType_Id == ru_type[0])
            & (ru.OtherReportingUnitType == ru_type[1])
        ]
        children = [x for x in children if x in right_type_ru.index]
    return children


def create_rollup(
    session,
    target_dir: str,
    top_ru_id: int,
    sub_rutype_id: int,
    election_id: int,
    datafile_list: list = None,
    by: str = "Id",
    by_vote_type: bool = False,
) -> str:
    """<target_dir> is the directory where the resulting rollup will be stored.
    <election_id> identifies the election; <datafile_id_list> the datafile whose results will be rolled up.
    <top_ru_id> is the internal cdf name of the ReportingUnit whose results will be reported
    <sub_rutype_id> identifies the ReportingUnitType
    of the ReportingUnits used in each line of the results file
    created by the routine. (E.g., county or ward)
    <datafile_list> is a list of files, with entries from field <by> in _datafile table.
    If no <datafile_list> is given, return all results for the given election.
    """

    connection = session.bind.raw_connection()
    cursor = connection.cursor()
    if not datafile_list:
        datafile_list, e = db.data_file_list(cursor, election_id, by="Id")
        if e:
            return e
        by = "Id"
        if len(datafile_list) == 0:
            return f"No datafiles found for Election_Id {election_id}"
    # set exclude_redundant_total
    vote_type_list, err_str = db.vote_type_list(cursor, datafile_list, by=by)
    if err_str:
        return err_str
    elif len(vote_type_list) == 0:
        return f"No vote types found for datafiles with {by} in {datafile_list} "

    if len(vote_type_list) > 1 and "total" in vote_type_list:
        exclude_redundant_total = True
    else:
        exclude_redundant_total = False

    # get names from ids
    top_ru = db.name_from_id(cursor, "ReportingUnit", top_ru_id)
    election = db.name_from_id(cursor, "Election", election_id)
    sub_rutype = db.name_from_id(cursor, "ReportingUnitType", sub_rutype_id)

    # create path to export directory
    leaf_dir = os.path.join(target_dir, election, top_ru, f"by_{sub_rutype}")
    Path(leaf_dir).mkdir(parents=True, exist_ok=True)

    # prepare inventory
    inventory_file = os.path.join(target_dir, "inventory.txt")
    inv_exists = os.path.isfile(inventory_file)
    if inv_exists:
        inv_df = pd.read_csv(inventory_file, sep="\t")
    else:
        inv_df = pd.DataFrame()
    inventory = {
        "Election": election,
        "ReportingUnitType": sub_rutype,
        "source_db_url": cursor.connection.dsn,
        "timestamp": datetime.date.today(),
    }

    for contest_type in ["BallotMeasure", "Candidate"]:
        # export data
        rollup_file = f"{cursor.connection.info.dbname}_{contest_type}_results.txt"
        while os.path.isfile(os.path.join(leaf_dir, rollup_file)):
            rollup_file = input(
                f"There is already a file called {rollup_file}. Pick another name.\n"
            )

        df, err_str = db.export_rollup_from_db(
            session,
            top_ru=top_ru,
            election=election,
            sub_unit_type=sub_rutype,
            contest_type=contest_type,
            datafile_list=datafile_list,
            by=by,
            exclude_redundant_total=exclude_redundant_total,
            by_vote_type=by_vote_type,
        )
        if not err_str:
            # create record for inventory.txt
            inv_df = inv_df.append(inventory, ignore_index=True).fillna("")
            err_str = None
            df.to_csv(os.path.join(leaf_dir, rollup_file), index=False, sep="\t")

    # export to inventory file
    inv_df.to_csv(inventory_file, index=False, sep="\t")
    cursor.close()
    return err_str


def create_scatter(
    session,
    jurisdiction_id,
    subdivision_type_id,
    h_election_id,
    h_category,
    h_count,
    h_type,
    h_runoff,
    v_election_id,
    v_category,
    v_count,
    v_type,
    v_runoff,
):
    connection = session.bind.raw_connection()
    cursor = connection.cursor()

    # get the mappings back to the DB labels
    h_count = ui.get_contest_type_mapping(h_count)
    v_count = ui.get_contest_type_mapping(v_count)

    dfh = get_data_for_scatter(
        session,
        jurisdiction_id,
        subdivision_type_id,
        h_election_id,
        h_category,
        h_count,
        h_type,
        h_runoff,
    )
    dfv = get_data_for_scatter(
        session,
        jurisdiction_id,
        subdivision_type_id,
        v_election_id,
        v_category,
        v_count,
        v_type,
        v_runoff,
    )
    if dfh.empty or dfv.empty:
        connection.close()
        return None

    unsummed = pd.concat([dfh, dfv])
    jurisdiction = db.name_from_id(cursor, "ReportingUnit", jurisdiction_id)

    # check if there is only 1 candidate selection (with multiple count types)
    single_selection = len(unsummed["Selection"].unique()) == 1
    # check if there is only one contest
    single_count_type = len(unsummed["CountItemType"].unique()) == 1

    if (h_runoff or v_runoff) and single_selection:
        pivot_col = "Contest"
    elif single_selection and not single_count_type:
        pivot_col = "CountItemType"
    elif single_selection and single_count_type:
        pivot_col = "Election_Id"
    else:
        pivot_col = "Selection"
    pivot_df = pd.pivot_table(
        unsummed, values="Count", index=["Name"], columns=pivot_col, aggfunc=np.sum
    ).reset_index()
    pivot_df = pivot_df.dropna()
    pivot_df.columns = pivot_df.columns.map(str)
    if pivot_df.empty:
        connection.close()
        return None

    # package up results
    if (h_runoff or v_runoff) and single_selection:
        cols = list(pivot_df.columns)
        results = package_results(pivot_df, jurisdiction, cols[-2], cols[-1])
        results["x"] = cols[-2]
        results["y"] = cols[-1]
    elif single_selection and not single_count_type:
        results = package_results(pivot_df, jurisdiction, h_category, v_category)
        results["x"] = h_count
        results["y"] = v_count
    elif single_selection and single_count_type:
        results = package_results(
            pivot_df, jurisdiction, str(h_election_id), str(v_election_id)
        )
        results["x"] = h_count
        results["y"] = v_count
    else:
        results = package_results(pivot_df, jurisdiction, h_count, v_count)
    results["x-election"] = db.name_from_id(cursor, "Election", h_election_id)
    results["y-election"] = db.name_from_id(cursor, "Election", v_election_id)
    results["subdivision_type"] = db.name_from_id(
        cursor, "ReportingUnitType", subdivision_type_id
    )
    results["x-count_item_type"] = h_category
    results["y-count_item_type"] = v_category
    results["x-title"] = scatter_axis_title(
        cursor,
        results["x"],
        results["x-election"],
        dfh.iloc[0]["Contest"],
        jurisdiction_id,
    )
    results["y-title"] = scatter_axis_title(
        cursor,
        results["y"],
        results["y-election"],
        dfv.iloc[0]["Contest"],
        jurisdiction_id,
    )
    h_preliminary = db.is_preliminary(cursor, h_election_id, jurisdiction_id)
    v_preliminary = db.is_preliminary(cursor, v_election_id, jurisdiction_id)
    results["preliminary"] = h_preliminary or v_preliminary

    # only keep the ones where there are an (x, y) to graph
    to_keep = []
    for result in results["counts"]:
        # need reporting unit, x, y, and x_ y_ pcts
        # otherwise it's invalid
        if len(result) == 5:
            to_keep.append(result)
    if not to_keep:
        connection.close()
        return None

    results["counts"] = to_keep
    connection.close()
    return results


def package_results(data, jurisdiction, x, y, restrict=None):
    results = {"jurisdiction": jurisdiction, "x": x, "y": y, "counts": []}
    if restrict and len(data.index) > restrict:
        data = get_remaining_averages(data, restrict)
    for i, row in data.iterrows():
        total = row[x] + row[y]
        if total == 0:
            x_pct = y_pct = 0
        else:
            x_pct = row[x] / total
            y_pct = row[y] / total
        results["counts"].append(
            {
                "name": row["Name"],
                "x": row[x],
                "y": row[y],
                "x_pct": x_pct,
                "y_pct": y_pct,
            }
        )
    return results


def get_data_for_scatter(
    session,
    jurisdiction_id,
    subdivision_type_id,
    election_id,
    count_item_type,
    filter_str,
    count_type,
    is_runoff,
):
    if count_type == "census":
        return get_census_data(
            session,
            jurisdiction_id,
            election_id,
            filter_str,
        )
    else:
        return get_votecount_data(
            session,
            jurisdiction_id,
            subdivision_type_id,
            election_id,
            count_item_type,
            filter_str,
            count_type,
            is_runoff
        )


def get_census_data(
    session,
    jurisdiction_id,
    election_id,
    filter_str,
):
    # get the census data
    connection = session.bind.raw_connection()
    cursor = connection.cursor()
    election = db.name_from_id(cursor, "Election", election_id)
    census_df = db.read_external(
        cursor,
        int(election[0:4]),
        jurisdiction_id,
        ["County", "Category", "Label", "Value"],
        restrict=filter_str,
    )
    cursor.close()

    # reshape data so it can be unioned with other results
    if not census_df.empty:
        census_df["Election_Id"] = election_id
        census_df["Contest_Id"] = 0
        census_df["Candidate_Id"] = 0
        census_df["Contest"] = "Census data"
        census_df["CountItemType"] = "total"
        census_df.rename(
            columns={"County": "Name", "Label": "Selection", "Value": "Count"},
            inplace=True,
        )
        census_df = census_df[
            [
                "Election_Id",
                "Name",
                "Selection",
                "Contest_Id",
                "Candidate_Id",
                "Contest",
                "CountItemType",
                "Count",
            ]
        ]
        return census_df
    return pd.DataFrame()


def get_votecount_data(
    session,
    jurisdiction_id,
    subdivision_type_id,
    election_id,
    count_item_type,
    filter_str,
    count_type,
    is_runoff,
):
    unsummed = db.get_candidate_votecounts(
        session, election_id, jurisdiction_id, subdivision_type_id
    )

    # limit to relevant data - runoff
    if is_runoff:
        unsummed = unsummed[
            unsummed["Contest"].str.contains("runoff", case=False)
        ]
    else:
        unsummed = unsummed[
            ~(unsummed["Contest"].str.contains("runoff", case=False))
        ]

    # limit to relevant data - count type
    if count_type == "candidates":
        filter_column = "Selection"
    elif count_type == "contests":
        filter_column = "Contest"
    elif count_type == "parties":
        filter_str = ui.get_contest_type_mapping(filter_str)
        # create new column to filter on
        unsummed["party_district_type"] = (
            (unsummed["Party"].str.replace("Party", "")).str.strip()
            + " "
            + unsummed["contest_district_type"]
        )
        filter_column = "party_district_type"

    # limit to relevant data - all data or not
    keep_all = filter_str.startswith("All ")
    if not keep_all:
        unsummed = unsummed[unsummed[filter_column].isin([filter_str])]
    if "party_district_type" in unsummed.columns:
        del unsummed["party_district_type"]
    unsummed = unsummed[unsummed["CountItemType"] == count_item_type]

    # cleanup for purposes of flexibility
    unsummed = unsummed[
        [
            "Election_Id",
            "ParentName",
            "Count",
            "Selection",
            "Contest_Id",
            "Candidate_Id",
            "Contest",
            "CountItemType",
        ]
    ].rename(columns={"ParentName": "Name"})

    # if All contests or All candidates
    if keep_all or count_type == "parties":
        unsummed["Selection"] = filter_str
        unsummed["Contest_Id"] = -1
        unsummed["Candidate_Id"] = -1
        unsummed["Contest"] = filter_str

    if count_type == "contests" and not keep_all:
        connection = session.bind.raw_connection()
        cursor = connection.cursor()
        unsummed["Selection"] = filter_str
        cursor.close()

    columns = list(unsummed.drop(columns="Count").columns)
    unsummed = unsummed.groupby(columns)["Count"].sum().reset_index()

    return unsummed


def create_bar(
    session,
    top_ru_id,
    subdivision_type_id,
    contest_type,
    contest,
    election_id,
    for_export,
):

    connection = session.bind.raw_connection()
    cursor = connection.cursor()

    unsummed = db.get_candidate_votecounts(
        session, election_id, top_ru_id, subdivision_type_id
    )

    if contest_type:
        contest_type = ui.get_contest_type_mapping(contest_type)
        unsummed = unsummed[unsummed["contest_district_type"] == contest_type]

    # through front end, contest_type must be truthy if contest is truthy
    # Only filter when there is an actual contest passed through, as opposed to
    # "All congressional" as an example
    if contest and not contest.startswith("All "):
        unsummed = unsummed[unsummed["Contest"] == contest]

    groupby_cols = [
        "ParentReportingUnit_Id",
        "ParentName",
        "ParentReportingUnitType_Id",
        "Candidate_Id",
        "CountItemType_Id",
        "CountItemType",
        "Contest_Id",
        "Contest",
        "Selection",
        "Selection_Id",
        "contest_type",
        "contest_district_type",
        "Party",
    ]
    unsummed = unsummed.groupby(groupby_cols).sum().reset_index()
    multiple_ballot_types = len(unsummed["CountItemType"].unique()) > 1

    # Now process data - this is the heart of the scoring/ranking algorithm
    try:
        ranked = assign_anomaly_score(unsummed)
        ranked["margins_pct"] = ranked["Count"] / ranked["reporting_unit_total"]
        ranked_margin = ranked
        votes_at_stake = calculate_votes_at_stake(ranked_margin)
        if not for_export:
            top_ranked = get_most_anomalous(votes_at_stake, 3)
        else:
            top_ranked = votes_at_stake
    except Exception:
        connection.close()
        return None
    if top_ranked.empty:
        connection.close()
        return None

    # package into list of dictionary
    result_list = []
    ids = top_ranked["unit_id"].unique()
    for idx in ids:
        temp_df = top_ranked[top_ranked["unit_id"] == idx]
        # some cleaning here to make the pivoting work
        scores_df = temp_df[temp_df["rank"] != 1]
        scores_df = scores_df[["ReportingUnit_Id", "score", "margins_pct"]]
        scores_df.rename(
            columns={
                "score": "max_score",
                "margins_pct": "max_margins_pct",
            },
            inplace=True,
        )
        temp_df = temp_df.merge(scores_df, how="left", on="ReportingUnit_Id")
        temp_df.drop(columns=["score", "margins_pct"], inplace=True)
        temp_df.rename(
            columns={
                "max_score": "score",
                "max_margins_pct": "margins_pct",
            },
            inplace=True,
        )

        candidates = temp_df["Candidate_Id"].unique()
        x = db.name_from_id(cursor, "Candidate", int(candidates[0]))
        y = db.name_from_id(cursor, "Candidate", int(candidates[1]))
        x_party = unsummed.loc[unsummed["Candidate_Id"] == candidates[0], "Party"].iloc[
            0
        ]
        x_party_abbr = create_party_abbreviation(x_party)
        y_party = unsummed.loc[unsummed["Candidate_Id"] == candidates[1], "Party"].iloc[
            0
        ]
        y_party_abbr = create_party_abbreviation(y_party)
        jurisdiction = db.name_from_id(cursor, "ReportingUnit", top_ru_id)

        pivot_df = pd.pivot_table(
            temp_df, values="Count", index=["Name"], columns="Selection", fill_value=0
        ).reset_index()
        score_df = temp_df.groupby("Name")[
            ["score", "margins_pct", "margin_ratio"]
        ].mean()
        pivot_df = pivot_df.merge(score_df, how="inner", on="Name")
        pivot_df = sort_pivot_by_margins(pivot_df)

        if for_export:
            results = package_results(pivot_df, jurisdiction, x, y)
        else:
            results = package_results(pivot_df, jurisdiction, x, y, restrict=8)
        results["election"] = db.name_from_id(cursor, "Election", election_id)
        results["contest"] = db.name_from_id(
            cursor, "Contest", int(temp_df.iloc[0]["Contest_Id"])
        )
        results["subdivision_type"] = db.name_from_id(
            cursor, "ReportingUnitType", int(temp_df.iloc[0]["ReportingUnitType_Id"])
        )
        results["count_item_type"] = temp_df.iloc[0]["CountItemType"]

        # display votes at stake, margin info
        results["votes_at_stake_raw"] = temp_df.iloc[0]["votes_at_stake"]
        results["margin_raw"] = (
            temp_df[temp_df["rank"] == 1].iloc[0]["ind_total"]
            - temp_df[temp_df["rank"] != 1].iloc[0]["ind_total"]
        )
        votes_at_stake = human_readable_numbers(results["votes_at_stake_raw"])
        if votes_at_stake[0] == "-":
            votes_at_stake = votes_at_stake[1:]
            acted = "narrowed"
        else:
            acted = "widened"
        results["votes_at_stake"] = f"Outlier {acted} margin by ~ {votes_at_stake}"
        results["margin"] = human_readable_numbers(results["margin_raw"])
        results["preliminary"] = db.is_preliminary(cursor, election_id, top_ru_id)

        # display ballot info
        if multiple_ballot_types:
            results[
                "ballot_types"
            ] = f"""{results["jurisdiction"]} provides data by vote type"""
        else:
            results["ballot_types"] = "Data by vote type unavailable"

        # display name with party
        results["x"] = f"""{results["x"]} ({x_party_abbr})"""
        results["y"] = f"""{results["y"]} ({y_party_abbr})"""

        results["score"] = temp_df["score"].max()
        results[
            "title"
        ] = f"""{results["count_item_type"].replace("-", " ").title()} Ballots Reported"""
        download_date = db.data_file_download(cursor, election_id, top_ru_id)
        if db.is_preliminary(cursor, election_id, top_ru_id) and download_date:
            results[
                "title"
            ] = f"""{results["title"]} as of {download_date} (preliminary)"""

        result_list.append(results)
    connection.close()
    return result_list


def assign_anomaly_score(data):
    """adds a new column called score between 0 and 1; 1 is more anomalous.
    Also adds a `unit_id` column which assigns a score to each unit of analysis
    that is considered. For example, we may decide to look at anomalies across each
    distinct combination of contest, reporting unit type, and vote type. Each
    combination of those would get assigned an ID. This means rows may get added
    to the dataframe if needed."""

    # Assign a ranking for each candidate by votes for each contest
    if "total" not in data["CountItemType"].unique():
        groupby_cols = list(data.columns)
        groupby_cols.remove("Count")
        total_data = data.groupby(groupby_cols).sum().reset_index()
    else:
        total_data = data[data["CountItemType"] == "total"]

    ranked_df = (
        total_data.groupby(["Contest_Id", "Selection", "Selection_Id"], as_index=False)[
            "Count"
        ]
        .sum()
        .sort_values(["Contest_Id", "Count"], ascending=False)
    )
    ranked_df["rank"] = ranked_df.groupby("Contest_Id")["Count"].rank(
        "dense", ascending=False
    )
    ranked_df.rename(columns={"Count": "ind_total"}, inplace=True)

    # Now get the total votes for the entire contest
    contest_df = ranked_df.groupby("Contest_Id")["ind_total"].sum().reset_index()
    contest_df.rename(columns={"ind_total": "contest_total"}, inplace=True)
    ranked_df = ranked_df.merge(contest_df, how="inner", on="Contest_Id")

    # Group data by parent info. This works because each child is also its own
    # parent in the DB table
    grouped_df = (
        data.groupby(
            [
                "ParentReportingUnit_Id",
                "ParentName",
                "ParentReportingUnitType_Id",
                "Candidate_Id",
                "CountItemType_Id",
                "CountItemType",
                "Contest_Id",
                "Contest",
                "Selection",
                "contest_type",
                "contest_district_type",
            ],
            as_index=False,
        )["Count"]
        .sum()
        .reset_index()
    )
    grouped_df.drop(columns="index", inplace=True)
    grouped_df.rename(
        columns={
            "ParentReportingUnit_Id": "ReportingUnit_Id",
            "ParentName": "Name",
            "ParentReportingUnitType_Id": "ReportingUnitType_Id",
        },
        inplace=True,
    )
    grouped_df = grouped_df.merge(
        ranked_df, how="inner", on=["Contest_Id", "Selection"]
    )

    # assign temporary unit_ids to unique combination of contest,
    # ru_type, and count type. These will be updated later to account
    # for 2 candidate pairings
    df_unit = grouped_df[
        ["Contest_Id", "ReportingUnitType_Id", "CountItemType"]
    ].drop_duplicates()
    df_unit = df_unit.reset_index()
    df_unit["unit_id_tmp"] = df_unit.index
    df_with_units = grouped_df.merge(
        df_unit, how="left", on=["Contest_Id", "ReportingUnitType_Id", "CountItemType"]
    )

    # loop through each unit ID and assign anomaly scores
    # also update the "real" unit_id which takes into account pairing of candidates
    unit_ids_tmp = df_with_units["unit_id_tmp"].unique()
    unit_id = 0
    df = pd.DataFrame()
    # for each unit ID
    for unit_id_tmp in unit_ids_tmp:
        # grab all the data there
        temp_df = df_with_units[df_with_units["unit_id_tmp"] == unit_id_tmp]
        for i in range(2, int(temp_df["rank"].max()) + 1):
            selection_df = temp_df[temp_df["rank"].isin([1, i])].copy()
            selection_df["unit_id"] = unit_id
            unit_id += 1
            total = (
                selection_df.groupby("ReportingUnit_Id")["Count"].sum().reset_index()
            )
            total.rename(columns={"Count": "reporting_unit_total"}, inplace=True)
            selection_df = selection_df.merge(total, how="inner", on="ReportingUnit_Id")
            if selection_df.shape[0] >= 12 and len(selection_df["rank"].unique()) > 1:
                pivot_df = (
                    pd.pivot_table(
                        selection_df,
                        values="Count",
                        index=["ReportingUnit_Id", "reporting_unit_total"],
                        columns="Selection",
                    )
                    .sort_values("ReportingUnit_Id")
                    .reset_index()
                )
                pivot_df = pivot_df[pivot_df["reporting_unit_total"] > 0]
                pivot_df_values = pivot_df.drop(
                    columns=["ReportingUnit_Id", "reporting_unit_total"]
                )
                to_drop = [
                    selection_df[selection_df["rank"] == 1]["Selection"].unique()[0],
                    selection_df[selection_df["rank"] == i]["Selection"].unique()[0],
                ]
                # pass in proportions instead of raw vlaues
                vote_proportions = pivot_df_values.div(
                    pivot_df["reporting_unit_total"], axis=0
                )
                np.nan_to_num(vote_proportions, copy=False)
                # assign z score and then add back into final DF
                scored = euclidean_zscore(vote_proportions.to_numpy())
                pivot_df["score"] = scored
                pivot_df = pivot_df[["ReportingUnit_Id", to_drop[1], "score"]]
                pivot_df["Selection"] = to_drop[1]
                pivot_df.rename(columns={to_drop[1]: "Count"}, inplace=True)
                scored_df = selection_df.merge(
                    pivot_df, how="left", on=["ReportingUnit_Id", "Selection", "Count"]
                )
                df = pd.concat([df, scored_df])
    if "score" in df.columns:
        df["score"] = df["score"].fillna(0)
    return df


def get_most_anomalous(data, n):
    """Gets n contest, with 2 from largest votes at stake ratio
    and 1 with largest score. If 2 from votes at stake cannot be found
    (bc of threshold for score) then we fill in the top n from scores"""
    # filter out very small votes at stake margins
    data = data[(data["margin_ratio"] > 0.01) | (data["margin_ratio"] < -0.01)]

    # grab data by highest votes at stake margin (magnitude)
    margin_data = data[data["score"] > 2.3]
    unit_by_margin = get_unit_by_column(margin_data, "margin_ratio")
    # grab data by highest z-score (magnitude)
    unit_by_score = get_unit_by_column(data, "score")
    # get data for n deduped unit_ids, with n-1 from margin data, filling
    # in from score data if margin data is unavailable
    unit_ids_all = unit_by_margin[0 : n - 1] + unit_by_score
    unit_ids = list(dict.fromkeys(unit_ids_all).keys())[0:n]
    data = data[data["unit_id"].isin(unit_ids)]

    zeros_df = data[
        [
            "Contest_Id",
            "ReportingUnitType_Id",
            "CountItemType",
            "ReportingUnit_Id",
            "Count",
        ]
    ]
    zeros_df = zeros_df.groupby(
        ["Contest_Id", "ReportingUnitType_Id", "ReportingUnit_Id", "CountItemType"]
    ).sum()
    zeros_df = zeros_df.reset_index()
    no_zeros = zeros_df[zeros_df["Count"] != 0]
    data = data.merge(
        no_zeros,
        how="inner",
        on=["Contest_Id", "ReportingUnitType_Id", "ReportingUnit_Id", "CountItemType"],
    )
    data.rename(columns={"Count_x": "Count"}, inplace=True)
    data.drop(columns=["Count_y"], inplace=True)

    data.sort_values(
        by=["margin_ratio", "Name", "Selection"],
        ascending=[False, True, True],
        inplace=True,
    )

    # now we get the top 8 reporting unit IDs, in terms of anomaly score, of the winner and most anomalous
    ids = data["unit_id"].unique()
    df = pd.DataFrame()
    for idx in ids:
        temp_df = data[data["unit_id"] == idx]
        max_score = temp_df["score"].max()
        if max_score > 0:
            rank = temp_df[temp_df["score"] == max_score].iloc[0]["rank"]
            temp_df = temp_df[temp_df["rank"].isin([1, rank])]
            df = pd.concat([df, temp_df])
    return df


def euclidean_zscore(li):
    """Take a list of vectors -- all in the same R^k,
    returns a list of the z-scores of the vectors -- each relative to the ensemble"""
    distance_list = [sum([dist.euclidean(item, y) for y in li]) for item in li]
    if len(set(distance_list)) == 1:
        # if all distances are the same, which yields z-score nan values
        return [0] * len(li)
    else:
        return list(stats.zscore(distance_list))


def calculate_votes_at_stake(data) -> pd.DataFrame:
    """Move the most anomalous pairing to the equivalent of the second-most anomalous
    and calculate the differences in votes that would be returned"""
    df = pd.DataFrame()
    unit_ids = data["unit_id"].unique()
    for unit_id in unit_ids:
        temp_df = data[data["unit_id"] == unit_id].copy()
        try:
            # get a df of the most anomalous pairing
            max_score = temp_df["score"].max()
            index = temp_df.index[temp_df["score"] == max_score][0]
            reporting_unit_id = temp_df.loc[index, "ReportingUnit_Id"]
            selection = temp_df.loc[index, "Selection"]
            margin_pct = temp_df.loc[index, "margins_pct"]
            reporting_unit_total = temp_df.loc[index, "reporting_unit_total"]
            anomalous_df = (
                temp_df[
                    (temp_df["ReportingUnit_Id"] == reporting_unit_id)
                    & (
                        (temp_df["score"] == max_score)
                        | (temp_df["rank"] == 1)
                        & (temp_df["reporting_unit_total"] == reporting_unit_total)
                    )
                ]
                .sort_values("rank", ascending=False)
                .drop_duplicates()
            )

            # Identify the next closest RU in terms of margins
            filtered_df = temp_df[
                (temp_df["ReportingUnit_Id"] != reporting_unit_id)
                & (temp_df["Selection"] == selection)
            ]
            # this finds the closest margin on either side (+/-)
            next_index = filtered_df.iloc[
                (filtered_df["margins_pct"] - margin_pct).abs().argsort()[:1]
            ].index[0]
            next_reporting_unit_id = temp_df.loc[next_index, "ReportingUnit_Id"]
            next_margin_pct = temp_df.loc[next_index, "margins_pct"]
            next_reporting_unit_total = temp_df.loc[next_index, "reporting_unit_total"]
            next_anomalous_df = (
                temp_df[
                    (temp_df["ReportingUnit_Id"] == next_reporting_unit_id)
                    & (
                        (temp_df["margins_pct"] == next_margin_pct)
                        | (temp_df["rank"] == 1)
                        & (temp_df["reporting_unit_total"] == next_reporting_unit_total)
                    )
                ]
                .sort_values("rank", ascending=False)
                .drop_duplicates()
            )

            # move the most anomalous to the closest and calculate what the
            # change to the Contest margin would be
            winner_bucket_total = int(anomalous_df[anomalous_df["rank"] == 1]["Count"])
            not_winner_bucket_total = int(
                anomalous_df[anomalous_df["rank"] != 1]["Count"]
            )
            reported_bucket_total = int(anomalous_df["Count"].sum())
            next_bucket_total = int(next_anomalous_df["Count"].sum())
            adj_margin = (
                next_anomalous_df[next_anomalous_df["rank"] != 1].iloc[0]["Count"]
                / next_bucket_total
            )
            not_winner_adj_bucket_total = int(reported_bucket_total * adj_margin)
            winner_adj_bucket_total = (
                reported_bucket_total - not_winner_adj_bucket_total
            )

            # calculate margins by raw numbers for the bucket
            contest_margin = winner_bucket_total - not_winner_bucket_total
            adj_contest_margin = winner_adj_bucket_total - not_winner_adj_bucket_total

            # calculate margins by raw numbers for the entire contest
            contest_margin_ttl = (
                anomalous_df[anomalous_df["rank"] == 1].iloc[0]["ind_total"]
                - anomalous_df[anomalous_df["rank"] != 1].iloc[0]["ind_total"]
            )
            temp_df["votes_at_stake"] = contest_margin - adj_contest_margin
            temp_df["margin_ratio"] = temp_df["votes_at_stake"] / contest_margin_ttl
        except Exception:
            temp_df["margin_ratio"] = 0
            temp_df["votes_at_stake"] = 0
        df = pd.concat([df, temp_df])
    return df


def create_candidate_contests(df, columns):
    contest_df = (
        df["VoteCount"]
        .merge(df["Contest"], how="left", left_on="Contest_Id", right_index=True)
        .rename(columns={"Name": "Contest", "Id": "ContestSelectionJoin_Id"})
        .merge(
            df["CandidateSelection"],
            how="left",
            left_on="Selection_Id",
            right_index=True,
        )
        .merge(df["Candidate"], how="left", left_on="Candidate_Id", right_index=True)
        .rename(columns={"BallotName": "Selection"})
        .merge(
            df["CandidateContest"], how="left", left_on="Contest_Id", right_index=True
        )
        .merge(df["Office"], how="left", left_on="Office_Id", right_index=True)
    )
    contest_df = contest_df[columns]
    if contest_df.empty:
        contest_df["contest_type"] = None
    else:
        contest_df = m.add_constant_column(contest_df, "contest_type", "Candidate")
    return contest_df


def create_ballot_measure_contests(df, columns):
    ballotmeasure_df = (
        df["ContestSelectionJoin"]
        .merge(
            df["BallotMeasureContest"],
            how="right",
            left_on="Contest_Id",
            right_index=True,
        )
        .rename(columns={"Name": "Contest"})
        .merge(
            df["BallotMeasureSelection"],
            how="left",
            left_on="Selection_Id",
            right_index=True,
        )
    )
    ballotmeasure_df = ballotmeasure_df[columns]
    if ballotmeasure_df.empty:
        ballotmeasure_df["contest_type"] = None
    else:
        ballotmeasure_df = m.add_constant_column(
            ballotmeasure_df, "contest_type", "BallotMeasure"
        )
    return ballotmeasure_df


def get_unit_by_column(data, column):
    """Given a dataframe of results, return a list of unique unit_ids
    that are sorted in desc order by the column's value"""
    data = data[["unit_id", column]]
    data = data.groupby("unit_id").max(column).sort_values(by=column, ascending=False)
    data = data.reset_index()
    return list(data["unit_id"].unique())


def human_readable_numbers(value):
    abs_value = abs(value)
    if abs_value < 10:
        return str(value)
    elif abs_value < 100:
        return str(round(value, -1))
    elif abs_value < 1000:
        return str(round(value, -2))
    else:
        return "{:,}".format(round(value, -3))


def sort_pivot_by_margins(df):
    """grab the row with the highest anomaly score, then sort the remainder by
    margin. The sorting order depends on whether the anomalous row is >50% or <50%"""

    # create an DF of just the most anomalous row
    i = df.index[df["score"] == df["score"].max()][0]
    anomalous_df = df.iloc[i, :].to_frame().transpose().reset_index(drop=True)

    # get the rest of the rows and sort based on margins
    remainder_df = df.drop(index=i)
    sort_ascending = (
        anomalous_df.iloc[0, anomalous_df.columns.get_indexer(["margins_pct"])[0]]
        < remainder_df["margins_pct"].max()
    )
    remainder_df = remainder_df.sort_values(
        "margins_pct", ascending=sort_ascending
    ).reset_index(drop=True)
    remainder_df.index = remainder_df.index + 1

    return pd.concat([anomalous_df, remainder_df])


def get_remaining_averages(df, restrict):
    """Take a dataframe and keep a number of the rows as-is up to the restrict
    number. The remaining rows get aggregated, either summed by the vote counts
    or averaged for the other metrics."""
    columns = df.columns.to_list()
    columns.remove("Name")
    df[columns] = df[columns].apply(pd.to_numeric)

    actual_df = df.iloc[0 : restrict - 1, :]
    average_df = df.iloc[restrict - 1 :, :].copy()

    average_df["Name"] = "Average of all others"
    average_df = average_df.groupby("Name").mean().reset_index()
    average_df.index = [restrict - 1]
    return pd.concat([actual_df, average_df])


def create_party_abbreviation(party):
    if party.strip().lower().startswith("none "):
        return "N/A"
    else:
        return (party.strip())[0].upper()


def dedupe_scatter_title(category, election, contest):
    title = f"{category} - {election}"
    if category != contest:
        title = f"{title} - {contest}"
    return title


def scatter_axis_title(cursor, category, election, contest, jurisdiction_id):
    if contest == "Census data":
        # get the actual year of data here
        census_year = db.read_external(
            cursor,
            int(election[0:4]),
            jurisdiction_id,
            ["CensusYear"],
            restrict=category,
        )["CensusYear"].iloc[0]
        return f"{category} - {census_year} American Community Survey"
    else:
        title = dedupe_scatter_title(category, election, contest)
        return ui.get_contest_type_display(title)


def nist_candidate_contest(session, election_id, jurisdiction_id):
    """return all the candidate contest info, including info related to
    the contest, the selection, and the actual vote counts themselves"""
    vote_count_df = db.read_vote_count(
        session,
        election_id,
        jurisdiction_id,
        ["VoteCount_Id", "ReportingUnit_Id", "CountItemType", "Count", "Selection_Id"],
        ["Id", "GpUnitId", "CountItemType", "Count", "Selection_Id"],
    )

    selection_df = db.read_vote_count(
        session,
        election_id,
        jurisdiction_id,
        ["Selection_Id", "Party_Id", "Candidate_Id", "Contest_Id"],
        ["Id", "PartyId", "CandidateId", "ContestId"],
    )

    contest_df = db.read_vote_count(
        session,
        election_id,
        jurisdiction_id,
        ["Contest_Id", "ContestName", "ContestType"],
        ["Id", "ContestName", "ContestType"],
    )
    contest_df = contest_df[contest_df["ContestType"] == "Candidate"]
    contest_df["Type"] = "CandidateContest"
    contest_df.drop(columns="ContestType", inplace=True)

    result = contest_df.to_json(orient="records")
    contests = json.loads(result)
    # for each contest, get the selections
    for contest in contests:
        contest["BallotSelection"] = []
        contest_id = contest["Id"]
        tmp_selection_df = selection_df[selection_df["ContestId"] == contest_id]
        selection_ids = tmp_selection_df["Id"].unique()

        # for each selection, get the selection/candidate info and vote count info
        for selection_id in selection_ids:
            # first the selection info
            selection_result_df = tmp_selection_df[
                tmp_selection_df["Id"] == selection_id
            ][["Id", "CandidateId"]].drop_duplicates()
            selection_result_df["Type"] = "CandidateSelection"
            selection_result = json.loads(
                selection_result_df.to_json(orient="records")
            )[0]

            # then the votecount info
            vote_count_by_selection_df = vote_count_df[
                vote_count_df["Selection_Id"] == selection_id
            ][["GpUnitId", "CountItemType", "Count"]]
            vote_count_result = vote_count_by_selection_df.to_json(orient="records")
            selection_result["VoteCounts"] = json.loads(vote_count_result)
            contest["BallotSelection"].append(selection_result)

    return contests


def nist_reporting_unit(session, election_id, jurisdiction_id):
    """ A ReportingUnit is a GPUnit """
    df = db.read_vote_count(
        session,
        election_id,
        jurisdiction_id,
        ["GP_Id", "GPReportingUnitName", "GPType"],
        ["Id", "Name", "ReportingUnitType"],
    )
    df["Type"] = "ReportingUnit"
    result = df.to_json(orient="records")
    return json.loads(result)


def nist_party(session, election_id, jurisdiction_id):
    df = db.read_vote_count(
        session,
        election_id,
        jurisdiction_id,
        ["Party_Id", "PartyName"],
        ["Id", "Name"],
    )
    result = df.to_json(orient="records")
    return json.loads(result)


def nist_election(session, election_id, jurisdiction_id):
    df = db.read_vote_count(
        session,
        election_id,
        jurisdiction_id,
        ["Election_Id", "ElectionName", "ElectionType"],
        ["Id", "Name", "Type"],
    )
    # Currently all our elections are at the state level.
    # Once we start handling local elections, this might need to be updated.
    df["ReportingUnit"] = jurisdiction_id
    # we also do not collect start date or end date of election at the moment
    df["StartDate"] = "uncollected"
    df["EndDate"] = "uncollected"
    result = df.to_json(orient="records")
    return json.loads(result)


def nist_office(session, election_id, jurisdiction_id):
    df = db.read_vote_count(
        session,
        election_id,
        jurisdiction_id,
        ["Office_Id", "OfficeName"],
        ["Id", "Name"],
    )
    result = df.to_json(orient="records")
    return json.loads(result)


def nist_candidate(session, election_id, jurisdiction_id):
    df = db.read_vote_count(
        session,
        election_id,
        jurisdiction_id,
        ["Candidate_Id", "BallotName", "Party_Id"],
        ["Id", "BallotName", "PartyId"],
    )
    result = df.to_json(orient="records")
    return json.loads(result)
