#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import ntpath
import os


def compare(
    fpath1: str, fpath2: str, out, count_col: str = "Count"
) -> (pd.DataFrame, str):
    err = None
    df1 = pd.read_csv(fpath1, sep="\t")
    df2 = pd.read_csv(fpath2, sep="\t")

    name1 = ntpath.basename(fpath1)
    name2 = ntpath.basename(fpath2)

    if set(df1.columns) != set(df2.columns):
        err = (
            f"Incompatible columns: \n{fpath1}\n{df1.columns}\n{fpath2}\n{df2.columns}"
        )
        return pd.DataFrame(), err

    if count_col not in df1.columns:
        err = f"Count column {count_col} not found in columns"
        return pd.DataFrame(), err

    merge_on = [c for c in df1.columns if c != count_col]
    df = df1.merge(df2, how="outer", on=merge_on, suffixes=[f"_{name1}", f"_{name2}"])
    df = df[
        (df[f"{count_col}_{name1}"].isnull())
        | (df[f"{count_col}_{name1}"].isnull())
        | (df[f"{count_col}_{name1}"] != df[f"{count_col}_{name2}"])
    ]
    df.to_csv(os.path.join(out, f"compare_{name1}_{name2}.csv"), index=False)
    return df, err
