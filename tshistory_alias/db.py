import pandas as pd
import numpy as np

from tshistory_alias import tsio


def register_priority(cn, path, override=False):
    df = pd.read_csv(path)
    aliases = np.unique(df['alias'])
    map_prune = {}
    map_coef = {}
    tsh = tsio.TimeSerie()
    for alias in aliases:
        sub_df = df[df['alias'] == alias]
        sub_df = sub_df.sort_values(by='priority')
        list_names = sub_df['serie']
        for row in sub_df.itertuples():
            if not pd.isnull(row.prune):
                map_prune[row.serie] = row.prune
            if not pd.isnull(row.coefficient):
                map_coef[row.serie] = row.coefficient
        tsh.build_priority(cn, alias, list_names, map_prune, map_coef, override)


def register_arithmetic(cn, path, override=False):
    df = pd.read_csv(path)
    aliases = np.unique(df['alias'])
    tsh = tsio.TimeSerie()
    for alias in aliases:
        sub_df = df[df['alias'] == alias]
        map_coef = {
            row.serie: row.coefficient
            for row in sub_df.itertuples()
        }
        tsh.build_arithmetic(cn, alias, map_coef, override)
