#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd


def extend_result_df(df, check_result, check_type, field_name, check_level):
    """Append a column to result dataframe"""

    column_name = (check_type, field_name, check_level)
    result_df = pd.concat([df, check_result.rename(str(column_name))], axis=1, sort=False)

    return result_df
