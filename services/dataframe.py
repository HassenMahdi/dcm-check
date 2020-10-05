#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd

from database.modifier_document import ModifierDocument
from api.utils.storage import get_mapped_df


def extend_result_df(df, check_result, check_type, field_name, check_level):
    """Append a column to result dataframe"""
    column_name = (check_type, field_name, check_level)
    result_df = pd.concat([df, check_result.rename(str(column_name))], axis=1, sort=False)

    return result_df


def reindex_result_df(df):
    counters = {}
    new_columns = []
    for column in df.columns:
        current_column_count = counters.get(column, 0)
        counters[column] = current_column_count + 1
        # if current_column_count:
        column = str(eval(column) + (current_column_count,))
        new_columns.append(column)

    df.columns = new_columns
    return df



def apply_filter(file_id, worksheet_id, filters):
    """Applies the filters on mapped_df for data preview"""

    sort_indices = []
    modifier_document = ModifierDocument()

    df = get_mapped_df(file_id, worksheet_id, usecols=[col_filter["column"] for col_filter in filters])
    modifier_document.apply_modifications(df, worksheet_id, is_all=True)
    date_operators = {"Before": "lt", "After": "gt"}

    for column_filter in filters:
        column = column_filter["column"]
        operator = column_filter["operator"]
        value = column_filter.get("value")
        if operator in ('lt', 'le', 'gt', 'ge'):
            df = df.loc[getattr(pd.to_numeric(df[column], errors='coerce'), operator)(float(value))]
        elif operator in ('eq', 'ne'):
            df = df.loc[getattr(df[column], operator)(value)]
        elif operator == 'Contains':
            df = df.loc[df[column].str.contains(value)]
        elif operator == 'Not contains':
            df = df.loc[~df[column].str.contains(value)]
        elif operator == 'Starts with':
            df = df.loc[df[column].str.startswith(value)]
        elif operator == 'Ends with':
            df = df.loc[df[column].str.endswith(value)]
        elif operator == date_operators:
            df = df.loc[getattr(pd.to_datetime(df[column], errors='coerce'), date_operators[operator])
                        (pd.to_datetime(value))]
    filter_indices = df.index.tolist()
    return filter_indices
    # if sort:
    # 	sort_indices = apply_sort(df, sort)

    # return [elem for elem in sort_indices if elem in filter_indices] if sort_indices else filter_indices


def apply_sort(file_id, worksheet_id, sort):
    """Applies sorting on mapped_df for data preview"""

# if not filtred:
# 	modifier_document = ModifierDocument()
# 	modifier_document.apply_modifications(df, worksheet_id, is_all=True)

    modifier_document = ModifierDocument()

    df = get_mapped_df(file_id, worksheet_id, usecols=[sort["column"]])
    modifier_document.apply_modifications(df, worksheet_id, is_all=True)
    df.index.name = "index"            
    sort_indices = df.sort_values(by=[sort["column"], "index"], ascending=sort["order"]).index.tolist()

    return sort_indices