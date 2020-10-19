#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd

from database.modifier_document import ModifierDocument
from api.utils.storage import get_mapped_df, get_check_results_df


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
        column = str(eval(column) + (current_column_count,))
        new_columns.append(column)

    df.columns = new_columns
    return df


def apply_errors_filter(file_id, worksheet_id, errors_filter):
    """Gets all rows that have errors in the specified column or that have the specified error type"""

    error_level = errors_filter.get("level")
    column_code = errors_filter.get("column")
    result_df = get_check_results_df(file_id, worksheet_id)
    indices = set()
    for column in result_df.columns.values:
        check_type, field_code, error_type, check_index = eval(column)
        if (field_code == column_code) or (error_level in [error_type, "all"]):
            if result_df[column].all():
                indices = range(0, result_df.shape[0])
                break
            else:
                indices.update(set(result_df.index[result_df[column] == True]))
    return indices


def apply_filter(file_id, worksheet_id, filters):
    """Applies the filters on mapped_df for data preview"""

    modifier_document = ModifierDocument()

    df = get_mapped_df(file_id, worksheet_id, usecols=[col_filter["column"] for col_filter in filters])
    modifier_document.apply_modifications(df, worksheet_id, is_all=True)
    numeric_operators = {"lessThan": "lt", "lessThanOrEqual": "le", "greaterThan": "gt", "greaterThanOrEqual": "ge"}
    date_operators = {"before": "lt", "after": "gt"}
    equality_operators = {"equals": "eq", "notEqual": "ne"}

    for column_filter in filters:
        column = column_filter["column"]
        operator = column_filter["operator"]
        value = column_filter.get("value")
        if operator in numeric_operators:
            df = df.loc[getattr(pd.to_numeric(df[column], errors='coerce'), numeric_operators[operator])(float(value))]
        elif operator in equality_operators:
            df = df.loc[getattr(df[column], equality_operators[operator])(value)]
        elif operator == 'contains':
            df = df.loc[df[column].str.contains(value)]
        elif operator == 'notContains':
            df = df.loc[~df[column].str.contains(value)]
        elif operator == 'startsWith':
            df = df.loc[df[column].str.startswith(value)]
        elif operator == 'endsWith':
            df = df.loc[df[column].str.endswith(value)]
        elif operator in date_operators:
            df = df.loc[getattr(pd.to_datetime(df[column], errors='coerce'), date_operators[operator])
                        (pd.to_datetime(value))]
    filter_indices = df.index.tolist()
    return filter_indices


def apply_sort(file_id, worksheet_id, sort):
    """Applies sorting on mapped_df for data preview"""

    modifier_document = ModifierDocument()

    df = get_mapped_df(file_id, worksheet_id, usecols=[sort["column"]])
    modifier_document.apply_modifications(df, worksheet_id, is_all=True)
    df.index.name = "index"            
    sort_indices = df.sort_values(by=[sort["column"], "index"], ascending=sort["order"]).index.tolist()

    return sort_indices
