#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd

from database.modifier_document import ModifierDocument
<<<<<<< HEAD
from api.utils.storage import get_mapped_df
=======
from api.utils.storage import get_mapped_df, get_check_results_df
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818


def extend_result_df(df, check_result, check_type, field_name, check_level):
    """Append a column to result dataframe"""

    column_name = (check_type, field_name, check_level)
    result_df = pd.concat([df, check_result.rename(str(column_name))], axis=1, sort=False)

    return result_df


<<<<<<< HEAD
def apply_filter(file_id, worksheet_id, filters):
    """Applies the filters on mapped_df for data preview"""

    sort_indices = []
=======
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

>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
    modifier_document = ModifierDocument()

    df = get_mapped_df(file_id, worksheet_id, usecols=[col_filter["column"] for col_filter in filters])
    modifier_document.apply_modifications(df, worksheet_id, is_all=True)
<<<<<<< HEAD
    date_operators = {"Before": "lt", "After": "gt"}
=======
    date_operators = {"Before": "lessThan", "After": "greaterThan"}
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818

    for column_filter in filters:
        column = column_filter["column"]
        operator = column_filter["operator"]
        value = column_filter.get("value")
<<<<<<< HEAD
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
=======
        if operator in ('lessThan', 'lessThanOrEqual', 'greaterThan', 'greaterThanOrEqual'):
            df = df.loc[getattr(pd.to_numeric(df[column], errors='coerce'), operator)(float(value))]
        elif operator in ('equals', 'notEqual'):
            df = df.loc[getattr(df[column], operator)(value)]
        elif operator == 'contains':
            df = df.loc[df[column].str.contains(value)]
        elif operator == 'notContains':
            df = df.loc[~df[column].str.contains(value)]
        elif operator == 'startsWith':
            df = df.loc[df[column].str.startswith(value)]
        elif operator == 'endsWith':
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
            df = df.loc[df[column].str.endswith(value)]
        elif operator == date_operators:
            df = df.loc[getattr(pd.to_datetime(df[column], errors='coerce'), date_operators[operator])
                        (pd.to_datetime(value))]
    filter_indices = df.index.tolist()
    return filter_indices
<<<<<<< HEAD
    # if sort:
    # 	sort_indices = apply_sort(df, sort)

    # return [elem for elem in sort_indices if elem in filter_indices] if sort_indices else filter_indices
=======
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818


def apply_sort(file_id, worksheet_id, sort):
    """Applies sorting on mapped_df for data preview"""

<<<<<<< HEAD
# if not filtred:
# 	modifier_document = ModifierDocument()
# 	modifier_document.apply_modifications(df, worksheet_id, is_all=True)

=======
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
    modifier_document = ModifierDocument()

    df = get_mapped_df(file_id, worksheet_id, usecols=[sort["column"]])
    modifier_document.apply_modifications(df, worksheet_id, is_all=True)
    df.index.name = "index"            
    sort_indices = df.sort_values(by=[sort["column"], "index"], ascending=sort["order"]).index.tolist()

<<<<<<< HEAD
    return sort_indices
=======
    return sort_indices
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
