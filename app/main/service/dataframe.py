#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
from app.db.Models.checker_documents import CheckerDocument

def get_columns_as_str(df):
    """Transforms all the dataframe columns name to string if they are of type tuple"""

    columns = df.columns.values
    deserialized_columns = []
    for c in columns:
        deserialized_columns.append(get_one_column_as_str(c))

    return deserialized_columns


def get_one_column_as_str(column):
    """Transforms a dataframe column name to string if it is of type tuple"""

    deserialized_column = column
    if type(column) == tuple:
        deserialized_column = "-_-" + "_".join(list(column))
    return deserialized_column


def extend_result_df(df, check_result, check_type, field_name, check_level):
    """Append a column to result dataframe"""

    column_name = (check_type, field_name, check_level)
    result_df = pd.concat([df, check_result.rename(str(column_name))], axis=1, sort=False)

    return result_df


def apply_check_modifications(df, modifications):
    """Apply check modification to the mapped dataframe"""

    if modifications["is_all"]:
        for column, value in modifications["content"].items():
            df[column] = value
        return df

    else:
        for column, value in modifications["content"].items():
            df[column].loc[modifications["indices"]] = value
        return df.loc[modifications["indices"]].reset_index(drop=True)


def calculate_field(df, expression, mappings, formulas):
    """Calculates dataframe column based on expression param"""

    if expression is None:
        return 0

    first_operand = expression.get("op1")
    second_operand = expression.get("op2")

    if (first_operand is None) and (second_operand is None):
        operand_type = expression.get("type")
        if operand_type == "field_value":
            if expression["code"] in mappings:
                return pd.to_numeric(df[expression['code']], errors='coerce').fillna(0)
            return calculate_field(df, formulas[expression["code"]], mappings, formulas)

        if operand_type == "constant":
            return expression["value"]

        if operand_type == "reference":
            checker_document = CheckerDocument()
            reference_data = checker_document.get_calculated_field_ref(df, expression)
            return pd.to_numeric(df[expression["conditions"][0]["value"]["code"]].map(reference_data)
                                 .replace(",", ".", regex=True), errors='coerce').fillna(0)
    left = calculate_field(df, first_operand, mappings, formulas)
    right = calculate_field(df, second_operand, mappings, formulas)
    operator = expression['operator']

    return pd.eval(f'left {operator} right')
