#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd

from services.check.checkers.checker import Checker


class DateBoundryChecker(Checker):

    check_code = "DATE_BOUNDRY_CHECK"
    check_name = "Date Boundry Value Check"
    check_message = "The field %s is not in %s ."
    check_level = "errors"

    def check_column(self, df, column, empty_column, *args, **kwargs):
        """Checks if a column is greater then numeric value"""

        if not empty_column.all():
            check = kwargs.get("check")
            operator = check.get("operator")
            operand = pd.to_datetime(check.get("operand"), errors="coerce")

            if operand in df.columns:
                df_column = pd.to_datetime(df[column], errors="coerce")

                return eval(f"df_column {operator} operand")
            else:
                return pd.Series(True, df.index)

    def get_message(self, **kwargs):

        field_data = kwargs.get("field_data")
        check = kwargs.get("check_type")
        field_name = field_data.get("label")
        rule = {}
        for check_rule in field_data.get("rules"):
            if check_rule["type"] == check:
                rule = check_rule
                break
        operator = rule.get("operator")
        operand = rule.get("operand")

        return f"{field_name} must be {operator} than {operand}"
