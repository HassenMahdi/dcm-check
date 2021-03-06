#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd

from services.check.checkers.checker import Checker


class NumericBoundryChecker(Checker):

    check_code = "NUMERIC_BOUNDRY_CHECK"
    check_name = "Numeric Boundry Value Check"
    check_message = "The field %s is not in %s ."
    check_level = "errors"

    def check_column(self, df, column, empty_column, *args, **kwargs):
        """Checks if a column is greater then numeric value"""

        if not empty_column.all():
            check = kwargs.get("check")
            operand = check.get("operand")
            operator = check.get("operator")
            df_column = pd.to_numeric(df[column], errors='coerce')

            return eval(f"df_column {operator} {operand}")

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
