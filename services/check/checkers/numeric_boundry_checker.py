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
            column = pd.to_numeric(df[column], errors='coerce')

            return pd.eval(f"column {operator} {operand}")
