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
<<<<<<< HEAD
            column = pd.to_datetime(df[column], errors="coerce")
            
            return pd.eval(f"column {operator} operand")
=======

            if operand in df.columns:
                df_column = pd.to_datetime(df[column], errors="coerce")

                return eval(f"df_column {operator} operand")
            else:
                return pd.Series(True, df.index)
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
