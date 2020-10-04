#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd

from services.check.checkers.checker import Checker


class PropertyBoundryChecker(Checker):

    check_code = "PROPERTY_BOUNDRY_CHECK"
    check_name = "Property Boundry Value Check"
    check_message = "The field %s is not in %s ."
    check_level = "errors"

    def check_column(self, df, column, empty_column, *args, **kwargs):
        """Checks if a column is greater then an other column"""

        if not empty_column.all():
            check = kwargs.get("check")
            operator = check.get("operator")
            boundry = check.get("property")

            if boundry in df.columns:
                boundry_column = pd.to_numeric(df[boundry], errors='coerce')
                column = pd.to_numeric(df[column], errors='coerce')

                return pd.eval(f"column {operator} boundry_column")
            else:
                return pd.Series(True, df.index)
