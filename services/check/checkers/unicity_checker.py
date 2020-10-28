#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd

from services.check.checkers.checker import Checker


class UnicityChecker(Checker):

    check_code = "UNICITY_CHECK"
    check_name = "unicity"
    check_message = "The field cannot be duplicated."
    check_level = "errors"

    def check_column(self, df, column, empty_column, *args, **kwargs):
        """Checks if a given column is empty"""

        if not empty_column.any():
            lookup = kwargs.get("check").get("lookup")
            if lookup == "source":
                return pd.Series(df[column].is_unique, df.index)
