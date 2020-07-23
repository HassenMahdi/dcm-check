#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd

from services.checks.checker import Checker


class ValidationChecker(Checker):
    check_code = "VALID"
    check_name = "valid"
    check_message = "The field %s is does not respect the rule %s"
    check_level = "errors"
    filter_name = "VALID"

    def check_column(self, df, column, empty_column, *args, **kwargs):
        """Checks if a given column matches the business requirements"""
        if not empty_column.all():
            validation_check = kwargs.get("check")
            field_type = kwargs.get("field_type")

            if field_type == "string":
                max_length = validation_check["properties"].get("maxLength", 0)
                min_length = validation_check["properties"].get("minLength", 0)
                return empty_column | ((df[column].astype(str).str.len() >= min_length) & (df[column].astype(str).str.len() <= max_length))
            else:
                max_value = validation_check["properties"].get("maxValue", float("inf"))
                min_value = validation_check["properties"].get("minValue", float("-inf"))
                df[column] = pd.to_numeric(df[column].replace(",", ".", regex=True))
                return empty_column | \
                       ((pd.to_numeric(df[column]) >= min_value) & (pd.to_numeric(df[column]) <= max_value))
