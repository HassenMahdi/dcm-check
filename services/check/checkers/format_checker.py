#!/usr/bin/python
# -*- coding: utf-8 -*-

from services.check.checkers.checker import Checker


class FormatChecker(Checker):
    check_code = "FORMAT"
    check_name = "format"
    check_message = "The field %s is not correct %s value."
    check_level = "errors"

    def check_column(self, df, column, empty_column, *args, **kwargs):
        """Checks the data format for a given column"""
        
        if not empty_column.all():
            format_check = kwargs.get("check")

            valid_format = format_check.get("regex")
            if valid_format:
                return empty_column | (df[column].astype(str).str.match(valid_format))