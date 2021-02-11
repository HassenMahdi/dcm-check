#!/usr/bin/python
# -*- coding: utf-8 -*-

from services.check.checkers.checker import Checker


class EmptyChecker(Checker):

    check_code = "EMPTY_CHECK"
    check_name = "empty"
    check_message = "The field cannot be empty."
    check_level = "errors"

    def check_column(self, df, column, empty_column, *args, **kwargs):
        """Checks if a given column is empty"""

        # if empty_column.any():
        related_fields = kwargs.get("check").get("rel")
        if related_fields:
            related_df = kwargs.get("empty_df")[related_fields]
            for related_field in related_fields:
                empty_column = empty_column & ~related_df[related_field]

        return empty_column == False

    def get_message(self, **kwargs):

        field_name = kwargs.get("field_data").get("label")

        return f"{field_name} can not be empty"
