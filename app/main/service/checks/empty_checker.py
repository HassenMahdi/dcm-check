#!/usr/bin/python
# -*- coding: utf-8 -*-

from app.main.service.checks.checker import Checker


class EmptyChecker(Checker):

    check_code = "NOTEMPTY"
    check_name = "empty"
    check_message = "The field cannot be empty."
    check_level = "errors"
    filter_name = "EMPTY_CELL"

    def check_column(self, df, column, empty_column, *args, **kwargs):
        """Checks if a given column is empty"""
        if empty_column.any():
            related_fields = kwargs.get("check").get("rel")
            if related_fields:
                related_df = kwargs.get("empty_df")[related_fields]
                for related_field in related_fields:
                    empty_column = empty_column & ~related_df[related_field]

            return empty_column == False
