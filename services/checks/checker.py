#!/usr/bin/python
# -*- coding: utf-8 -*-

from abc import abstractmethod


class Checker:

    check_code = None
    check_name = None
    check_message = None
    check_level = None
    filter_name = None

    def run(self, df, column, empty_column, *args, **kwargs):
        """Applies the data check rules to each column of the dataframe"""

        check = self.check_column(df, column, empty_column, *args, **kwargs)
        if check is not None:
            condition = check == False
            if condition.any():
                return condition

    @abstractmethod
    def check_column(self, df, column, empty_column, *args, **kwargs):
        pass

    def get_message(self, **kwargs):
        """Gets the check message"""

        return self.check_message
