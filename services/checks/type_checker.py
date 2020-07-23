#!/usr/bin/python
# -*- coding: utf-8 -*-

from services.checks.checker import Checker


class TypeChecker(Checker):
    check_code = "TYPE"
    check_name = "type"
    check_message = "The field %s is not correct %s value."
    check_level = "errors"
    filter_name = "type"

    def check_column(self, df, column, empty_column, *args, **kwargs):
        """Checks the data format for a given column"""

        field_type = kwargs.get("field_type", "string")

        rgx = self.get_regex(field_type)
        match = df[column].astype(str).str.match("^" + rgx + "$", case=False)
        if field_type =="string":
            return  match == False
        else:
            return  match

    def get_regex(self, field_type):
        """Returns a regex that represents the field type"""

        dict_format = {
            "double": "-?\d*(.?,?\d*)?",
            "boolean": "(yes)|(no)|(False)|(True)",
            "date": "(\d{2})/(\d{2})/(\d{4})$",
            "integer": "-?\d*"
        }

        return dict_format.get(field_type, "")

    def get_message(self, **kwargs):
        t = kwargs.get("type", "ERROR")
        p = kwargs.get("name", "ERROR")
        return self.check_message % (p, t)
