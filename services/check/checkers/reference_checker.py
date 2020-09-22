#!/usr/bin/python
# -*- coding: utf-8 -*-

from database.checker_document import CheckerDocument
from services.check.checkers.checker import Checker


class ReferenceChecker(Checker):
    check_code = "REFERENCE_CHECK"
    check_name = "reference"
    check_message = "The field %s is not in the %s reference"
    check_level = "errors"
    filter_name = "reference"

    def check_column(self, df, column, empty_column, *args, **kwargs):
        """Checks if a given column matches the business requirements"""

        if not empty_column.all():
            reference = kwargs.get("check").get("ref")
            list_values = reference.get("values")
            ref_collection = reference.get("collection")
            field_name = reference.get("fieldName")
            condition = reference.get("conditions", {})

            if list_values:
                return empty_column | df[column].isin(list_values)
            else:
                checker_document = CheckerDocument()
                ref_values = checker_document.get_ref_value(ref_collection, field_name, condition)
                return empty_column | df[column].isin(ref_values)