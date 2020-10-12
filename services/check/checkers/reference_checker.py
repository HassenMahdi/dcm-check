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
            check = kwargs.get("check")
            list_values = check.get("values")
            if list_values:
                return empty_column | df[column].isin(list_values)
            
            field_name = check.get("field_name")
            conditions = check.get("conditions", {})
            checker_document = CheckerDocument()
            ref_values = checker_document.get_ref_value(conditions, field_name)

            return empty_column | df[column].str.lower().isin(ref_values)