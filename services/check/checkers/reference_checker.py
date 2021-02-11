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
            conditions = {"ref_type_id": kwargs.get("ref_type_id")}
            conditions.update(check.get("conditions", {}))
            checker_document = CheckerDocument()
            ref_values = checker_document.get_ref_value(conditions, field_name)

            return empty_column | df[column].str.lower().isin({ref_value.lower() for ref_value in ref_values})

    def get_message(self, **kwargs):

        field_data = kwargs.get("field_data")
        rule = {}
        check = kwargs.get("check_type")
        field_name = field_data.get("label")
        ref_type_id = field_data.get("ref_type_id")
        for check_rule in field_data.get("rules"):
            if check_rule["type"] == check:
                rule = check_rule
                break
        checker_document = CheckerDocument()
        reference_type = checker_document.get_reference_type(ref_type_id)

        return f"{field_name} must be {reference_type['label']} ({reference_type['version']})"

    def get_ref_list(self, check, ref_type_id):
        """Fetches the references values list from the database"""

        list_values = check.get("values")
        if list_values:
            return list_values

        field_name = check.get("field_name")
        conditions = {"ref_type_id": ref_type_id}
        conditions.update(check.get("conditions", {}))
        checker_document = CheckerDocument()
        ref_values = checker_document.get_ref_value(conditions, field_name)

        return ref_values
