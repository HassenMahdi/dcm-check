#!/usr/bin/python
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod

from services.checks.check_type_enum import CheckTypesEnum
from services.checks.empty_checker import EmptyChecker
from services.checks.format_checker import FormatChecker
from services.checks.type_checker import TypeChecker
from services.checks.reference_checker import ReferenceChecker
from services.checks.validation_checker import ValidationChecker


class CheckerFactory(ABC):
    """Helper class that provides a standard way to create a Data Checker using factory method"""

    @abstractmethod
    def _(self):
        pass

    @staticmethod
    def get_checker(check_code):
        """Factory method that returns a Data Checker based on the checkCode"""

        if check_code == CheckTypesEnum.Empty.value or check_code == "NOTNULL":
            return EmptyChecker()
        elif check_code == CheckTypesEnum.Type.value:
            return TypeChecker()
        elif check_code == CheckTypesEnum.Valid.value:
            return ValidationChecker()
        elif check_code == CheckTypesEnum.Ref.value:
            return ReferenceChecker()
        elif check_code == CheckTypesEnum.Format.value:
            return FormatChecker()
        else:
            raise ValueError(check_code)
