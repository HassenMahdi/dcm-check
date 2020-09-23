#!/usr/bin/python
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod

from services.check.check_types_enum import  CheckTypesEnum
from services.check.checkers.type_checker import TypeChecker
from services.check.checkers.empty_checker import EmptyChecker
from services.check.checkers.format_checker import FormatChecker
from services.check.checkers.reference_checker import ReferenceChecker
from services.check.checkers.date_boundry_checker import DateBoundryChecker
from services.check.checkers.numeric_boundry_checker import NumericBoundryChecker
from services.check.checkers.property_boundry_checker import PropertyBoundryChecker


class CheckerFactory(ABC):
    """Helper class that provides a standard way to create a Data Checker using factory method"""

    @abstractmethod
    def _(self):
        pass

    @staticmethod
    def get_checker(check_code):
        """Factory method that returns a Data Checker based on the checkCode"""

        if check_code == CheckTypesEnum.Empty.value:
            return EmptyChecker()
        elif check_code == CheckTypesEnum.Type.value:
            return TypeChecker()
        elif check_code == CheckTypesEnum.Ref.value:
            return ReferenceChecker()
        elif check_code == CheckTypesEnum.Date_Boundry.value:
            return DateBoundryChecker()
        elif check_code == CheckTypesEnum.Format.value:
            return FormatChecker()
        elif check_code == CheckTypesEnum.Numeric_Boundry.value:
            return NumericBoundryChecker()
        elif check_code == CheckTypesEnum.Property_Boundry.value:
            return PropertyBoundryChecker()
        else:
            raise ValueError(check_code)
