#!/usr/bin/python
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod

from app.main.service.checks.check_type_enum import  CheckTypesEnum
from app.main.service.checks.empty_checker import EmptyChecker
from app.main.service.checks.format_checker import FormatChecker
from app.main.service.checks.property_max import MaxPropertyChecker
from app.main.service.checks.property_min import MinPropertyChecker
from app.main.service.checks.type_checker import TypeChecker
from app.main.service.checks.reference_checker import ReferenceChecker
from app.main.service.checks.validation_checker import ValidationChecker
from app.main.service.checks.interval import IntervalChecker
from app.main.service.checks.max import MaxChecker
from app.main.service.checks.min import MinChecker



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
        elif check_code == CheckTypesEnum.Interval.value:
            return IntervalChecker()
        elif check_code == CheckTypesEnum.Max.value:
            return MaxChecker()
        elif check_code == CheckTypesEnum.Min.value:
            return MinChecker()
        elif check_code == CheckTypesEnum.MaxProperty.value:
            return MaxPropertyChecker()
        elif check_code == CheckTypesEnum.MinProperty.value:
            return MinPropertyChecker()
        else:
            raise ValueError(check_code)
