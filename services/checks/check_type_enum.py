#!/usr/bin/python
# -*- coding: utf-8 -*-

import enum

from services.checks.empty_checker import EmptyChecker
from services.checks.format_checker import FormatChecker
from services.checks.type_checker import TypeChecker
from services.checks.reference_checker import ReferenceChecker
from services.checks.validation_checker import ValidationChecker


class CheckTypesEnum(enum.Enum):
    """Returns an enumeration of all possible check types"""

    Empty = EmptyChecker.check_code
    Type = TypeChecker.check_code
    Valid = ValidationChecker.check_code
    Ref = ReferenceChecker.check_code
    Format = FormatChecker.check_code
