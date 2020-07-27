#!/usr/bin/python
# -*- coding: utf-8 -*-

import enum

from app.main.service.checks.empty_checker import EmptyChecker
from app.main.service.checks.format_checker import FormatChecker
from app.main.service.checks.type_checker import TypeChecker
from app.main.service.checks.reference_checker import ReferenceChecker
from app.main.service.checks.validation_checker import ValidationChecker


class CheckTypesEnum(enum.Enum):
    """Returns an enumeration of all possible check types"""

    Empty = EmptyChecker.check_code
    Type = TypeChecker.check_code
    Valid = ValidationChecker.check_code
    Ref = ReferenceChecker.check_code
    Format = FormatChecker.check_code
