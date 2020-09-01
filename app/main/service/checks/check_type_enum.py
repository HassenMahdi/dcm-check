#!/usr/bin/python
# -*- coding: utf-8 -*-

import enum

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


class CheckTypesEnum(enum.Enum):
    """Returns an enumeration of all possible check types"""

    Empty = EmptyChecker.check_code
    Type = TypeChecker.check_code
    Valid = ValidationChecker.check_code
    Ref = ReferenceChecker.check_code
    Format = FormatChecker.check_code
    Interval = IntervalChecker.check_code
    Min = MinChecker.check_code
    Max = MaxChecker.check_code
    MaxProperty = MaxPropertyChecker.check_code
    MinProperty = MinPropertyChecker.check_code
