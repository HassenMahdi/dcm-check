import enum

from services.check.checkers.type_checker import TypeChecker
from services.check.checkers.empty_checker import EmptyChecker
from services.check.checkers.format_checker import FormatChecker
from services.check.checkers.unicity_checker import UnicityChecker
from services.check.checkers.reference_checker import ReferenceChecker
from services.check.checkers.date_boundry_checker import DateBoundryChecker
from services.check.checkers.numeric_boundry_checker import NumericBoundryChecker
from services.check.checkers.property_boundry_checker import PropertyBoundryChecker


class CheckTypesEnum(enum.Enum):
    """Returns an enumeration of all possible check types"""

    Type = TypeChecker.check_code
    Empty = EmptyChecker.check_code
    Ref = ReferenceChecker.check_code
    Format = FormatChecker.check_code
    Unicity = UnicityChecker.check_code
    Date_Boundry = DateBoundryChecker.check_code
    Numeric_Boundry = NumericBoundryChecker.check_code
    Property_Boundry = PropertyBoundryChecker.check_code

