import pandas as pd
from app.main.service.checks.checker import Checker

class MinPropertyChecker(Checker):

    check_code = "MINIMUM_PROPERTY_CHECK"
    check_name = "Minimum Property Value Check"
    check_message = "The field %s is not in %s ."
    check_level = "errors"
    filter_name = "min-property"


    def check_column(self, df, column, empty_column, *args, **kwargs):
        try:
            inclusive = kwargs.get("check").get("inclusive", False)
            limit_column = kwargs.get("check").get("property")
            limit_df = pd.to_numeric(df[limit_column], errors='coerce')
            column_values = pd.to_numeric(df[column], errors='coerce')
            if inclusive:
                return  limit_df <= column_values
            else:
                return limit_df < column_values

        except:
            return empty_column
