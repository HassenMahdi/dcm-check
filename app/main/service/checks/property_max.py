import pandas as pd
from app.main.service.checks.checker import Checker

class MaxPropertyChecker(Checker):

    check_code = "MAXIMUM_PROPERTY_CHECK"
    check_name = "Maximum Property Value Check"
    check_message = "The field %s is not in %s ."
    check_level = "errors"
    filter_name = "max-property"


    def check_column(self, df, column, empty_column, *args, **kwargs):
        try:
            inclusive = kwargs.get("check").get("inclusive", False)
            limit_column = kwargs.get("check").get("property")
            limit_df = pd.to_numeric(df[limit_column], errors='coerce')
            column_values = pd.to_numeric(df[column], errors='coerce')
            if inclusive:
                return column_values <= limit_df
            else:
                return column_values < limit_df

        except:
            return empty_column
