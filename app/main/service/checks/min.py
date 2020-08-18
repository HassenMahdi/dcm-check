import pandas as pd
from app.main.service.checks.checker import Checker

class MinChecker(Checker):

    check_code = "MINIMUM_CHECK"
    check_name = "Minimum Value Chec"
    check_message = "The field %s is not in %s ."
    check_level = "errors"
    filter_name = "min"

    def check_column(self, df, column, empty_column, *args, **kwargs):

        if not empty_column.all():
            min = kwargs.get("check").get("min")
            df = pd.to_numeric(df[column], errors='coerce')
            conditions = (df>= float(min))
            return  conditions
        else:
            return empty_column
