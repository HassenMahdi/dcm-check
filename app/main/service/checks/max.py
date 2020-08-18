import pandas as pd
from app.main.service.checks.checker import Checker

class MaxChecker(Checker):

    check_code = "MAXIMUM_CHECK"
    check_name = "Maximum Value Check"
    check_message = "The field %s is not in %s ."
    check_level = "errors"
    filter_name = "max"





    def check_column(self, df, column, empty_column, *args, **kwargs):

        if not empty_column.all():
            max = kwargs.get("check").get("max")
            df = pd.to_numeric(df[column], errors='coerce')
            conditions = (df <= float(max))
            return  conditions
        else:
            return empty_column
