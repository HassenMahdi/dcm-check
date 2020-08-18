import pandas as pd
from app.main.service.checks.checker import Checker


class IntervalChecker(Checker):

    check_code = "INTERVAL_CHECK"
    check_name = "Interval Value Check"
    check_message = "The field %s is not in %s ."
    check_level = "errors"
    filter_name = "interval"



    def check_column(self, df, column, empty_column, *args, **kwargs):

        if not empty_column.all():
            min = kwargs.get("check").get("min")
            max = kwargs.get("check").get("max")
            df = pd.to_numeric(df[column], errors='coerce')
            conditions = (df <= float(max)) & (df>= float(min))
            return  conditions
        else:
            return empty_column
