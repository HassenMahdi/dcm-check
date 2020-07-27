from app.datacheck import CheckAbstract, CheckParam


class Check(CheckAbstract):

    id = "INTERVAL_CHECK"
    name = "Interval Value Check"
    category = None
    description = None

    parameters = [
        CheckParam('max', label='Max'),
        CheckParam('min', label='Min')
    ]

    def check_column(self, df, column, *args, **kwargs):
        return df[column]