from app.datacheck import CheckAbstract, CheckParam


class Check(CheckAbstract):

    id = "MINIMUM_CHECK"
    name = "Minimum Value Check"
    category = None
    description = None

    parameters = [
        CheckParam('min', label='Min')
    ]

    def check_column(self, df, column, *args, **kwargs):
        return df[column]