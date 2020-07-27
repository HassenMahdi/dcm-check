from app.datacheck import CheckAbstract, CheckParam


class Check(CheckAbstract):

    id = "MAXIMUM_CHECK"
    name = "Maximum Value Check"
    category = None
    description = None

    parameters = [
        CheckParam('max', label='Max')
    ]

    def check_column(self, df, column, *args, **kwargs):
        return df[column]