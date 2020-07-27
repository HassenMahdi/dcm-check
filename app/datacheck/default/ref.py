from app.datacheck import CheckAbstract, CheckParam


class Check(CheckAbstract):

    id = "REFERENCE_CHECK"
    name = "Reference Value Check"
    category = None
    description = None

    parameters = [
        CheckParam('reference_type', label='Type')
    ]

    def check_column(self, df, column, *args, **kwargs):
        return df[column]