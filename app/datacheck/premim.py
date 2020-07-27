from app.datacheck import CheckAbstract, CheckParam


class Check(CheckAbstract):

    id = "PREMIUM_CHECK"
    name = "Premium Domain Check"
    category = None
    description = None

    parameters = []

    def check_column(self, df, column, *args, **kwargs):
        return df[column]