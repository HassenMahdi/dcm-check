from app.datacheck import CheckAbstract, CheckParam


class Check(CheckAbstract):

    id = "EMPTY_CHECK"
    name = "Empty Cell Check"
    category = None
    description = None

    def check_column(self, df, column, *args, **kwargs):
        return df[column]