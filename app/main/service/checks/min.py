class Check():

    id = "MINIMUM_CHECK"
    name = "Minimum Value Check"
    category = None
    description = None

    def check_column(self, df, column, *args, **kwargs):
        return df[column]