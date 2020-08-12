class Check():

    id = "MAXIMUM_CHECK"
    name = "Maximum Value Check"
    category = None
    description = None


    def check_column(self, df, column, *args, **kwargs):
        return df[column]