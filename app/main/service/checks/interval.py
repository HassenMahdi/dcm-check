class Check():

    id = "INTERVAL_CHECK"
    name = "Interval Value Check"
    category = None
    description = None



    def check_column(self, df, column, *args, **kwargs):
        return df[column]