import traceback


class ModifierService:
 #   mdb = ModificationsDocument()

    def apply(self, filename, df):
        modofications = self.mdb.get(filename)

        for m in modofications.line_modofications:
            try:
                row = m["cell"]["row"]
                column = m["cell"]["column"]
                value = m["value"]
                s = df["rowID"]
                c = s == row
                index = s[c].index

                if column in df.columns.values and len(index) > 0:
                    df.loc[index, column] = value

            except Exception:
                traceback.print_exc()

        return df

    def save(self, filename, modofications):

        m = self.mdb.get(filename)

        for line_id, modif_per_line in modofications.items():
            for v in modif_per_line:
                column = v.get("column")
                value = v.get("newValue")
                m.add_line_modify(line_id, column, value)

        self.mdb.save(m)