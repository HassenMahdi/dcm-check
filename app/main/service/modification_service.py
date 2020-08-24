import traceback

from app.db.Models.modif_document import ModificationsDocument


class ModifierService:
    mdb = ModificationsDocument()

    def apply(self,modifications, df):
        """ search by filename worksheet and domain id """
        for key in modifications.columns.keys():
           for row_index, value in modifications.columns[key].items():
                df[key][int(row_index)]= value

        return df

    def applys(self, worksheet,domain_id, df):
        """ search by filename worksheet and domain id """
        modifications = self.mdb.get(worksheet,domain_id)
        if len(modifications.columns)>0:

                for key in modifications.columns.keys():
                    for row_index, value in modifications.columns[key].items():
                        df[key][int(row_index)] = value
                return df
        else:
            return df



    def save(self, worksheet,domain_id, modifications):

        m = self.mdb.get(worksheet, domain_id)

        m=self.update(m,modifications)
        self.mdb.save(m,_id=m._id)
        return m

    def get(self, worksheet, domain_id):
        return self.mdb.get(worksheet, domain_id)

    def update(self,m,modif):
        modif = self.transform_to_dict(modif)
        old_modif=m.columns.keys()
        for key in modif.keys():
            if key in old_modif:
                for row_index,value in modif[key].items():
                   m.columns[key][row_index]=value
            else:
                m.add_columns_modifications(key, modif[key])

        return m

    def transform_to_dict(self,modif):
        m={}
        for modif_per_column in modif["columns"]:
           column,modif= modif_per_column.items()
           m[column[1]]= modif[1]
        return m
