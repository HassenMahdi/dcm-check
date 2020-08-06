from app.db.connection import mongo

class ModificationsDocument:

    def get(self,worksheet,domain_id):
        m = Modifications(mongo.db.modifications.find_one({'_id': worksheet,'domain_id':domain_id}), worksheet=worksheet,domain_id=domain_id)
        return m

    def save(self, m, **kwargs):
        return mongo.db.modifications.save(m.to_dict())


class Modifications:
    _id = None
    domain_id=None
    columns = {}

    def __init__(self, obj, worksheet=None,domain_id=None):
        obj = obj or {}
        self._id = obj.get("_id") or worksheet
        self.domain_id=domain_id
        self.columns = obj.get("columns") or self.columns

    def to_dict(self):
        return {
            "_id": self._id,
            "domain_id":self.domain_id,
            "columns": self.columns
        }

    def add_columns_modifications(self,column, modifications):
        self.columns[column]=modifications
