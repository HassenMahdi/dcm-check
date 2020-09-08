from app.db.connection import mongo
from app.main.util.strings import generate_id


class ModificationsDocument:
    _id = None

    def get(self,worksheet,domain_id):
        m = Modifications(mongo.db.modifications.find_one({'worksheet_id': worksheet}), worksheet=worksheet,domain_id=domain_id)
        return m

    def save(self, m, **kwargs):
        document= m.to_dict()
        document['_id'] = document.get('_id', None) or generate_id()
        return mongo.db.modifications.save(document)


class Modifications:
    worksheet_id = None
    domain_id = None
    _id = None
    columns = {}

    def __init__(self, obj, worksheet=None,domain_id=None):
        obj = obj or {}
        self.worksheet_id = obj.get("worksheet_id", None) or worksheet
        self.domain_id=domain_id
        self.columns = obj.get("columns", None) or self.columns
        self._id = obj.get("_id", None) or None

    def to_dict(self):
        return {
            "worksheet_id": self.worksheet_id,
            "domain_id":self.domain_id,
            "columns": self.columns,
            "_id":self._id
        }

    def add_columns_modifications(self,column, modifications):
        self.columns[column]=modifications
