from database.connectors import mongo
from database.document import Document

class TargetField(Document):

    __TABLE__ = "fields"

    def db(self, **kwargs):
        domain_id = kwargs['domain_id']
        return mongo.db[f"{domain_id}.{self.__TABLE__}"]

    name = None
    description = None
    label = None
    created_on = None
    modified_on = None
    type = None
    category = None
    rules = None
    editable = None
    mandatory = None