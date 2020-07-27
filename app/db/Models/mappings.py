from app.db.document import Document


class Mappings(Document):
    __TABLE__ = "mappings"

    mappingId = None
    fileId = None
    rules = None
    domainId = None
