from app.db.document import Document


class SuperDomain(Document):
    __TABLE__ = "super-domains"

    name = None
    description = None
    identifier = None
    created_on = None
    modified_on = None

