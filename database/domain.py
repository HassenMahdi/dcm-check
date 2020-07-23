from database.document import Document


class Domain(Document):
    __TABLE__ = "domains"

    name = None
    description = None
    identifier = None
    created_on = None
    modified_on = None
    super_domain_id = None
