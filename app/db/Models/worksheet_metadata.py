from app.db.document import Document


class Mappings(Document):
    __TABLE__ = "worksheet_metadata"

    fileId = None
    file_name = None
    worksheetId = None
    worksheet_name = None
    domainId = None
    totalExposures = None
    columns = None
