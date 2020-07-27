from app.db.document import Document


class Mappings(Document):
    __TABLE__ = "data_check_jobs"
    mappingId = None
    jobResult = None
    totalErrors = None
    uniqueErrorLines = None
