class ModificationsDocument:

    def get(self, jobId):
        m = Modifications(mongo.db.modifications.find_one({'_id': int(jobId)}), job_id=jobId)
        return m

    def save(self, m, **kwargs):
        return mongo.db.modifications.save(m.to_dict())


class Modifications:
    _id = None
    line_modofications = []

    def __init__(self, obj, job_id=None):
        obj = obj or {}
        self._id = obj.get("_id") or job_id
        self.line_modofications = obj.get("line_modofications") or self.line_modofications

    def to_dict(self):
        return {
            "_id": self._id,
            "line_modofications": self.line_modofications
        }

    def add_line_modify(self, row, col, value):
        self.line_modofications.append({
            "cell": {
                "column": col,
                "row": row
            },
            "value": value
        })
