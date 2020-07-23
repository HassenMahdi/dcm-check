from database.connectors import get_next_sequence_value, mongo


class Document:

    __TABLE__ = None
    __ID_COUNTER__ = None

    _id = None

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    # @property
    def db(self, **kwargs):
        return mongo.db[self.__TABLE__]

    def save(self , **kwargs):
        self._id = self._id
                   # or get_next_sequence_value(self.__ID_COUNTER__)
        self._id = self.db(**kwargs).save(self.to_dict())
        return self

    def load(self, query=None, **kwargs):
        if not query:
            query = {'_id': self._id}
        self.from_dict(self.db(**kwargs).find_one(query))
        return self

    def to_dict(self):
        return self.__dict__

    def from_dict(self, d):
        if d:
            self.__dict__ = d
        else:
            self.id = None
        return self

    @classmethod
    def get_all(cls, **kwargs):
        return [cls(**r) for r in cls().db(**kwargs).find({})]
