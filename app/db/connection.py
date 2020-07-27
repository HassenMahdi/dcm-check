from flask_pymongo import PyMongo

mongo = PyMongo()


def get_next_sequence_value(sequence_name):
    sequence_document = mongo.db.counters.find_one_and_update({"_id": sequence_name}, {"$inc": {"sequence_value": 1}})

    if not sequence_document:
        sequence_document = mongo.db.counters.insert({"_id": sequence_name, "sequence_value": 1})

    return sequence_document["sequence_value"]
