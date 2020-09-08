import uuid


def camelCase(st):
    output = ''.join(x for x in st.title() if x.isalnum())
    return output[0].lower() + output[1:]


def generate_id():
    return uuid.uuid4().hex.upper()