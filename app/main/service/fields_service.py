import uuid
import datetime

import xlrd

from app.db.Models.domain import Domain
from app.db.Models.field import TargetField
from app.main import db
from app.main.model.user import User
from app.main.util.strings import camelCase


def save_field(data, domain_id):
    dom = Domain(**{'id': domain_id}).load()

    if dom.id:
        target_field = TargetField(**data).load(domain_id=domain_id)

        if not target_field.id:
            identifier = uuid.uuid4().hex.upper()
            new_dom = TargetField(
               **{
                    'id': identifier,
                    'created_on': datetime.datetime.utcnow(),
                    'name': camelCase(data['label'])
                })
            target_field = new_dom

        target_field.label = data['label']
        # target_field.name = data['name']
        target_field.description = data.get('description', None)
        # target_field.category = data.get('category', None)
        target_field.type = data['type']
        target_field.mandatory = data.get('mandatory', False)
        target_field.editable = data.get('editable', False)
        target_field.rules = data.get('rules', [])
        target_field.modified_on = datetime.datetime.utcnow()

        target_field.save(domain_id=domain_id)

    else:
        raise Exception(f'NO DOMAIN WITH ID {domain_id} FOUND')

    return target_field


def delete_field(data, domain_id):
    tf = TargetField(**data).delete(domain_id=domain_id)
    return tf


def get_all_fields(domain_id):
    return TargetField.get_all(domain_id = domain_id)


def fields_from_file(file, domain_id):

    col_field = {
        'MANDATORY': 'mandatory',
        'DESCRIPTION': 'description',
        'FIELD': 'label',
        'EDITABLE': 'editable',
        'TYPE': 'type'
    }

    wb = xlrd.open_workbook(file_contents=file.read())
    sh = wb.sheet_by_index(0)

    first_row = []  # The row where we stock the name of the column
    for col in range(sh.ncols):
        first_row.append(sh.cell_value(0, col))

    fields_data = []
    for row in range(1, sh.nrows):
        elm = {}
        for col in range(sh.ncols):
            data_field_name = col_field[first_row[col]]
            elm[data_field_name] = sh.cell_value(row, col)

        fields_data.append(elm)

    TargetField.drop(domain_id=domain_id)
    for data in fields_data:
        save_field(data, domain_id)

    return
