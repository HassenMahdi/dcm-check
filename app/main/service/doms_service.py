import uuid
import datetime

from app.db.Models.domain import Domain
from app.db.Models.field import TargetField
from app.db.Models.super_domain import SuperDomain


def save_domain(data):
    super_domain_id = data['super_domain_id']
    super_dom = SuperDomain(**{'id':super_domain_id}).load()

    if super_dom.id:
        dom = Domain(**data).load()
        if not dom.id:
            identifier = uuid.uuid4().hex.upper()

            new_dom = Domain(
                **{**data, **{
                    'id': identifier,
                    'identifier': identifier,
                    'created_on': datetime.datetime.utcnow(),
                    'super_domain_id': super_domain_id

                }})
            #     CREATE NEW TABLES HERE
            dom = new_dom
        else:
            dom.name = data['name']
            dom.description = data['description']

        dom.save()
    else:
        raise Exception(f'NO SUPER DOMAIN WITH ID {super_domain_id} FOUND')

    return dom


def delete_domain(data):
    super_domain_id = data['super_domain_id']
    super_dom = SuperDomain(**{'id':super_domain_id}).load()

    if super_dom.id:
        dom = Domain(**data).delete()
        TargetField.drop(domain_id=dom.id)
    else:
        raise Exception(f'NO SUPER DOMAIN WITH ID {super_domain_id} FOUND')

    return dom


def get_all_domains():
    return Domain.get_all()


def get_domains_by_super_id(super_id):
    return Domain.get_all(query={'super_domain_id':super_id})
