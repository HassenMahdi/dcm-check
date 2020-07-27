import uuid
import datetime

from app.db.Models.domain import Domain
from app.db.Models.field import TargetField
from app.db.Models.super_domain import SuperDomain
from app.main.service.doms_service import get_all_domains, get_domains_by_super_id


def save_super_domain(data):
    dom = SuperDomain(**data).load()
    if not dom.id:
        identifier = uuid.uuid4().hex.upper()
        new_dom = SuperDomain(
            **{**data, **{
                'id': identifier,
                'identifier': identifier,
                'created_on': datetime.datetime.utcnow()

            }})
        #     CREATE NEW TABLES HERE
        dom = new_dom
    else:
        dom.name = data['name']
        dom.description = data['description']

    dom.save()

    return dom


def delete_super_domain(data):
    super_dom = SuperDomain(**data).load()
    if super_dom.id:
        dms = Domain.get_all(query={'super_domain_id':super_dom.id})
        dm: Domain
        for dm in dms:
            TargetField.drop(domain_id=dm.id)
            dm.delete()

        super_dom.delete()

    return super_dom


def get_all_super_domains():
    return SuperDomain.get_all()
