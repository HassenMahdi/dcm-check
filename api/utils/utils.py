#!/usr/bin/python
# -*- coding: utf-8 -*-

import uuid

from api.utils.storage import get_mapping_path
from api.utils.paginator import Paginator
from database.checker_document import CheckerDocument
from services.check.check_types_enum import CheckTypesEnum


def generate_id():
    return uuid.uuid4().hex.upper()


def create_check_metadata(df, job_id, file_id, worksheet_id, mapping_id, domain_id):
    """Creates the metadata dict for run checks function"""

    if not job_id:
        job_id = generate_id()
    
    data_check_result = {"jobId": job_id,
                         "fileId": file_id,
                         "worksheetId": worksheet_id,
                         "mappingId": mapping_id,
                         "domainId": domain_id,
                         "totalLocations": df.shape[0], 
                         "totalErrors": 0, 
                         "totalRowsInError": 0, 
                         "jobResult": {}}

    return data_check_result


def get_dataframe_page(file_id, worksheet_id, base_url, params, total_exposures, filtred, indices=None, sort=None):
    """Gets the dataframe page using Paginator class"""

    path = get_mapping_path(file_id, worksheet_id)
    paginator = Paginator(base_url=base_url, query_dict=params, page=int(params["page"]), limit=int(params["nrows"]))
    data = paginator.load_paginated_dataframe(path, total_exposures, worksheet_id, filtred, filter_indices=indices)
    if sort:
        data.index.name = "index"
        data = data.sort_values(by=[sort["column"], "index"], ascending=sort["order"])
        
    return paginator.get_paginated_response(data)


def replace_alias(cell, ref_values):
    for field_name, alias in ref_values.items():
        if cell in alias:
            return field_name
    return cell


def normalize_data(df, target_fields):
    """Normalizes data before starting the cleansing"""

    checker_document = CheckerDocument()
    for field_code, field_data in target_fields.items():
        ref_type_id = field_data.get("ref_type_id")
        if ref_type_id:
            conditions = {"ref_type_id": ref_type_id}
            ref_values = checker_document.get_ref_value(conditions, "code", alias=True)
            df[field_code] = df[field_code].apply(replace_alias, args=(ref_values,))
