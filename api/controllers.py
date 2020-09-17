#!/usr/bin/python
# -*- coding: utf-8 -*-

import time

import pandas as pd

from database.checker_document import CheckerDocument
from database.job_result_document import JobResultDocument
from database.modifier_document import ModifierDocument
from services.cleansing_service import run_checks, check_modifications
from api.utils.utils import create_check_metadata
from api.utils.storage import get_imported_data_df, get_mapped_df, get_check_results_df, save_mapped_df, \
                              save_check_results_df


def apply_mapping_transformation(df, mapping_id, target_fields):
    """Apply the mapping transformation in case of multiple source mapping"""

    checker_document = CheckerDocument()
    transformed_df = pd.DataFrame(columns=list(target_fields.keys()))
    mappings = checker_document.get_mappings(mapping_id)

    for target, source in mappings.items():
        data_type = target_fields[target]["type"]

        if len(source) > 1:
            if data_type == "string":
                transformed_df[target] = df[source[0]].str.cat(df[source[1:]].astype(str), sep=" ")
            else:
                transformed_df[target] = df[source].apply(pd.to_numeric, errors='coerce').sum(axis=1)
        else:
            transformed_df[target] = df[source[0]]

    return transformed_df


def start_check_job(job_id, file_id, worksheet_id, mapping_id, domain_id, is_transformed, modifications=None):
    """Starts the data check service"""

    if is_transformed:
        transformed_path = worksheet_id.split('/')
        file_id = transformed_path[-2]
        worksheet_id = transformed_path[-1]

    checker_document = CheckerDocument()
    modifier_document = ModifierDocument()
    target_fields = checker_document.get_all_target_fields(domain_id)

    start = time.time()
    if modifications:
        result_df = get_check_results_df(file_id, worksheet_id)
        
        rows_indices = set()
        for modification in modifications.values():
            rows_indices.update(map(int, modification.keys()))
        nrows = len(rows_indices)
        skiprows = set(range(1, max(rows_indices) + 1)) - set([index +1 for index in rows_indices])

        mapped_df = get_mapped_df(file_id, worksheet_id, nrows=nrows, skiprows=skiprows)
        mapped_df.index = rows_indices
        modifier_document.save_modifications(worksheet_id, modifications)
        modifier_document.apply_modifications(mapped_df, worksheet_id, rows_indices)
        data_check_result = create_check_metadata(result_df.reset_index(), job_id, worksheet_id, mapping_id, domain_id)
        result_df = check_modifications(mapped_df, result_df, target_fields, data_check_result, modifications, rows_indices)

        save_check_results_df(result_df, file_id, worksheet_id)
        print("end checks")
        print(time.time() - start)

    else:
        start = time.time()
        df = get_imported_data_df(file_id, worksheet_id, nrows=None, skiprows=None)
        final_df = apply_mapping_transformation(df, mapping_id, target_fields)
        modifier_document.apply_modifications(final_df, worksheet_id, is_all=True)
        save_mapped_df(final_df, file_id, worksheet_id)
        print("end mapping")
        print(time.time() - start)
        data_check_result = create_check_metadata(final_df.reset_index(), job_id, worksheet_id, mapping_id, domain_id)
        result_df = run_checks(final_df, target_fields,data_check_result)
        save_check_results_df(result_df, file_id, worksheet_id)
        print("end checks")
        print(time.time() - start)

    job_result_document = JobResultDocument()

    return job_result_document.save_check_job(data_check_result)


def read_exposures(base_url, file_id, worksheet_id, page, nrows, is_transformed, sort, filters):
    """Gets paginated data, filters and sorts data"""

    if is_transformed:
        transformed_path = worksheet_id.split('/')
        file_id = transformed_path[-2]
        worksheet_id = transformed_path[-1]

    if filters:
        operators = {"gt": ">", "lt": "<", "gte": ">=", "lte": "=<", "eq": "=="}
        mapped_df = get_mapped_df(file_id, workshhet_id, usecols=[column_filter["column"] for column_filter in filters])
        