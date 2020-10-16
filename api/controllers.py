#!/usr/bin/python
# -*- coding: utf-8 -*-

import time

import pandas as pd

<<<<<<< HEAD
=======
from database.user_document import UserDocument
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
from database.checker_document import CheckerDocument
from database.job_result_document import JobResultDocument
from database.modifier_document import ModifierDocument
from services.cleansing_service import run_checks, check_modifications
<<<<<<< HEAD
from services.dataframe import apply_filter, apply_sort
=======
from services.dataframe import apply_filter, apply_sort, apply_errors_filter
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
from api.utils.paginator import Paginator
from api.utils.utils import create_check_metadata, get_dataframe_page
from api.utils.storage import get_imported_data_df, get_mapped_df, get_check_results_df, save_mapped_df, \
                              save_check_results_df, get_mapping_path


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


<<<<<<< HEAD
def start_check_job(job_id, file_id, worksheet_id, mapping_id, domain_id, is_transformed, modifications=None):
=======
def start_check_job(job_id, file_id, worksheet_id, mapping_id, domain_id, is_transformed, user_id, modifications=None):
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
    """Starts the data check service"""

    if is_transformed:
        transformed_path = worksheet_id.split('/')
        file_id = transformed_path[-2]
        worksheet_id = transformed_path[-1]

<<<<<<< HEAD
    checker_document = CheckerDocument()
    modifier_document = ModifierDocument()
    target_fields = checker_document.get_all_target_fields(domain_id)

    start = time.time()
    if modifications:
        result_df = get_check_results_df(file_id, worksheet_id)
        
        rows_indices = set()
        
        rows_indices.update(map(int, modifications.keys()))
        print(rows_indices)
        nrows = len(rows_indices)
        skiprows = set(range(1, max(rows_indices) + 1)) - set([index +1 for index in rows_indices])

        mapped_df = get_mapped_df(file_id, worksheet_id, nrows=nrows, skiprows=skiprows)
        mapped_df.index = rows_indices
        modifier_document.save_modifications(worksheet_id, modifications)
        modifier_document.apply_modifications(mapped_df, worksheet_id, rows_indices)
=======
    keys = ["label", "type", "rules"]
    checker_document = CheckerDocument()
    modifier_document = ModifierDocument()
    target_fields = checker_document.get_all_target_fields(domain_id, keys)

    start = time.time()
    if modifications:
        rows_indices = set()
        rows_indices.update(map(int, modifications.keys()))
        nrows = len(rows_indices)
        skiprows = set(range(1, max(rows_indices) + 1)) - set([index +1 for index in rows_indices])

        result_df = get_check_results_df(file_id, worksheet_id)
        mapped_df = get_mapped_df(file_id, worksheet_id, nrows=nrows, skiprows=skiprows)
        mapped_df.index = rows_indices
        modifier_document.save_modifications(worksheet_id, modifications, user_id)
        modifier_document.apply_modifications(mapped_df, worksheet_id, list(rows_indices))
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
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


<<<<<<< HEAD
def read_exposures(base_url, file_id, worksheet_id, url_params, is_transformed, sort, filters):
=======
def read_exposures(base_url, file_id, worksheet_id, url_params, is_transformed, sort, filters, errors_filter):
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
    """Gets paginated data, filters and sorts data"""

    if is_transformed:
        transformed_path = worksheet_id.split('/')
        file_id = transformed_path[-2]
        worksheet_id = transformed_path[-1]

    indices = []
<<<<<<< HEAD
    sort_indices = []
    filter_indices = []
    checker_document = CheckerDocument()

    if filters:
        filter_indices = apply_filter(file_id, worksheet_id, filters)    

=======
    filtred = False
    sort_indices = []
    filter_indices = set()
    checker_document = CheckerDocument()

    if errors_filter:
        filter_indices = apply_errors_filter(file_id, worksheet_id, errors_filter)
        filtred = True
    if filters:
        indices = apply_filter(file_id, worksheet_id, filters)    
        filter_indices.update(indices)
        filtred = True
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
    if sort:
        sort_indices = apply_sort(file_id, worksheet_id, sort)
        if filter_indices:
            indices = [elem for elem in sort_indices if elem in filter_indices]
<<<<<<< HEAD
    indices = indices if indices else sort_indices or filter_indices
    total_lines = len(indices) if indices else checker_document.get_worksheet_length(worksheet_id)
    preview = get_dataframe_page(file_id, worksheet_id, base_url, url_params, total_lines, indices, sort)
=======
        filtred = True
    indices = indices if indices else sort_indices or list(filter_indices)
    total_lines = len(indices) if indices else checker_document.get_worksheet_length(worksheet_id)
    preview = get_dataframe_page(file_id, worksheet_id, base_url, url_params, total_lines, filtred, indices, sort)
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
    
    if preview.get("absolute_index"):
        preview["results"] = read_result(file_id, worksheet_id, preview["absolute_index"])
    else:
        preview["results"] = read_result(file_id, worksheet_id, preview["index"])
    
    return preview


def read_result(file_id, worksheet_id, index):
    """Reads the data check result file"""

    result = {}
    try:
        result_df = get_check_results_df(file_id, worksheet_id)
        result_df= result_df.iloc[index]
        result = {}    
        for column in result_df.columns.values:
            count = 0
            error = {}
<<<<<<< HEAD
            indexes = result_df.index[result_df[column] == True].tolist()
            check_type, field_code, error_type = eval(column)           
=======
            s_check_res=result_df[column]
            indexes = s_check_res[s_check_res].index
            check_type, field_code, error_type, check_index = eval(column)
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
            for index in result_df.index:
                if index in indexes:
                    target = result.setdefault(count, {})
                    target = target.setdefault(field_code, {})
                    target = target.setdefault(error_type, [])
                    target.append(check_type)
                else:
                    result.setdefault(count, {})
                count = count + 1

        return result

    except pd.errors.EmptyDataError:
        return result


<<<<<<< HEAD
def get_check_modifications(worksheet_id):
    """Fetches the check modification data"""

    audit_trial = {}
    modifier_document = ModifierDocument()

    modified_data = modifier_document.get_modifications(worksheet_id, is_all=True)
    for modification in modified_data:
        for column, col_modif in modification["columns"].items():
            line_modif = {"previous": col_modif["previous"], "new": col_modif["new"]}
            if audit_trial.get(column):
                audit_trial[column][modification["line"]] = line_modif
            else:
                audit_trial[column] = {}
                audit_trial[column][modification["line"]] = line_modif 

    return audit_trial
=======
def get_check_modifications(worksheet_id, domain_id):
    """Fetches the check modification data"""

    audit_trial = {}
    user_document = UserDocument()
    checker_document = CheckerDocument()
    modifier_document = ModifierDocument()

    target_fields = checker_document.get_all_target_fields(domain_id, ["label"])
    modified_data = modifier_document.get_modifications(worksheet_id, is_all=True)
    for modification in modified_data:
        for column, col_modif in modification["columns"].items():
            label = target_fields.get(column)["label"]
            line_modif = {"previous": col_modif["previous"][-1], "new": col_modif["new"], 
                          "updated_at": col_modif["updatedAt"],
                          "user": user_document.get_user_fullname(col_modif["userId"])}
            if audit_trial.get(label):
                audit_trial[label][modification["line"]] = line_modif
            else:
                audit_trial[label] = {}
                audit_trial[label][modification["line"]] = line_modif

    return audit_trial
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
