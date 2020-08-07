#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import time

import pandas as pd

from app.main.service.filter_service import update_table
from app.main.service.modification_service import ModifierService
from app.main.util.paginator import Paginator
from app.db.Models.modifier_document import ModifierDocument
from app.main.service.cleansing_service import run_checks, check_modifications
from  app.db.Models.checker_documents import CheckerDocument, JobResultDocument
from app.main.util.storage import get_mapped_df, get_imported_data_df, save_mapped_df, save_check_results_df, \
    get_check_results_df, get_mapping_path, get_results_path, get_dataframe_from_csv
from app.main.service.dataframe import apply_check_modifications, calculate_field


def trim_fraction(column):
    if '.' in column:
        return column[:column.rfind('.')]
    return column


def apply_mapping_transformation(df, params, target_fields):
    """Apply the mapping transformation in case of multiple source mapping"""

    checker_document = CheckerDocument()
    target_fields_names=[key for key in target_fields.keys()]
    transformed_df = pd.DataFrame(columns=target_fields_names)
    default_values = checker_document.get_default_values(params["domain_id"])
    mappings = checker_document.get_mappings(params["worksheet_id"], params["domain_id"])

    for target, source in mappings.items():
        data_type = target_fields[target]["type"]
        remove_digits = target_fields[target].get("skipDecimalDigits",None)
        if len(source) > 1:
            if data_type == "string":
                transformed_df[target] = df[source[0]].str.cat(df[source[1:]].astype(str), sep=" ")
            else:
                transformed_df[target] = df[source].apply(pd.to_numeric, errors='coerce').sum(axis=1)
        else:
            transformed_df[target] = df[source[0]]
        if default_values.get(target):
            transformed_df[target] = transformed_df[target].fillna("").replace("", default_values[target])
            del default_values[target]
        if remove_digits:
            transformed_df[target] = transformed_df[target].astype(str).apply(trim_fraction)
        if data_type in ["double", "int"]:
            transformed_df[target] = df[source].apply(pd.to_numeric, errors='coerce')
    if "pd_value" in mappings:
        transformed_df["pd_split"] = "No"
    else:
        transformed_df["pd_split"] = "Yes"
    condition = [{"property": "pd_split", "value": transformed_df.loc[0]["pd_split"]}]
    #formulas = checker_document.get_calculated_fields_formula(params["lob_id"], condition, mappings)

    #for field_code, formula in formulas.items():
    #   transformed_df[field_code] = calculate_field(transformed_df, formula, mappings, formulas)

    if default_values:
        for target_field, default_value in default_values.items():
            transformed_df[target_field] = transformed_df[target_field].fillna("").replace("", default_value)

    return transformed_df


def start_check_job(params, modifications={}):
    """Starts the data check service"""


    checker_document = CheckerDocument()
    modifier=ModifierService()
    #TODO: get target fileds by domain and categories
    target_fields = checker_document.get_all_target_fields(params["domain_id"])

    start = time.time()
    if modifications and len(modifications.get("columns",[]))>0:
        rows_indexes=set()
        modifs = modifier.save(params["worksheet"], params["domain_id"], modifications)
        result_df = get_check_results_df(params["filename"], params["worksheet"])
        columns=modifs.columns.keys()
        for key in columns:
            rows_indexes.update(set(map(lambda x:int(x) , modifs.columns[key])))
        rows_indexes=list(rows_indexes)
        rows_indexes.sort()
        nrows = len(rows_indexes)
        skiprows =list(set(range(1, max(rows_indexes) + 1)) -
                       set([index +1 for index in rows_indexes]))
        mapped_df = get_mapped_df(params["filename"], params["worksheet"], skiprows=skiprows, nrows=nrows)
        mapped_df.index =rows_indexes
        #Todo: load only modif column and apply only for n rows
        final_df = modifier.apply(params["worksheet"], params["domain_id"],modifs,mapped_df)
        data_check_result, result_df = check_modifications(final_df, rows_indexes, params, target_fields, result_df,
                                                               modifs)
        save_check_results_df(result_df, params["filename"], params["worksheet"])
        print("end checks")
        print(time.time() - start)
        print(result_df.shape)
        job_result_document = JobResultDocument()

        return job_result_document.save_check_job(data_check_result)
    else:
        start = time.time()
        df = get_imported_data_df(params["filename"], params["worksheet"], nrows=None, skiprows=None)

        modifier_document = ModifierDocument()
        df = modifier_document.load_import_modifications(df, params["worksheet_id"])
        final_df = apply_mapping_transformation(df, params, target_fields)
        final_df = modifier_document.load_top_panel(final_df, params["worksheet_id"])
        save_mapped_df(final_df, params["filename"], params["worksheet"])
        print("end mapping")
        print(time.time() - start)
        data_check_result, result_df = run_checks(final_df, params, target_fields)
        #print(result_df.shape)
        save_check_results_df(result_df, params["filename"], params["worksheet"])
        print("end checks")
        print(time.time() - start)
        job_result_document = JobResultDocument()

        return job_result_document.save_check_job(data_check_result)


def read_exposures(request, params,filter_sort):
    """Reads the mapped data csv file"""

    sort = []
    filter = ""
    checker_document = CheckerDocument()
    base_url = request.base_url
    paginator = Paginator(base_url=base_url, query_dict=params, page=int(params["page"]), limit=int(params["nrows"]))
    path = get_mapping_path(params["filename"], params["worksheet"])
    if filter_sort:
        sort =filter_sort["sort"]
        filter = filter_sort["filter"]


    data = update_table(params,path,params["page"], params["nrows"], sort, filter)
    headers = paginator.load_headers(path)
    lables=checker_document.get_target_fields(params["domain_id"], query={"name": {"$in": headers}})
    lables = list(map(lambda x: {"field":x["name"], "headerName":x["label"],"type":x["type"]},lables))
    data["row_index"] = data.index.astype(int)
    check_results = read_result(params, data )
    exposures = paginator.get_paginated_response(data,lables,check_results)


    return exposures


def read_results(params):
    """Reads the data check result file"""

    offset = (int(params["page"])-1) * params["nrows"]
    end = offset + params["nrows"] - 1
    check_results = {"count": 0, "errors": {}, "warnings": {}}

    try:
        df = get_check_results_df(params["filename"], params["worksheet"]).loc[offset:end]
        check_results["count"] = df.shape[0]
        result = {}
        for column in df.columns.values:
            check_type, field_code, error_type = eval(column)
            check_results[error_type][field_code] = {}
            check_results[error_type][field_code][check_type] = df.index[df[column] == 'True'].tolist()

            error = {}
            indexes = df.index[df[column] == 'True']
            for index in indexes:

                target = result.setdefault(index, {})
                target = target.setdefault(field_code, {})
                target = target.setdefault(error_type, [])
                target.append(check_type)



        return result
    except pd.errors.EmptyDataError:
        return check_results

def read_column(params):
    """Reads longitude and latitude columns"""

    mapped_df = get_mapped_df(params["filename"], params["worksheet"], usecols=[params["column"]])
    if params["unique"]:
        column_data = mapped_df[params["column"]].unique().tolist()
    else:
        column_data = mapped_df[params["column"]].tolist()
    return {"column": column_data}

def read_result(params,data):
    """Reads the data check result file"""
    sort = []
    filter = ""
    check_results = {"count": 0, "errors": {}, "warnings": {}}

    try:
        path = get_results_path(params["filename"], params["worksheet"], as_folder=False, create=True)

        dff = pd.read_csv(path, engine="c", dtype=str, skipinitialspace=True, na_filter=False, delimiter=";")

        df= dff.iloc[data.index]

        check_results["count"] = df.shape[0]
        result = {}
        for column in df.columns.values:
            check_type, field_code, error_type = eval(column)
            check_results[error_type][field_code] = {}
            check_results[error_type][field_code][check_type] = df.index[df[column] == 'True'].tolist()

            error = {}
            indexes = df.index[df[column] == 'True']
            count = 0
            for index in indexes:
                target = result.setdefault(count, {})
                target = target.setdefault(field_code, {})
                target = target.setdefault(error_type, [])
                target.append(check_type)
                count = count + 1


        return result
    except pd.errors.EmptyDataError:
        return check_results

def to_float(column):

    """Converts a column of type np numeric to float"""
    return float(column)

