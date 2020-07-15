#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

from database.checker_documents import JobResultDocument
from services.checks.checker_factory import CheckerFactory
from services.dataframe import extend_result_df


def run_checks(final_df, params, target_fields, metadata=True):
    """Runs all the checks defined for all targets in a given mappings document"""

    total_errors_lines = 0
    result_df = pd.DataFrame()
    unique_errors_lines = set()
    check_empty_df = final_df.isin(["", np.nan, "NaN"])
    data_check_result = {"filename": params["filename"], "worksheetId": params["worksheet_id"],
                         "jobId": f"{params['worksheet_id']}_job", "lobName": params["lob_name"],
                         "lobId": params["lob_id"], "totalExposures": final_df.shape[0], "uniqueErrorLines": 0,
                         "totalErrors": 0, "totalTIV": calculate_tiv(final_df), "currency": "EUR", "jobResult": []}

    for field_code, field_data in target_fields.items():
        print(field_data)
        field_type = field_data["dataType"]
        data_check = field_data["dataCheck"]
        empty_column = check_empty_df[field_code]
        error_lines_per_field = []

        if (field_type != "string") and (not empty_column.all()):
            checker = CheckerFactory.get_checker("TYPE")
            type_check = checker.run(final_df, field_code, empty_column, field_type=field_type)
            if type_check is not None:
                print("TYPE")
                result_df = extend_result_df(result_df, type_check, checker.check_code, field_code,
                                             checker.check_level)
                if metadata:
                    error_lines_per_field = final_df[field_code][type_check].index.tolist()
                    total_errors_per_field = len(error_lines_per_field)
                    if total_errors_per_field:
                        total_errors_lines += total_errors_per_field
                        unique_errors_lines.update(error_lines_per_field)
                        data_check_result["jobResult"].append({field_code: len(error_lines_per_field)})
                continue

        if data_check:
            for check in data_check["rules"]:
                checker = CheckerFactory.get_checker(check["type"])

                check_result = checker.run(final_df, field_code, empty_column, check=check, field_type=field_type,
                                           empty_df=check_empty_df)
                if check_result is not None:
                    print(check["type"])
                    result_df = extend_result_df(result_df, check_result, checker.check_code, field_code,
                                                 checker.check_level)
                    if metadata:
                        error_lines = final_df[field_code][check_result].index.tolist()
                        error_lines_per_field = list(set().union(error_lines_per_field, error_lines, []))
            print(len(error_lines_per_field))
            total_errors_per_field = len(error_lines_per_field)
            if total_errors_per_field:
                total_errors_lines += total_errors_per_field
                unique_errors_lines.update(error_lines_per_field)
                data_check_result["jobResult"].append({field_code: len(error_lines_per_field)})

    data_check_result["uniqueErrorLines"] = len(unique_errors_lines)
    data_check_result["totalErrors"] = total_errors_lines

    return data_check_result, result_df


def check_modifications(final_df, tiv_df, params, target_fields, result_df, modifications):
    """Runs checks on modifications"""

    modified_columns = {column: 1 for column in modifications["content"]}
    indices = range(0, result_df.shape[0]) if modifications.get("is_all") else modifications.get("indices")

    data_check_result, modifications_result_df = run_checks(final_df, params, target_fields, metadata=False)
    modifications_result_df.index = indices

    for column in modifications_result_df.columns.values:
        check_type, field_code, error_type = eval(column)
        check_column = pd.Series(data=False, index=range(0, result_df.shape[0]))
        check_column.loc[indices] = modifications_result_df[column]

        if result_df.get(column) is None:
            result_df = extend_result_df(result_df, check_column, check_type,
                                         field_code, error_type)
        else:
            result_df[column].loc[indices] = check_column

    update_data_check_metadata(data_check_result, result_df, modified_columns, modifications_result_df, indices)
    if modifications["is_all"] or (len(modifications["indices"]) > 100):
        data_check_result["totalTIV"] = calculate_tiv(final_df)
    else:
        data_check_result["totalTIV"] = update_tiv(final_df, tiv_df, params["worksheet_id"])
    return data_check_result, result_df


def update_data_check_metadata(data_check_result, result_df, modified_columns, modifications_result_df, indices=None):
    """Update a data check metadata dictionnary from result dataframe"""

    total_errors_lines = 0
    unique_errors_lines = set()
    job_result = {}
    for column in result_df.columns.values:
        _, field_code, _ = eval(column)
        if modified_columns.get(field_code) and modifications_result_df.get(column) is None:
            if indices:
                result_df[column].loc[indices] = False
            if not result_df[column].any():
                result_df.drop(column, axis=1, inplace=True)
                continue
        total_errors_per_field = result_df[column].sum()
        if total_errors_per_field:
            if job_result.get(field_code):
                job_result[field_code] += total_errors_per_field
            else:
                job_result[field_code] = total_errors_per_field
            total_errors_lines += int(total_errors_per_field)
            unique_errors_lines.update(result_df.index[result_df[column]].tolist())

    data_check_result["jobResult"] = [{field_code: int(job_result[field_code])} for field_code in job_result]
    data_check_result["uniqueErrorLines"] = len(unique_errors_lines)
    data_check_result["totalErrors"] = total_errors_lines


def calculate_tiv(df):
    """Calculates total insured value"""

    return float(pd.to_numeric(df["tiv_amount"]).sum())


def update_tiv(final_df, tiv_df, worksheet_id):
    """Updates total insured value after each modification"""

    job_result_document = JobResultDocument()

    tiv = job_result_document.get_tiv_amount(worksheet_id)
    delta_tiv = 0
    for row in tiv_df.itertuples():
        delta_tiv += (pd.to_numeric(final_df.loc[row.Index]["tiv_amount"]) - pd.to_numeric(tiv_df.loc[row.Index]["tiv_amount"]))

    return tiv + delta_tiv
