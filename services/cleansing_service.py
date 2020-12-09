#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

from services.dataframe import extend_result_df, reindex_result_df
from database.job_result_document import JobResultDocument
from services.check.checker_factory import CheckerFactory


def run_checks(final_df, target_fields, data_check_result, metadata=True):
    """Runs all the checks defined for all targets in a given mappings document"""

    total_errors_lines = 0
    result_df = pd.DataFrame()
    unique_errors_lines = set()
    check_empty_df = final_df.isin(["", np.nan, "NaN"])

    for field_code, field_data in target_fields.items():
        print(field_data)
        field_type = field_data["type"]
        data_check = field_data["rules"]
        ref_type_id = field_data.get("ref_type_id")
        empty_column = check_empty_df[field_code]
        error_lines_per_field = []

        if (field_type != "string") and (not empty_column.all()):
            checker = CheckerFactory.get_checker("TYPE_CHECK")
            # TODO REMOVE IF WHEN APP IS STABLE
            if checker:
                type_check = checker.run(final_df, field_code, empty_column, field_type=field_type)
                if type_check is not None:
                    result_df = extend_result_df(result_df, type_check, checker.check_code, field_code,
                                                 checker.check_level)
                    if metadata:
                        error_lines_per_field = final_df[field_code][type_check].index.tolist()
                        print("TYPE")
                        print(len(error_lines_per_field))
                        total_errors_per_field = len(error_lines_per_field)
                        if total_errors_per_field:
                            total_errors_lines += total_errors_per_field
                            unique_errors_lines.update(error_lines_per_field)
                            data_check_result["jobResult"][field_code] = {"name": field_code,
                                                                          "label": field_data["label"],
                                                                          "errors": len(error_lines_per_field)}

        if data_check:
            for check in data_check:
                checker = CheckerFactory.get_checker(check["type"])
                # TODO REMOVE IF WHEN APP IS STABLE
                if checker:
                    check_result = checker.run(final_df, field_code, empty_column, check=check, field_type=field_type,
                                               empty_df=check_empty_df, ref_type_id=ref_type_id,
                                               domain_id=data_check_result["domainId"])
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
                if data_check_result["jobResult"].get(field_code):
                    data_check_result["jobResult"][field_code]["errors"] += len(error_lines_per_field)
                else:
                     data_check_result["jobResult"][field_code] = {"name": field_code,
                                                                   "label": field_data["label"],
                                                                   "errors": len(error_lines_per_field)}

    data_check_result["totalRowsInError"] = len(unique_errors_lines)
    data_check_result["totalErrors"] = total_errors_lines

    return reindex_result_df(result_df)


def check_modifications(final_df, result_df, target_fields, data_check_result, modifications, indices):
    """Runs checks on modifications"""

    modified_columns = {column: 1 for column in modifications}

    modifications_result_df = run_checks(final_df, target_fields, data_check_result, metadata=False)
    if not modifications_result_df.empty:
        modifications_result_df.index = indices

    for column in modifications_result_df.columns:
        check_type, field_code, error_type, check_index = eval(column)
        check_column = pd.Series(data=False, index=range(0, result_df.shape[0]))
        check_column.loc[indices] = modifications_result_df[column]

        if result_df.get(column) is None:
            result_df = extend_result_df(result_df, check_column, check_type,
                                         field_code, error_type)
        else:
            result_df[column].loc[indices] = check_column.loc[indices]

    # result_df = reindex_result_df(result_df)

    update_data_check_metadata(data_check_result, result_df, modified_columns, modifications_result_df, target_fields,
                               indices)

    return result_df


def update_data_check_metadata(data_check_result, result_df, modified_columns, modifications_result_df, target_fields,
                               indices):
    """Update a data check metadata dictionnary from result dataframe"""

    total_errors_lines = 0
    unique_errors_lines = set()
    job_result = {}
    for column in result_df.columns.values:
        _, field_code, _, _ = eval(column)
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

    data_check_result["jobResult"] = [{"name": field_code, "label": target_fields[field_code]["label"],
                                       "errors": int(job_result[field_code])} for field_code in job_result]
    data_check_result["totalRowsInError"] = len(unique_errors_lines)
    data_check_result["totalErrors"] = total_errors_lines
    data_check_result["totalLocations"] = result_df.shape[0]
