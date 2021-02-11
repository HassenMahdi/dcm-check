#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import pyarrow as pa

from api.utils.storage import get_mapped_df
from database.modifier_document import ModifierDocument
from services.azure_blob_downloader import AzureBlobDownloader
from services.check.checkers.checker import Checker


class UnicityChecker(Checker):

    check_code = "UNICITY_CHECK"
    check_name = "unicity"
    check_message = "The field cannot be duplicated."
    check_level = "errors"

    def check_column(self, df, column, empty_column, *args, **kwargs):
        """Checks if a given column is empty"""

        if not empty_column.any():
            lookup = kwargs.get("check").get("lookup")
            kwargs.get("reload").append(column)
            data_check_result = kwargs.get("data_check_result")
            if df.shape[0] != data_check_result["totalLocations"]:
                modifier_document = ModifierDocument()
                mapped_df = get_mapped_df(data_check_result["fileId"], data_check_result["worksheetId"])
                modifier_document.apply_modifications(mapped_df, data_check_result["worksheetId"], is_all=True)
            else:
                mapped_df = df
            if lookup == "all":
                domain_id = data_check_result.get("domainId")
                azure_blob_downloader = AzureBlobDownloader()
                tables = azure_blob_downloader.download_all_blobs_in_container(prefix=f'{domain_id}/')
                if len(tables) > 0:
                    old_data = pa.concat_tables(tables, promote=True).select([column]).to_pandas()
                    unique_series = mapped_df[column].append(old_data[column], ignore_index=True)
                    return unique_series.duplicated(keep=False)[df.index] == False

            return mapped_df[column].duplicated(keep=False)[df.index] == False

    def get_message(self, **kwargs):

        field_data = kwargs.get("field_data")
        field_name = field_data.get("label")

        return f"{field_name} values must be unique"