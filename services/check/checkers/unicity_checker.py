#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import pyarrow as pa

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
            if lookup == "all":
                domain_id = kwargs.get("domain_id")
                azure_blob_downloader = AzureBlobDownloader()
                tables = azure_blob_downloader.download_all_blobs_in_container(prefix=f'{domain_id}/')

                if len(tables) > 0:
                    old_data = pa.concat_tables(tables, promote=True).select(column).to_pandas()
                    df = pd.concat([df[column], old_data], axis=0, ignore_index=True)
                    return pd.Series(df[column].duplicated(keep=False), df.index)

            return pd.Series(df[column].duplicated(keep=False), df.index)
