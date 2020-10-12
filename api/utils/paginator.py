#!/usr/bin/python
# -*- coding: utf-8 -*-

import math
import time
from urllib.parse import urlencode

import pandas as pd

from api.utils.storage import get_mapped_df
from database.modifier_document import ModifierDocument


class Paginator:
    """
    A general purpose paginator based on offset/limit style.
    """

    # maximun size per page that we can reach
    MAX_LIMIT = 500
    DEFAULT_PAGE = 1  # first page
    DEFAULT_LIMIT = 50  # default size per page

    def __init__(self, base_url, query_dict={}, page=0, limit=0):
        self.base_url = base_url
        self.query_dict = query_dict.copy()

        if page <= 0:
            page = self.DEFAULT_PAGE
        else:
            page = int(page)

        if limit <= 0:
            limit = self.DEFAULT_LIMIT
        elif limit > self.MAX_LIMIT:
            limit = self.MAX_LIMIT
        else:
            limit = int(limit)

        self.query_dict["page"] = self.page = page
        self.query_dict["nrows"] = self.limit = limit

    def __generate_self_link(self):
        return self.base_url + "?" + urlencode(self.query_dict)

    def __generate_first_link(self):
        updated_query = self.query_dict.copy()
        updated_query["page"] = 1

        return self.base_url + "?" + urlencode(updated_query)

    def __generate_prev_link(self):
        if self.query_dict["page"] <= 1:
            return None
        updated_query = self.query_dict.copy()
        updated_query["page"] -= 1
        return self.base_url + "?" + urlencode(updated_query)

    def __generate_next_link(self):
        if self.query_dict["page"] >= self.last_page:
            return None
        updated_query = self.query_dict.copy()
        updated_query["page"] += 1
        return self.base_url + "?" + urlencode(updated_query)

    def __generate_last_link(self):
        updated_query = self.query_dict.copy()
        updated_query["page"] = self.last_page
        return self.base_url + "?" + urlencode(updated_query)

    def __generate_links(self, last_page):
        self.last_page = last_page
        self.links = {
            "self": self.__generate_self_link(),
            "first": self.__generate_first_link(),
            "prev": self.__generate_prev_link(),
            "next": self.__generate_next_link(),
            "last": self.__generate_last_link()
        }

    def load_paginated_dataframe(self, path, total_exposures, worksheet_id, filter_indices=None):
        self.total = total_exposures
        last_page = math.ceil(self.total / self.limit)
        if last_page <= 0:
            last_page = self.DEFAULT_PAGE

        self.__generate_links(last_page)

        offset = (self.page - 1) * self.limit
        delta_nrows = total_exposures - (self.page * self.limit)
        nrows = self.limit if delta_nrows >= 0 else total_exposures - ((self.page - 1) * self.limit)
        end = offset + nrows
        if filter_indices is not None:
            if len(filter_indices) == 0:
                skiprows = range(0, total_exposures)
            else:
                indices = filter_indices[offset:end]
                in_rows = {index + 1 for index in indices}
                skiprows = set(range(1, max(indices) + 1)) - in_rows if indices else {}
                self.absolute_index = indices
        else:
            skiprows = lambda x: x != 0 and x <= offset
            indices = list(range(offset, end))

        start = time.time()
        exposures = pd.read_csv(path,
                                error_bad_lines=False,
                                engine='c',
                                dtype=str,
                                na_filter=False,
                                skiprows=skiprows,
                                nrows=nrows,
                                delimiter=';')
        self.offset = offset
        if exposures.empty:
            return exposures
        modifier_document = ModifierDocument()
        exposures.index = indices
        modifier_document.apply_modifications(exposures, worksheet_id, indices=indices)
        end = time.time()

        # save offset for later use


        print("Paginated exposures loading took %s" % (end - start))

        return exposures

    def get_paginated_response(self, data):

        data["row_index"] = data.index.astype(int)
        paginated_response = {
            "_links": self.links,
            "current_page": self.page,
            "first_page": self.DEFAULT_PAGE,
            "last_page": self.last_page,
            "total": self.total,
            "count": len(data),
            "data": data.to_dict(orient='records')
        }

        exposures = paginated_response["data"]
        if isinstance(exposures, dict) and "index" in exposures:
           del exposures["index"]

        end = self.limit + self.offset
        if end > self.total:
            end = self.total
        paginated_response["index"] = list(range(self.offset, end))

        if hasattr(self, "absolute_index"):
            paginated_response["absolute_index"] = getattr(self, "absolute_index")

        return paginated_response