import math
import time
from urllib.parse import urlencode
import pandas as pd


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

    def load_headers(self, sheetPath):
        return pd.read_csv(sheetPath,
                                engine='c',
                                dtype=str,
                                na_filter=False,
                                nrows=0, sep=';').columns.tolist()




    def get_paginated_response(self, data,labels,check_results):
        paginated_response = {

            "total": len(data),
            "count": len(data),
            "headers":labels,
            "results":check_results,
            "data": data.to_dict(orient='records')
        }



        return paginated_response
