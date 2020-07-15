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

    def load_paginated_dataframe(self, sheetPath, totalExposures, worksheet_id):
        self.total = totalExposures
        last_page = math.ceil(self.total / self.limit)
        if last_page <= 0:
            last_page = self.DEFAULT_PAGE

        self.__generate_links(last_page)

        offset = (self.page - 1) * self.limit
        end = offset + self.limit

        start = time.time()
        exposures = pd.read_csv(sheetPath,
                                engine='c',
                                dtype=str,
                                na_filter=False,
                                skiprows=lambda x: x != 0 and x <= offset,
                                nrows=self.limit)

        modifier_document = ModifierDocument()
        modification = {"indices": list(range(offset, end)), "is_all": False}
        exposures = modifier_document.load_check_modifications(exposures, worksheet_id, modification)

        end = time.time()

        # save offset for later use
        self.offset = offset

        print("Paginated exposures loading took %s" % (end - start))

        return exposures

    def get_paginated_response(self, data):
        paginated_response = {
            "_links": self.links,
            "current_page": self.page,
            "first_page": self.DEFAULT_PAGE,
            "last_page": self.last_page,
            "total": self.total,
            "count": len(data),
            "exposures": data.to_dict(orient='records'),  # check split, records, or index
        }

        exposures = paginated_response["exposures"]
        if isinstance(exposures, dict) and "index" in exposures:
            del exposures["index"]

        end = self.limit + self.offset
        if end > self.total:
            end = self.total
        paginated_response["index"] = list(range(self.offset, end))

        return paginated_response
