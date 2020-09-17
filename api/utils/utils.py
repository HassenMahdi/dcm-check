#!/usr/bin/python
# -*- coding: utf-8 -*-

import uuid


def generate_id():
    return uuid.uuid4().hex.upper()


def create_check_metadata(df, job_id, worksheet_id, mapping_id, domain_id):
    """Creates the metadata dict for run checks function"""

    if not job_id:
        job_id = generate_id()
    
    data_check_result = {"jobId": job_id,
                         "worksheetId": worksheet_id,
                         "mappingId": mapping_id,
                         "domainId": domain_id,
                         "totalLocations": df.shape[0], 
                         "totalErrors": 0, 
                         "totalRowsInError": 0, 
                         "jobResult": {}}

    return data_check_result
