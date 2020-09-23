#!/usr/bin/python
# -*- coding: utf-8 -*-

from database.connectors import mongo


class JobResultDocument:

    def get_data_check_job(self, job_id):
        """Fetches a data cleansing document from job_results collection"""

        job_results = mongo.db.data_check_jobs

        return job_results.find_one({"jobId": job_id})

    def save_check_job(self, job):
        """Saves a data cleansing document in job_results collection"""

        job_results = mongo.db.data_check_jobs

        exist_job = self.get_data_check_job(job["jobId"])
        if exist_job:
            _id = exist_job["_id"]
            job_results.update_one(
                {'_id': _id},
                {'$set': {
                    "jobResult": job["jobResult"],
                    "totalErrors": job["totalErrors"],
                    "totalLocations": job["totalLocations"],
                    "totalRowsInError": job["totalRowsInError"],
                }
                }, upsert=False
            )

        else:
            job["jobResult"] = [job["jobResult"][field] for field in job["jobResult"]]
            job_results.insert_one(job)

        return {"job_id": job["jobId"]}
