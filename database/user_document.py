#!/usr/bin/python
# -*- coding: utf-8 -*-

from database.connectors import mongo


class UserDocument:

    def get_user_fullname(self, user_id):
        """Fetches user fullname from users collection based on id"""

        users = mongo.db.users

        user = users.find_one({"_id": user_id}, projection=["first_name", "last_name"])
        if user:
            return f"{user['first_name']} {user['last_name']}"