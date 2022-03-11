import logging
import pyodbc
from sql_connection import SqlConnection


class SqlQueries:

    def __init__(self):

        try:
            self.connection = pyodbc.connect('Driver={};Port=1433;Server={};Database={};uid={};pwd={}'.format(
                SqlConnection.driver, SqlConnection.server, SqlConnection.database, SqlConnection.username,
                SqlConnection.password))
            self.cursor = self.connection.cursor()
            logging.info("Input Database Connection established")

        except Exception as e:
            logging.error("Input Database Connection is not established", e)

    def insert_user_details(self, firstname, lastname, username, password):
        insert_user = "INSERT INTO user_details VALUES ('{}', '{}', '{}', '{}')".format(firstname, lastname, username,
                                                                                         password)
        self.cursor.execute(insert_user)
        self.connection.commit()

    def insert_mongodb_credentials(self, username, uri):
        insert_cred = "INSERT INTO mongodb_users VALUES(NULL, '{}', '{}', NULL, NULL)".format(username, uri)
        self.cursor.execute(insert_cred)
        self.connection.commit()

    def insert_sql_credentials(self, username, uri):
        insert_cred = "INSERT INTO sql_users VALUES(NULL, '{}', '{}', NULL, NULL)".format(username, uri)
        self.cursor.execute(insert_cred)
        self.connection.commit()

    def update_mongodb_cosmos_name(self, username, mongodb_cosmos_group):
        update_cred = "UPDATE mongodb_users SET cosmos_group_name = '{}' where username = '{}'". \
            format(mongodb_cosmos_group, username)
        self.cursor.execute(update_cred)
        self.connection.commit()

    def update_sql_cosmos_name(self, username, sql_cosmos_group):
        update_cred = "UPDATE sql_users SET cosmos_group_name = '{}' where username = '{}'".format(sql_cosmos_group,
                                                                                                   username)
        self.cursor.execute(update_cred)
        self.connection.commit()

    def update_mongodb_adf_name(self, username, adf_group):
        update_cred = "UPDATE mongodb_users SET adf_resource_name = '{}' where username = '{}'".format(adf_group,
                                                                                                       username)
        self.cursor.execute(update_cred)
        self.connection.commit()

    def update_sql_adf_name(self, username, adf_group):
        update_cred = "UPDATE sql_users SET adf_resource_name = '{}' where username = '{}'".format(adf_group, username)
        self.cursor.execute(update_cred)
        self.connection.commit()

    def check_user_existence(self, username, password):
        self.cursor.execute("select distinct username from user_details where username = '{}' and pswd = '{}'"
                            .format(username, password))
        return [result[0] for result in self.cursor.fetchall()]

    def check_mongodb_uri_existence(self, username, uri):
        self.cursor.execute("select distinct mongodb_uri from mongodb_users where username = '{}' and mongodb_uri"
                            " = '{}'".format(username, uri))
        return [result[0] for result in self.cursor.fetchall()]

    def check_sql_uri_existence(self, username, uri):
        self.cursor.execute("select distinct sql_uri from sql_users where username = '{}' and sql_uri = '{}'"
                            .format(username, uri))
        return [result[0] for result in self.cursor.fetchall()]

    def get_mongodb_uris_with_username(self, username):
        self.cursor.execute("select distinct mongodb_uri from mongodb_users where username = '{}'".format(username))
        return [result[0] for result in self.cursor.fetchall()]

    def get_sql_uris_with_username(self, username):
        self.cursor.execute("select distinct sql_uri from sql_users where username = '{}'".format(username))
        return [result[0] for result in self.cursor.fetchall()]

    def get_last_mongodb_id(self):
        self.cursor.execute("select id from mongodb_users order by id desc limit 1")
        res = [result[0] for result in self.cursor.fetchall()]
        if not res:
            return 0
        return res[0]

    def get_last_sql_id(self):
        self.cursor.execute("select id from sql_users order by id desc limit 1")
        res = [result[0] for result in self.cursor.fetchall()]
        if not res:
            return 0
        return res[0]

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
