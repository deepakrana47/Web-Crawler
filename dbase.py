import sqlite3

class DB:
    def __init__(self, db_file):
        conn = self.create_connection(db_file)
        pass

    def create_connection(self, db_file):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        # try:
        self.conn = sqlite3.connect(db_file)
        # except Error as e:
        #     print(e)
        return

    def create_table(self, create_table_sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        # try:
        c = self.conn.cursor()
        c.execute(create_table_sql)
        # except Error as e:
        #     print(e)

    def query(self, q):
        # try:
        c = self.conn.cursor()
        c.execute(q)
        # except Error as e:
        #     print(e)
        return c.fetchall()

    def insert(self, q):
        # try:
        c = self.conn.cursor()

        c.execute(q)
        # except Error as e:
        #     print(e)
        return

    def commit(self):
        # try:
        self.conn.commit()
        # except Error as e:
        #     print(e)
        return