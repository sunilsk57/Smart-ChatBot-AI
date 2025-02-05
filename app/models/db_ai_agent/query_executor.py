import sqlite3
import yaml


class SqlQueryExecutor:
    def __init__(self, db_manager, config=None):
        self.db_manager = db_manager
        if config is None:
            with open("./app/config.yml", "r") as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = config

    def execute_sql_query(self, sql_query: str):
        """Execute the generated SQL query on the database."""
        connection = sqlite3.connect(self.config["DB_NAME"])
        cursor = connection.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
        connection.close()
        return result
