import sqlite3
import luigi
import pandas as pd
from generatescore import RedditScoreGenerator


class DatabaseConnector(luigi.Task):

    def requires(self):
        return RedditScoreGenerator()

    def input(self):
        return luigi.LocalTarget('score.csv')

    def output(self):
        return luigi.LocalTarget('score.db')

    def run(self):
        df = pd.read_csv('score.csv')
        for row in df.values.tolist():
            self.insert_db(row)
        #self.fetch_from_db()

    def initialize(self):
        connect = sqlite3.connect('scores.db')
        connect.execute("""
            CREATE TABLE score (
                title char(250) not null,
                subreddit char(250) not null,
                post_score float not null
            )        
        """)
        connect.close()

    def insert_db(self, record):
        print(record)
        connect = sqlite3.connect('scores.db')
        cursor = connect.cursor()
        truncate_sql = "DELETE FROM score"
        cursor.execute(truncate_sql)
        connect.commit()

        sql = f"""INSERT INTO score(title, subreddit, post_score) VALUES ("{record[0]}", "{record[1]}", {record[2]})"""
        print(sql)
        cursor.execute(sql)
        connect.commit()
        connect.close()

    def fetch_from_db(self):
        connect = sqlite3.connect('scores.db')
        cursor = connect.cursor()
        cursor.execute("""
            Select * from score       
        """)
        records = cursor.fetchall()
        for row in records:
            print(row)
        connect.close()


if __name__ == '__main__':
    db = DatabaseConnector()
    db.initialize()
    # df = pd.read_csv('score.csv')
    # for row in df.values.tolist():
    #     DatabaseConnector.insert_db(row)
    # DatabaseConnector.fetch_from_db()
