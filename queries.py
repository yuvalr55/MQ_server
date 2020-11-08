import sqlite3


class dbHandler:
    def __init__(self, dbPath):
        self.path = dbPath
        self.conn = None

    def getPath(self):
        return self.path

    def connect(self):
        try:
            conn = sqlite3.connect(self.path)
            self.conn = conn  # .cursor()
            return self.conn
        except Exception as err:
            print('connection error:', err)
            return -1

    def disconnect(self):
        if self.conn:
            self.conn.close()
            print("disconnected from database")

    def select(self, query):
        cur = self.conn.cursor()
        cur.execute(query)
        rs = cur.fetchall()
        self.disconnect()
        return rs


dbHandler()