import MySQLConnection
import time

while True:
    MySQLConnection.load_data()
    time.sleep(2 * 60 * 60)