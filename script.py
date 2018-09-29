import config
import mysql.connector
import time

mongodb = config.mongodb
mongocol = mongodb["boletas"]

def getLastRemoteId():
    last_remote_id = 0
    for row in mongocol.find({}, { "sync_id": 1, "_id": 0 }):
        last_remote_id = row['sync_id']
    return last_remote_id

def uploadChanges():
    last_remote_id = getLastRemoteId()
    conn = config.connect()
    mysqldb = conn.cursor()
    mysqldb.execute("SELECT * FROM boletas")
    result_set = mysqldb.fetchall()
    for row in result_set:
        local_id = row[0]
        if (int(local_id) > int(last_remote_id)):
            mongocol.insert_one({ "sync_id": local_id })
            print('record uploaded')
    print('Synced')
    mysqldb.close()

while True:
    time.sleep(5)
    uploadChanges()
