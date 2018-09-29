import config
import time

mongodb = config.mongodb
mongocol = mongodb["detalle_boleta"]

def getLastLocalId():
    mysqldb = config.mysqldb
    mysqldb.execute("SELECT * FROM detalle_boleta")    
    for row in mysqldb:
        last_local_id = row[0]
    return last_local_id

def getLastRemoteId():
    for row in mongocol.find({}, { "sync_id": 1, "_id": 0 }):
        last_remote_id = row['sync_id']
    return last_remote_id

def uploadChanges():
    last_remote_id = getLastRemoteId()
    mysqldb = config.mysqldb
    mysqldb.execute("SELECT * FROM detalle_boleta")
    for row in mysqldb:
        local_id = row[0]
        if (int(local_id) > int(last_remote_id)):
            mongocol.insert_one({ "sync_id": local_id })
            print('record uploaded')
    print('Synced')

while True:
    time.sleep(5)
    uploadChanges()



# record = [{ "sync_id": 2762 }, { "sync_id": 2763 }]
# mongocol.insert_many(record)
