# backup - 备份当月和上个月病人数据
# backup patient_guid 备份指定病人数据
# recover 指定文件夹路径(递归)
#  pyinstaller --onefile   .\mongodb_backup.py
import motor.motor_asyncio
import bson
import os
import asyncio
import glob
from pymongo import MongoClient
import pymongo
from gridfs import *
import datetime as dt
from bson.objectid import ObjectId

print("backup - ip 天数")
print("backup patient_guid ip")
print("recover 路径 ip")
inp = input()
comm = inp.split(' ')
if len(comm) >= 3:
    ip=comm[2]
else:
    ip='127.0.0.1'
client = MongoClient(ip, 27227)
if comm[0]=='backup':
    guid = comm[1]
    work_dir=os.getcwd()
    guid_path = work_dir + '\\' + guid
    fs_path = guid_path + '\\' + 'fs' + '\\'
    if not os.path.exists(work_dir):
        os.mkdir(work_dir)
    if not os.path.exists(guid_path):
        os.mkdir(guid_path)
    if not os.path.exists(fs_path):
        os.mkdir(fs_path)
db = client.datu_data
db.authenticate("datu_super_root", "c74c112dc3130e35e9ac88c90d214555__strong")
fs = GridFSBucket(db)
mongoUri = "mongodb://datu_super_root:c74c112dc3130e35e9ac88c90d214555__strong@"+ip+":27227/?authSource=datu_data"
asyncio_client = motor.motor_asyncio.AsyncIOMotorClient(mongoUri)
datu_data = asyncio_client.datu_data
dataCheckMap=dict()
coll_list=[]
basic_coll_list=[]
mydb = client['datu_data']
backup_day=1
if len(comm)>=4:
    backup_day=comm[3]
for collection_name in mydb.list_collection_names():
    count = mydb[collection_name].count_documents({})
    if count<500:
        basic_coll_list.append(collection_name)
        continue
    coll_list.append(collection_name)
# 指定需要复制fs文件的collection
coll_list.append('image')
coll_list.append('image_2d')
coll_list.append('rtss')
coll_list.append('dose')

def savebson(collection_name, file_data, file_id):
    if collection_name=='fs.files' or collection_name=="fs.chunks":
        return
    if type(file_id)!=str:
        file_id=str(file_id)
    file_id.replace('\\','')
    file_id.replace('/','')
    file_id.replace(':','')
    file_id.replace('*','')
    file_id.replace('?','')
    file_id.replace('"','')
    file_id.replace('<','')
    file_id.replace('>','')
    file_id.replace('|','')
    check=collection_name+":"+file_id
    if check in dataCheckMap:
        return
    dataCheckMap[check]=True
    print(check)
    path = guid_path + '\\' + collection_name
    if not os.path.exists(path):
        os.mkdir(path)
    data = bson.BSON.encode(file_data)
    cur_path = path + '\\' + file_id + '.bson'
    file_bson = open(cur_path, 'wb')
    file_bson.write(bson.binary.Binary(data))


def savefs(file_id,file_name,file_data):
    output_name=fs_path + '\\' + str(file_name)+"__"+ str(file_id)
    print('fs:'+output_name)
    out = open(output_name+'.fs', 'wb')
    out.write(bson.binary.Binary(file_data))
    out.close()


async def startt():
    if len(comm) >= 2:
        if comm[0]=='backup':
            if guid != '-':
                patient_list = datu_data.patient.find({"_id": guid})
            else:
                today = dt.date.today()
                StartTime=today - dt.timedelta(days=backup_day)
                print("start time =", StartTime.strftime("%d/%m/%Y %H:%M:%S"))
                queryTime=dt.datetime(StartTime.year,StartTime.month,StartTime.day)
                patient_list = datu_data.patient.find({"created_time": {"$gte":queryTime}})
            patient_guid_list=[]
            async for patient in patient_list:
                ref_patient_guid = patient["_id"]
                patient_guid_list.append(ref_patient_guid)
                savebson('patient', patient, ref_patient_guid)
                dicom_series = datu_data.dicom_series.find({"ref_patient_id": ref_patient_guid})
                async for dicom in dicom_series:
                    savebson('dicom_series', dicom, dicom["_id"])
            for collection_name in coll_list:
                if collection_name=='dvh':
                    continue
                ref_data_list = datu_data[collection_name].find({"ref_patient_guid": {"$in":patient_guid_list}})
                async for data in ref_data_list:
                    data_guid = data["_id"]
                    savebson(collection_name, data, data_guid)
                    for grid_out in fs.find({"filename":{"$regex": data_guid}}, no_cursor_timeout=True):
                        grid_data = grid_out.read()
                        savefs(grid_out._id,grid_out.filename,grid_data)
            # collection not related patient
            for collection_name in basic_coll_list:
                ref_data_list = datu_data[collection_name].find({})
                async for data in ref_data_list:
                    data_guid = data["_id"]
                    savebson(collection_name, data, data_guid)
        elif comm[0]=='recover':
            path = comm[1]
            list_of_dir = []
            all_files = os.scandir(path)
            for it in all_files:
                if it.is_dir():
                    list_of_dir.append(it)
            while list_of_dir:
                cur_file = list_of_dir.pop()
                list_of_files = glob.glob(cur_file.path + '/*.bson')
                list_of_fs = glob.glob(cur_file.path + '/*.fs')
                for f in list_of_fs:
                    fs_file = open(f, 'rb')
                    file_name = f[len(cur_file.path) + 1:len(f) - 3]
                    imgput = GridFS(db)
                    data_id_name = file_name.split('__')
                    file_nam=data_id_name[0]
                    file_id=ObjectId(data_id_name[1])
                    fs.delete(file_id)
                    res = imgput.put(fs_file, filename=file_nam,_id=file_id)
                    fs_file.close()
                for file in list_of_files:
                    bson_file = open(file, 'rb')
                    bson_data = bson.decode_all(bson_file.read())
                    collection_name = os.path.basename(cur_file.path)
                    coll = datu_data[collection_name]
                    chec =coll.find_one(bson_data[0]['_id'])
                    if chec:
                        _id = coll.update_one(bson_data[0])
                    else:
                        _id = coll.insert_one(bson_data[0])
                all_files = os.scandir(cur_file.path)
                for it in all_files:
                    if it.is_dir():
                        list_of_dir.append(it)
loop = asyncio.get_event_loop()
loop.run_until_complete(startt())