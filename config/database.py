from pymongo import MongoClient

client = MongoClient("mongodb+srv://luvratan:1A7blmhecqOxowmc@cluster0.qyoff.mongodb.net/TIKLE_Documents?retryWrites=true&w=majority&appName=Cluster0")

db = client.TIKLE_Documents
collection_name = db["TIKLE_BDC_Documents_status"]