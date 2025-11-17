from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient


MONGO_URI = "mongodb+srv://luvratan:1A7blmhecqOxowmc@cluster0.qyoff.mongodb.net/Tickle?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "Tickle"
COLLECTION_NAME = "newDocsNotification"

# Create MongoDB client
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

