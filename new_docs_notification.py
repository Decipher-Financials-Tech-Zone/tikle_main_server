import pandas as pd
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient, UpdateOne
import httpx
from datetime import datetime, timedelta
import json
import logging
from fastapi.middleware.cors import CORSMiddleware

# Initialize FastAPI app
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change "*" to specific origins if needed
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (POST, GET, OPTIONS, etc.)
    allow_headers=["*"],
)

# MongoDB configuration
MONGO_URL = "mongodb+srv://luvratan:1A7blmhecqOxowmc@cluster0.qyoff.mongodb.net/Tickle?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "Tickle"
COLLECTION_NAME = "newDocsNotification"

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MongoDB client setup
try:
    client = MongoClient(MONGO_URL)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logging.info("Connected to MongoDB successfully.")
except Exception as e:
    logging.error(f"Failed to connect to MongoDB: {e}")
    raise RuntimeError(f"Failed to connect to MongoDB: {e}")


def function_to_adjust_meta_Data(data, when: str):
    filings = data.get("filings", {})
    recent = pd.DataFrame(filings.get("recent", []))

    # Determine the date filter
    if when == "endofday" or when == "reset":
        filter_date = (datetime.today() - timedelta(days=1)).date()
    else:
        filter_date = datetime.today().date()

    filter_date_str = filter_date.strftime('%Y-%m-%d')

    # Uncomment this to enter the date of the notifications we want to fetch.
    # filter_date_str = "2025-04-04"
    # print(filter_date_str)

    # Filter for filings with the specific date
    recent_filtered = recent[recent["filingDate"] == filter_date_str].copy()

    # Add the 'cikName' column
    recent_filtered["cikName"] = data["name"]

    # Convert the filtered DataFrame to JSON
    new_filings = recent_filtered.to_json(orient="records")

    # Create the meta_data dictionary
    meta_data = {
        "name": data["name"],
        "cik": data["cik"],
        "fiscalYearEnd": data["fiscalYearEnd"],
        "exchanges": data["exchanges"],
        "tickers": data["tickers"],
        "recent_filings_array": new_filings
    }

    return meta_data


async def get_SEC_metadata(CIK: str, when: str):
    try:
        url = f"https://data.sec.gov/submissions/CIK{CIK}.json"
        headers = {'User-Agent': 'luv.ratan@decipherfinancials.com'}

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)

        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code,
                                detail=f"Error fetching data: {response.text}")

        data = response.json()
        final_data = function_to_adjust_meta_Data(data, when)
        return final_data

    except httpx.RequestError as e:
        logging.error(f"HTTP request error: {e}")
        raise HTTPException(status_code=500, detail=f"HTTP request error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


async def function_to_fetch_new_docs(when: str):
    try:
        cik_info = pd.read_excel("./CIK_info.xlsx")
        current_time = datetime.utcnow()
        if when == "reset":
            logging.info(f"[{current_time}] Dropping collection {COLLECTION_NAME} before resetting.")
            collection.drop()  # Drop the collection
        logging.info(f"[{current_time}] Processing filings for: {when}")
        dataarr = []

        for _, row in cik_info.iterrows():
            try:
                cik_number = row.iloc[0]
                cik_name = row.iloc[2]
                filled_cik = str(cik_number).zfill(10)

                logging.info(f"[{current_time}] Processing: {cik_name} ({filled_cik})")
                data = await get_SEC_metadata(filled_cik, when)

                recent_filings_array = data.get("recent_filings_array", [])
                if isinstance(recent_filings_array, str):
                    recent_filings_array = json.loads(recent_filings_array)

                if recent_filings_array:
                    df = pd.DataFrame(recent_filings_array)
                    if not df.empty:
                        df["cikName"] = cik_name
                        df["ciknumber"] = cik_number
                        df["timestamp"] = current_time  # Add timestamp to the DataFrame
                        dataarr.append(df)
            except Exception as e:
                logging.warning(f"[{current_time}] Error processing CIK {filled_cik}: {e}")
                continue

        if dataarr:
            final_df = pd.concat(dataarr, ignore_index=True)

            # Prepare updates for MongoDB
            operations = []
            for _, row in final_df.iterrows():
                filter_query = {
                    "cikName": row["cikName"],
                    "filingDate": row["filingDate"],
                    "form": row["form"]
                }
                row_dict = row.to_dict()
                row_dict["timestamp"] = current_time  # Add timestamp to MongoDB update
                update_data = {"$set": row_dict}
                operations.append(UpdateOne(filter_query, update_data, upsert=True))

            if operations:
                collection.bulk_write(operations)

            logging.info(f"[{current_time}] New documents updated successfully.")
            return {"message": "New documents have been updated successfully"}
        else:
            logging.info(f"[{current_time}] No new documents found for the given date.")
            return {"message": "No new documents found for the given date"}

    except FileNotFoundError:
        logging.error(f"[{current_time}] The file './CIK_info.xlsx' does not exist.")
        raise HTTPException(status_code=404, detail="CIK info file not found.")
    except Exception as e:
        logging.error(f"[{current_time}] An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


@app.get("/store-notifications")
async def store_end_of_day_notifications():
    try:
        result = await function_to_fetch_new_docs("endofday")
        return result
    except Exception as e:
        logging.error(f"Failed to fetch new documents: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch new documents: {e}")


@app.get("/store-notifications-today")
async def store_today_notifications():
    try:
        result = await function_to_fetch_new_docs("today")
        return result
    except Exception as e:
        logging.error(f"Failed to fetch today's documents: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch today's documents: {e}")
    

@app.get("/reset-notifications")
async def reset_notifications():
    try:
        result = await function_to_fetch_new_docs("reset")
        return result
    except Exception as e:
        logging.error(f"Failed to fetch new documents: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch new documents: {e}")
