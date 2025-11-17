from fastapi import FastAPI, Depends, HTTPException, status, Body
from fastapi.security import OAuth2PasswordBearer
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
from jose import jwt, JWTError
from datetime import datetime, timedelta
from typing import Optional

# MongoDB configuration
MONGO_URL = "mongodb+srv://luvratan:1A7blmhecqOxowmc@cluster0.qyoff.mongodb.net/Tickle?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "Tickle"
COLLECTION_NAME = "users"

# JWT Configuration
JWT_SECRET = "Decipherproprietory"
JWT_ALGORITHM = "HS256"

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# MongoDB client setup
client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]
users_collection = db[COLLECTION_NAME]

# Initialize FastAPI
app = FastAPI()

# Pydantic model for login request


class LoginRequest(BaseModel):
    email: EmailStr
    password: str

# Pydantic model for token data


class TokenData(BaseModel):
    sub: Optional[str] = None


# Function to create JWT token


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=60))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)



# Token extraction
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# Dependency to validate and retrieve the current user
async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
        token_data = TokenData(sub=email)
    except JWTError:
        raise credentials_exception

    user = await users_collection.find_one({"email": token_data.sub})
    if user is None:
        raise credentials_exception
    return user
