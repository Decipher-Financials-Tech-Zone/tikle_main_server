from pydantic import BaseModel

class Bdc_status(BaseModel):
    bdc_name : str
    complete_file_name: str
    reporting_date: str
    claude_batch_reference_id:str 
    claude_status_of_requests:str
    postgresql_status : str
    postgresql_reference_id: str
	
