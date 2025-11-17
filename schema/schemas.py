def get_one_bdc_status(bdc_status) -> dict:
    return{
        "id":str(bdc_status["_id"]),
        "bdc_name" : str(bdc_status["bdc_name"]),
        "complete_file_name": str(bdc_status["complete_file_name"]),
        "reporting_date": str(bdc_status["reporting_date"]),
        "claude_batch_reference_id":str(bdc_status["claude_batch_reference_id"]) ,
        "claude_status_of_requests":str(bdc_status["claude_status_of_requests"]),
        "postgresql_status" : str(bdc_status["postgresql_status"]),
        "postgresql_reference_id": str(bdc_status["postgresql_reference_id"]),
    }

def get_all_bdc_status(cursor):
    return [get_one_bdc_status(status) for status in cursor]

