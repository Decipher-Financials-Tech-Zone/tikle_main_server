import anthropic
from fastapi import HTTPException



# Function to fetch status from Claude
def fetch_status_from_claude(batch_id):
    client = anthropic.Anthropic(
        api_key="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"
    )
    
    try:
        response = client.messages.batches.retrieve(batch_id)
        return response
    except anthropic.APIError as e:
        # Handle Anthropic API errors
        raise HTTPException(
            status_code=500, 
            detail=f"Error fetching batch status from Claude: {str(e)}"
        )