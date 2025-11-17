import anthropic
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request
import pandas as pd
import io


def claude_batch_processing_portfolio(df_tables_list):

    client = anthropic.Anthropic(api_key="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")

    count = 0
    requests = []

    for df in df_tables_list:
        # Convert dataframe to CSV string instead of dict records
        
        column_oriented_json = df.to_dict(orient="list")
        prompt = f""" I need you to structure investment data from a CSV into a standardized format.

        Here's the raw data in dictionary format inside:
        {column_oriented_json}

        Please transform each record according to the following rules:

        1. Parse the 'Reported Investment Type' field (for debt investments) into the following subfields:
        - Investment Type (e.g., Term Loan, Revolver)
        - Calculated Yield
        - Reference Rate (e.g., SOFR, PRIME)
        - Spread (percentage points above reference rate)
        - Floor (minimum interest rate)
        - PIK (payment-in-kind component, if applicable)
        - ETP (if applicable)

        2. Parse the 'Portfolio Company' field into:
        - Portfolio Company (actual company name)
        - Reported Industry (industry name, if present in same field)
        - Notes (Footnotes, if present in the same field)
        - If the field includes both parent and child company names, concatenate both names together and treat the result as the Portfolio Company name.

        3. Clean and standardize all numeric fields:

        - Remove dollar signs and commas
        - For the fields Par, Cost, and FV, convert values in parentheses (e.g., (21)) to negative numbers with a minus sign (e.g., -21)
        - Use null or an empty string ("") for missing values instead of 0

        4. Number of Shares will go under "Shares".

        5. Series will also go under "Reported Investment Type"

        6. Handle edge cases:
        - If fields are missing or in unexpected formats, use reasonable defaults
        - For date fields with varying formats, standardize to MM/DD/YYYY
        - For ambiguous entries, prefer the most likely interpretation based on context
        - If a date is provided in MM/YY format (e.g., 03/24), interpret it as the first day of that month, i.e., convert it to 01/03/2024 (MM/DD/YYYY)

        7. Maintain all other fields as-is and structure the records as a JSON array of objects following this schema.

        8. If an interest rate is already provided in the raw data , use that value for the "Calculated Yield" field.

        9. If you find any fields that could represent percentages and the values are given in decimal form (e.g., 0.05), do not convert them to percentage format. Leave the values exactly as they are in the original data, without adding a percent sign or multiplying by 100.

        Here's an example of the expected output format for a single record:

        "Portfolio Company": "Bleriot US Bidco Inc.",
        "Reported Industry": "Aerospace and Defense",
        "Reported Investment Type": "First Lien Term Loan",
        "Cost": "3351",
        "FV": "3373",
        "Par": "3351",
        "Shares": "4512",
        "Percent of Net Assets": "0.27",
        "Reference Rate": "SOFR",
        "Spread": "2.75",
        "Stated Coupon": "7.25",
        "Floor": "12.00",
        "Calculated Yield": "7.06",
        "Maturity": "10/31/2030",
        "PIK": "2.5",
        "Notes": "(5)(15)",
        "Origin Date": "10/31/2022", 
        "Ceiling": "2.50",
        "ETP": "2.50"

        CRITICAL INSTRUCTIONS FOR DATA COMPLETENESS:

        1. DO NOT OMIT ANY INFORMATION from the original data under any circumstances. Every piece of data in the source must appear somewhere in the output.

        2. Do not remove similar investments if you find any.

        3. Verify all records from input are present in output (exact same count).

        4. Remove any subtotals if you find any.

        5. Normalize all partial dates (like MM/YY) into full MM/DD/YYYY format by assuming the day is 01 (e.g., 03/24 becomes 01/03/2024).
        
        """




        function_schema = {
            "name": "structure_records",
            "description": "Transform raw investment data into a clean, structured format",
            "input_schema": {
                "type": "object",
                "properties": {
                    "records": {
                        "type": "array",
                        "description": "List of transformed investment records",
                        "items": {
                            "type": "object",
                            "properties": {
                                "Portfolio Company": {"type": "string"},
                                "Reported Industry": {"type": "string"},
                                "Reported Investment Type": {"type": "string"},
                                "Calculated Yield": {"type": "string"},
                                "Reference Rate": {"type": "string"},
                                "Spread": {"type": "string"},
                                "Floor": {"type": "string"},
                                "PIK": {"type": "string"},
                                "Stated Coupon": {"type": "string"},
                                "Ceiling": {"type": "string"},
                                "ETP": {"type": "string"},
                                "Par": {"type": "string"},
                                "Cost": {"type": "string"},
                                "Shares": {"type": "string"},
                                "FV": {"type": "string"},
                                "Percent of Net Assets": {"type": "string"},
                                "Notes": {"type": "string"},
                                "Origin Date": {"type": "string"},
                                "Maturity": {"type": "string"},
                            },
                            "required": [
                                "Portfolio Company",
                                "Reported Investment Type",
                                "Cost",
                                "FV",
                            ],
                        },
                    }
                },
                "required": ["records"],
            },
        }
        data = Request(
            custom_id=str(count),
            params=MessageCreateParamsNonStreaming(
                model="claude-3-7-sonnet-20250219",
                max_tokens=14000,
                tools=[function_schema],
                messages=[{"role": "user", "content": prompt}]
            )
        )

        requests.append(data)
        count += 1

    response = client.messages.batches.create(requests=requests)
    return response