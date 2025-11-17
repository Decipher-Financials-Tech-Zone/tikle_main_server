import anthropic
import pandas as pd
import json


def fetch_portfolio_data_from_claude(claude_reference_id):
    client = anthropic.Anthropic(api_key="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")

    dataframes = []

    for result in client.messages.batches.results(claude_reference_id):
        try:
            # Convert result to dict
            data = json.loads(result.model_dump_json())
            
            # Navigate safely through expected keys
            records = data["result"]["message"]["content"][1]["input"]["records"]
            df = pd.DataFrame(records)
            dataframes.append(df)

        except (KeyError, IndexError, ValueError, TypeError) as e:
            print(f"Error processing batch result: {e}")
            continue

    if dataframes:
        batched_df = pd.concat(dataframes, ignore_index=True)
        return batched_df
    else:
        return pd.DataFrame()
