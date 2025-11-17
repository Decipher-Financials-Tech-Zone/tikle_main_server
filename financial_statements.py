from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import xml.etree.ElementTree as ET
import requests
from bs4 import BeautifulSoup
import pandas as pd

app = FastAPI()

HEADERS = {"user-agent": "luv.ratan@decipherfinancials.com"}


# Fetch Individual Filings

def extract_first_table_from_sec(url: str, headers) -> pd.DataFrame:
    """
    Fetches the first HTML table from an SEC filing page and returns it as a pandas DataFrame.
    
    Parameters:
        url (str): The URL of the SEC filing page (e.g., R3.htm or similar).
        
    Returns:
        pd.DataFrame: The first table found on the page as a pandas DataFrame.
    """
    HEADERS = headers
    
    # Fetch the HTML data
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()  # raise error if failed
    
    # Parse the HTML
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Find the first table
    first_table = soup.find("table")
    if first_table is None:
        raise ValueError("No table found on the provided page.")
    
    # Convert the table into a DataFrame
    df = pd.read_html(str(first_table))[0]
    
    return df

# Fetch Data from financial Summary

def fetch_and_parse_filing_summary(url: str):
    """
    Fetches FilingSummary.xml from SEC EDGAR and extracts Report elements as JSON.
    
    Args:
        url (str): URL to the FilingSummary.xml file
        
    Returns:
        dict: Dictionary containing parsed reports
    """
    # Fetch the XML data
    response = requests.get(url+"/FilingSummary.xml", headers=HEADERS)
    response.raise_for_status()  # Raise error for bad status codes
    
    # Parse the XML
    root = ET.fromstring(response.text)
    
    # Find all Report elements
    reports = []
    for report in root.findall('.//Report'):
        report_dict = {}
        
        # Extract instance attribute if it exists
        if 'instance' in report.attrib:
            report_dict['instance'] = report.attrib['instance']
        
        # Extract all child elements
        for child in report:
            tag = child.tag
            text = child.text
            
            # Convert boolean strings to actual booleans
            if text in ['true', 'false']:
                text = text == 'true'
            # Convert numeric strings to integers where appropriate
            elif tag == 'Position' and text and text.isdigit():
                text = int(text)
            
            # Convert tag to camelCase
            if tag:
                tag = tag[0].lower() + tag[1:]
                report_dict[tag] = text
        
        reports.append(report_dict)
    
    # Create final JSON structure
    output = {"reports": reports}
    
    return output

def group_reports_by_category(parsed_data):
    from collections import defaultdict
    
    grouped = defaultdict(list)
    for r in parsed_data["reports"]:
        category = r.get("menuCategory", "Uncategorized")
        grouped[category].append(r)
    
    for cat in grouped:
        grouped[cat].sort(key=lambda x: x.get("position", 0))
    
    return grouped
