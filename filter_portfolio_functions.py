def find_portfolio_end(bdc_name, table):

    if bdc_name == "PENNANTPARK INVESTMENT CORP":
        if "Total Investments in Controlled, Affiliated Portfolio Companies" in table:
            return True  # Stop processing when 'Net Assets' is found
    elif bdc_name == "Main Street Capital CORP":
        if "Total Portfolio Investments" in table:
            return True
    elif bdc_name == "FS KKR Capital Corp" or bdc_name == "KKR FS Income Trust" or bdc_name == "AB Private Credit Investors Corp" or bdc_name == "AB Private Lending Fund" or bdc_name == "FS Specialty Lending Fund":
        if "TOTAL INVESTMENTS" in table:
            return True
    elif bdc_name == "Franklin BSP Real Estate Debt BDC" or bdc_name == "Nuveen Churchill Private Capital Income Fund" or bdc_name == "26North BDC, Inc." or bdc_name == "CAPITAL SOUTHWEST CORP" or bdc_name == "Palmer Square Capital BDC INC.":
        if "Total Investments" in table:
            return True
    elif bdc_name == "First Eagle Private Credit Fund":
        if "Total Investments - non-controlled/non-affiliated" in table:
            return True
    elif bdc_name == "Goldman Sachs Middle Market Lending Corp. II" or bdc_name == "Goldman Sachs Private Credit Corp." or bdc_name == "Goldman Sachs Private Middle Market Credit II LLC":
        if "Total Investments in Affiliated Money Market Fund" in table:
            return True
    elif bdc_name == "Overland Advantage":
        if "Total Investments and Cash Equivalents" in table:
            return True
    elif bdc_name == "TriplePoint Venture Growth BDC Corp":
        if "Total Investments in Portfolio Companies" in table:
            return True
    elif bdc_name == "Golub Capital Direct Lending Unlevered Corp":
        if "Total equity investments" in table:
            return True
    elif bdc_name == "Blackrock TCP Capital Corp.":
        if "Total Cash and Investments" in table:
            return True
    elif bdc_name == "Main Street Capital CORP":
        if "Total Portfolio Company" in table:
            return True
    elif bdc_name == "Bain Capital Specialty Finance, Inc.":
        if "Investments Total" in table:
            return True
    return False  # Continue processing


def find_portfolio_start(bdc_name, table):
    if bdc_name == "Barings BDC, Inc.":
        if "us-gaap:ConcentrationRiskPercentage1" in table:
            return True
    return False


# Filter Porfolio functions for private BDCs
