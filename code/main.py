'''
Name: William Wu
Date: 1/11/2023
Description: Script to match zipcode against the current FIPS and Address
'''

import os
import logging
import logging.handlers
from datetime import datetime

import pandas as pd
import requests 
import re

MAIN_DIR =  "D:\\Code\\PYTHON\\FIPS_Address_Matching\\"
LOG_DIR = MAIN_DIR + f"Log\\{datetime.now().strftime('%Y%m%d')}\\" 
LOG_FILE = LOG_DIR + f"Log_{datetime.now().strftime('%H%M%S')}.log"

def directory_setup(dir_list):
    '''
    DESCRIPTION -> If the directory does not exist it will create it
    '''
    for directory in dir_list:
        if not os.path.exists(directory):
            os.makedirs(directory)

def logging_setup():
    '''
    DESCRIPTION -> Setups the logging file for code
    '''
    try:
      handler = logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", LOG_FILE))
      formatter = logging.Formatter(fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
      handler.setFormatter(formatter)
      logging.getLogger().handlers.clear()
      root = logging.getLogger()
      root.setLevel(os.environ.get("LOGLEVEL", "INFO"))
      root.addHandler(handler)
      logging.propogate = False
      logging.info("Log File was created successfully.")
    except Exception as e:
        exit
   
def get_hud_data(year, month):
    '''
    DESCRIPTION -> Reads the hud excel sheet and gets the county zip code data

    PARAM 1 -> Desired year
    PARAM 2 -> Desired month
    '''
    date = month + str(year)
    file_url = f"https://www.huduser.gov/portal/datasets/usps/ZIP_COUNTY_{date}.xlsx"
    df = pd.read_excel(file_url, usecols="A:B", dtype=str)
    return df
    
def process_hud_date(df, year, month):
    '''
    DESCRIPTION -> Changes the fips data into the correct format and then enriches it with the year and month

    PARAM 1 -> Desired year
    PARAM 2 -> Desired month
    '''
    
    df.columns = df.columns.str.upper()

    df.rename(columns={"COUNTY":"ST_CTY_FIPS"}, inplace=True)
    
    df["ZIP"] = df["ZIP"].apply(lambda x: x.zfill(5))

    df["YEAR"] = year
    df["MONTH"] = month
    # Shifts the columns
    third_col = df.pop("YEAR")
    fourth_col = df.pop("MONTH")
    df.insert(2, "YEAR", third_col)
    df.insert(3, "MONTH", fourth_col)
    return df
        
def get_county_fips_data():
    '''
    DESCRIPTION -> Scrapes the fips code information
    '''
    fips_url = "https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt"
    fips_r = requests.get(fips_url)

    fips_path = MAIN_DIR + "\\data\\results\\gov_fips.csv"

    with open(fips_path, 'wb') as url:
        for chunk in fips_r.iter_content(chunk_size=1024):
            if chunk:
                url.write(chunk)
    return fips_path

def county_fips_process(fips_path):
    '''
    DESCRIPTION -> Cleans the gov_fips data that was scraped

    PARAM 1 -> fips_path -> The desired output path.
    '''
    with open(fips_path, 'r') as file:
        raw_lines = file.readlines()
        start_index = raw_lines.index('  FIPS code        name\n')
        # We want to drop the first line which is the fips_code and name, as well as the second line which is just blank space
        cleaning_lines = raw_lines[start_index+2:]
        df = pd.DataFrame(columns=["ST_CTY_FIPS", "Name"])
        code_list = []
        name_list = []

        for line in cleaning_lines:
            # Removes leading and trailing white spaces
            leading_trailing_space = re.compile(r"^\s+|\s+$")
            line = leading_trailing_space.sub('',line)

            # Removes the phrase in a parenthesis that occansionally comes after text.
            parenthesis_after = re.compile(r"\(.*$")
            line = parenthesis_after.sub('', line)
            
            # Removes any extra whitespace that occurs and replaces them with a single space
            extra_whitespace = re.compile(r"\s+")
            line = extra_whitespace.sub(' ',line)
            
            # Everything remaining should just be the code and location name
            line_list = line.split(' ',1)
            code_list.append(line_list[0])
            name_list.append(line_list[1])

        # Both lists are read into a dictionary where finally theyre converted into a df after concatening them together
        concat_dict = {"ST_CTY_FIPS": code_list, "Name": name_list}
        df = pd.concat([pd.Series(value, name=key) for key, value in concat_dict.items()], axis=1)
        
    fips_path = MAIN_DIR + "\\data\\results\\county_fips.csv"
    df.to_csv(fips_path, index=False)
    return df

def main():
    dir_list = [MAIN_DIR, LOG_DIR]
    directory_setup(dir_list)
    logging_setup()

    START_YEAR = 2020
    END_YEAR = 2021
    QUARTERS = ["03", "06", "09", "12"]

    merged_df = pd.DataFrame()

    for year in range(START_YEAR, END_YEAR+1):
        for month in QUARTERS:
            # Get HUD data
            hud_df = get_hud_data(year, month)
            # Clean HUD data
            processed_hud_df = process_hud_date(hud_df, year, month)
           
            # Get FIPS data
            fips_path = get_county_fips_data()
            # Clean FIPS data
            processed_fips_df = county_fips_process(fips_path)
            
            # Merge data together
            zip_fips_df = processed_fips_df.merge(processed_hud_df)
            print(f"Completed: {year}-{month}")
            merged_df = pd.concat([merged_df, zip_fips_df])
    merged_path = MAIN_DIR + "\\data\\results\\ct_zip_fips.dta"
    merged_df.to_stata(merged_path)
    print("Done")

if __name__ == "__main__":
    main()