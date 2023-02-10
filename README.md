# FIPS_Address_Scraper

Gathers and enriches accurate time-specific data containing zipcode, and county level FIPS information from HUD.
Does not use the HUD API. It was easier in this case to do a manual scrape with requests.


## Getting Started

The only setup needed is to change the MAIN_DIR variable located in the "code/main.py" directory.
Change it to the parent folder of where your storing your code. 
The rest of the directory should be automatically created.

### Prerequisites

The file can be found in the "code/requirements.txt" directory.

```
pip install -r "./FIPS_Address_Scraper/code/requirements.txt"
```

