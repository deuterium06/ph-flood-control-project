from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException, UnexpectedAlertPresentException
from selenium.webdriver.support.ui import Select, WebDriverWait

import sys
import pandas as pd
import time
import os
from datetime import datetime
from bs4 import BeautifulSoup
import warnings

import time
from datetime import datetime

# Record start time
start_time = datetime.now()
print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

# Ignore all warnings
warnings.filterwarnings("ignore")
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
#sys.stdout = open("output_log.txt", "w", encoding="utf-8")

options = Options()
# options.add_argument("--headless")        # Run in headless mode
#options.add_argument("--disable-gpu")     # Recommended for Windows

# Initialize Chrome once
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 10)

def get_region(driver):

    lists = []
    button = wait.until(EC.element_to_be_clickable((By.ID, "toggle-filters")))
    button.click()

    dropdown = driver.find_element(By.ID, "region")
    select = Select(dropdown)

    region_values = [opt.text.strip() for opt in select.options[1:]]

    driver.refresh()

    return region_values

def select_region(driver, region):

    # Click the button
    button = wait.until(EC.element_to_be_clickable((By.ID, "toggle-filters")))
    button.click()

    dropdown = wait.until(EC.presence_of_element_located((By.ID, "region")))
    
    # Select option by visible text
    select = Select(dropdown)
    select.select_by_visible_text(region)

    search_button = driver.find_element(By.CLASS_NAME, "filter-search")
    search_button.click()

    time.sleep(2)
    print(f"\rScraping data for {region}")

def load_more_rows(driver):
    loadCount = 0

    while True:
        try:
            # Find the "Load more" button
            button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div.fcp-loadmore-wrap button#load-more-projects"))) #driver.find_element(By.CSS_SELECTOR, "div.fcp-loadmore-wrap button#load-more-projects")
            has_more = button.text 
            
            if has_more == "Load more":
                
                driver.execute_script("arguments[0].scrollIntoView(true);", button)
                button.click()
                time.sleep(1) # wait for new rows

                # overwrite same line with progress
                sys.stdout.write(f"\rClicking {loadCount} 'Load more' buttons. ")
                sys.stdout.flush()

                loadCount = loadCount + 1
            else:
                print("No more projects to load.")
                break
        except UnexpectedAlertPresentException as e:
            print(f"Unexpected alert detected: {e}. ")
            try:
                alert = driver.switch_to.alert
                alert.dismiss()   # or alert.accept()
                print("Alert dismissed.")
                time.sleep(3)
                continue
            except:
                print("No alert found on retry.")
            time.sleep(2)

        except (NoSuchElementException, ElementClickInterceptedException, TimeoutException) as e:
            print("No 'Load more' button found. Finished.")
            break

def scrape_rows(driver):
    """Scrape current <tr> and <template> rows from the table."""
    mainTable = driver.find_element(By.ID, "projects-body")

    rows = mainTable.find_elements(By.XPATH, "./tr | ./template")

    COLUMN_KEYS = [
        "Project Description",
        "Province",
        "Contractor",
        "Cost",
        "Completion Date",
        "Report"
    ]

    rows = mainTable.find_elements(By.XPATH, "./tr | ./template")

    combined_data = []
    skip_next = False
    rowCount = 0 
    for idx, row in enumerate(rows):
        if skip_next:  
            skip_next = False
            continue

        if row.tag_name.lower() == "tr":
            row_data = {}
            values = row.find_elements(By.TAG_NAME, "td")
            for key, value in zip(COLUMN_KEYS, values):
                row_data[key] = value.text.strip()

            # look ahead to next element: should be a <template>
            if idx + 1 < len(rows) and rows[idx + 1].tag_name.lower() == "template":
                template = rows[idx + 1]
                inner_html = template.get_attribute("innerHTML")
                soup = BeautifulSoup(inner_html, "html.parser")

                row_data["Start Date"] = soup.select_one(".start-date span").text.strip()
                button = soup.select_one(".open-report-form")
                row_data["Long Lat"] = soup.select_one(".longi span").text.strip()
                row_data["Region"] = button["data-region"]
                row_data["Contract ID"] = button["data-contract_id"]
                others = soup.select("div.others span")
                row_data["Type of Work"] = construction_type = others[1].text.strip()
                row_data["Fiscal Year"] = fiscal_year = others[2].text.strip()   

                skip_next = True  # skip processing this template separately

            combined_data.append(row_data)
        
        rowCount = rowCount + 1

        sys.stdout.write(f"\rScraping {rowCount} data for this region.")
        sys.stdout.flush()

    driver.refresh()
    time.sleep(2)

    return combined_data

def main():
    driver.get("https://sumbongsapangulo.ph/")
    time.sleep(2)  # wait for page to render

    regions = get_region(driver) # get all regions from dropdown

    for region in regions:

        select_region(driver, region) # pick region from dropdown  
        load_more_rows(driver) # click "Load more" until done
        data = scrape_rows(driver) # scrape only when fully loaded
        df = pd.DataFrame(data)

        #date_str = datetime.now().strftime("%Y-%m-%d")
        fileName = f"flood-control-data.csv"
        
        try:
            if os.path.exists(fileName):
                df.to_csv(fileName, mode="a", header=False, index=False)
            else:
                df.to_csv(fileName, mode="w", header=True, index=False)
            print (f"\r{region} data has been added to csv.")
        except:
            print(f"\rThere seems to an issue writing data for {region}.")
    
    # ''' for test case '''    
    # select_region(driver, "BARMM") # pick region from dropdown
    # load_more_rows(driver) # click "Load more" until done
    # data = scrape_rows(driver) # scrape only when fully loaded
    # df = pd.DataFrame(data)
    # df.to_csv("flood-control-data.csv", index=False, encoding="utf-8-sig")

    end_time = datetime.now()
    print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    elapsed = end_time - start_time
    minutes, seconds = divmod(elapsed.total_seconds(), 60)
    print(f"Total Runtime: {int(minutes)} minutes {int(seconds)} seconds")

if __name__ == "__main__":
    main()
    driver.quit()

    
