import pandas as pd
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from tqdm import tqdm
from selenium.common.exceptions import TimeoutException

# Initialize WebDriver
driver = webdriver.Safari()
driver.set_page_load_timeout(15)  # Set timeout limit

BASE_URL = "https://www.erowid.org/experiences/exp.cgi?ShowViews=0&Cellar=0&Start=0&Max=39877"
LINKS_FILE = "erowid_links.txt"
REPORTS_FILE = "Erowid_Trip_Reports.csv"
BATCH_SIZE = 100  # Save data in batches

def get_all_report_links(start_url):
    """Fetch all experience report links and save them to LINKS_FILE."""
    if os.path.exists(LINKS_FILE):
        print("Loading existing report links...")
        with open(LINKS_FILE, "r") as f:
            return list(set(f.read().splitlines()))
    
    driver.get(start_url)
    report_links = []
    
    start_time = time.time()
    print(f"Scraping index page: {driver.current_url}")
    page_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'exp.php?ID=')]")
    report_links.extend([link.get_attribute('href') for link in page_links 
                         if link.get_attribute('href').startswith("https://www.erowid.org")])
    
    seconds = time.time() - start_time
    print(f"<<<<<<<<<< Finished gathering links >>>>>>>>>\nTook {seconds:.2f} seconds.")
    
    # Remove duplicates and save to file
    report_links = list(set(report_links))
    with open(LINKS_FILE, "w") as f:
        f.write("\n".join(report_links))
    
    return report_links

def scrape_erowid_reports(report_links):
    """Scrape trip reports and merge new data into REPORTS_FILE without duplicates."""
    all_data = []
    processed_count = 0

    # Load already scraped links from CSV if available
    if os.path.exists(REPORTS_FILE):
        existing_df = pd.read_csv(REPORTS_FILE)
        scraped_links = set(existing_df["Link"].dropna().tolist())
    else:
        scraped_links = set()

    # Remove duplicates from input list and filter out already scraped links
    report_links = list(set(report_links))
    report_links = [link for link in report_links if link not in scraped_links]
    print(f"Remaining reports to scrape: {len(report_links)}")

    start_time = time.time()

    for i, href in enumerate(tqdm(report_links, desc="Scraping Reports", unit="report")):
        # Safety check to ensure link is not processed twice
        if href in scraped_links:
            continue

        if not href.startswith("https://www.erowid.org"):
            print(f"Skipping invalid link: {href}")
            continue

        try:
            driver.get(href)
        except TimeoutException:
            print(f"Timeout error: Skipping {href}")
            continue
        
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        
        if "403 Forbidden: Your IP Address Has Been Blocked" in page_source:
            ip_address = soup.find("h2").text.split(": ")[-1].strip()
            print(f"🚨 IP BANNED: {ip_address} 🚨")
            print(f"Process stopped. Successfully scraped {processed_count} reports.")
            driver.quit()
            break
        
        if "reset.me" in driver.current_url or "wordpress.com" in driver.current_url:
            print(f"🚨 Redirected to external site: {driver.current_url}. Skipping... 🚨")
            continue

        try:
            title_element = soup.find("div", class_="title")
            title = title_element.text.strip() if title_element else "Unknown Title"
            
            substance_element = soup.find("div", class_="substance")
            substance = substance_element.text.strip() if substance_element else "Unknown Substance"
            
            author_element = soup.find("div", class_="author")
            author = author_element.text.replace("by", "").strip() if author_element else "Unknown"
            
            bodyweight_element = soup.find("td", class_="bodyweight-amount")
            bodyweight = bodyweight_element.text.strip() if bodyweight_element else "Unknown"
            
            dose_chart_entries = []
            for row in soup.select("table.dosechart tbody tr"):
                cols = row.find_all("td")
                if len(cols) == 5:
                    dose_chart_entries.append(
                        " | ".join([col.text.strip() for col in cols])
                    )
            dose_chart = "\n".join(dose_chart_entries) if dose_chart_entries else "No Dose Chart Available"
            
            report_text_element = soup.find("div", class_="report-text-surround")
            report_text = "".join(report_text_element.stripped_strings) if report_text_element else "No Text Available"
            
            all_data.append({
                "Title": title,
                "Substance": substance,
                "Author": author,
                "Bodyweight": bodyweight,
                "Dose Chart": dose_chart,
                "Report Text": report_text,
                "Link": href
            })
            processed_count += 1
            scraped_links.add(href)  # Mark as scraped
        except Exception as e:
            print(f"Error scraping {href}: {e}")
            continue

        # Save in batches
        if (i + 1) % BATCH_SIZE == 0 or (i + 1) == len(report_links):
            df_batch = pd.DataFrame(all_data)
            if os.path.exists(REPORTS_FILE):
                existing_df = pd.read_csv(REPORTS_FILE)
                combined_df = pd.concat([existing_df, df_batch], ignore_index=True)
                combined_df.drop_duplicates(subset="Link", inplace=True)
                combined_df.to_csv(REPORTS_FILE, index=False)
            else:
                df_batch.drop_duplicates(subset="Link", inplace=True)
                df_batch.to_csv(REPORTS_FILE, index=False)
            all_data.clear()
            print(f"Saved batch {(i + 1)} to {REPORTS_FILE}")

    seconds_total = time.time() - start_time
    print(f"Finished scraping {processed_count} reports in {seconds_total:.2f} seconds.")
    driver.quit()

def remove_duplicates_from_csv():
    """Clean the CSV file by removing any duplicate entries based on the Link column."""
    if os.path.exists(REPORTS_FILE):
        df = pd.read_csv(REPORTS_FILE)
        before = len(df)
        df.drop_duplicates(subset="Link", inplace=True)
        df.to_csv(REPORTS_FILE, index=False)
        after = len(df)
        print(f"Removed {before - after} duplicate entries from {REPORTS_FILE}.")

# Main execution steps
report_links = get_all_report_links(BASE_URL)
scrape_erowid_reports(report_links)
remove_duplicates_from_csv()