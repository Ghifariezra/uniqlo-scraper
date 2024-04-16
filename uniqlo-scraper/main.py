from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.wait import WebDriverWait as WDW
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidArgumentException
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from ast import literal_eval
from fake_useragent import UserAgent
from itertools import chain
import re
import asyncio
import os
import gc

load_dotenv()

async def generate(url, catalog):
    # Generate URL with specific catalog
    return url + catalog

async def catalogURL(catalogs):
    # Create data catalog
    urlData = {
        'women': catalogs[0],
        'men': catalogs[1],
        'kids': catalogs[2],
        'home': catalogs[3],
    }

    return urlData

async def drivers():
    # Initialization for requests
    UA = UserAgent(
        browsers=['firefox', 'edge', 'chrome'], 
        os=['linux', 'mac'], 
        platforms='pc'
    )

    # Config Driver
    options = Options()
    options.binary_location = './driver/chrome-win64/chrome.exe'
    options.add_argument('--window-size=1920,1080') # Set Window Size --> This is Required when use headless
    options.add_argument('--headless=new')
    options.add_argument(f'--user-agent={UA.random}')
    options.add_argument('log-level=3')

    driver = webdriver.Chrome(options=options)
    return driver

async def getAllLink(link):
    HANDLER = os.getenv("HANDLER")

    drv = await drivers()
    drv.get(link)

    await asyncio.sleep(0.30) # Wait load page

    # Parsing Data & Get all links
    soup = BeautifulSoup(drv.page_source, "lxml")

    elementCategory = soup.body.select(
        selector='#root > div > div > div > div > main > div > div:nth-child(2) > div > div > div > nav:nth-child(5) > div > div.fr-grid.fr-image-with-overlay-text-grid.mb-l.withoutOverlay > article > a'
    ) # Get all links based on search category

    filterValue = [
        c.get('href')
        for c in elementCategory
        if HANDLER in c.get('href')
    ] # Get all links and filter

    missValue = [
        HANDLER + c.get('href')
        for c in elementCategory
        if HANDLER not in c.get('href')
    ] # Handle missing value

    searchCategoryProduct = list(
        chain(filterValue, missValue)
    ) # Update value 

    drv.quit()
    del drv

    gc.collect()

    return searchCategoryProduct

async def scrollUntilDown(drv, wait):
    try:
        scrollElement = '#root > div > div > div > div > main > div > div:nth-child(2) > div > div > div > section > div > div > div:nth-child(2) > div.w12'
        scroll = ActionChains(drv)
        loadMore = wait. \
                until(
                    EC.visibility_of_element_located(
                        (
                            By.CSS_SELECTOR,
                            scrollElement
                        )
                    )
                )

        scroll.move_to_element(
            loadMore
        ).double_click().perform()

        return True

    except Exception as e:
        return False
    
async def getIds(link):
    pattern = r'[A-Z0-9-]+'

    try:
        drv = await drivers()
        drv.get(link)

        wait = WDW(drv, 10)

        modalElemnt = '#root > div > div > div.fr-layer-item.toast > div > div > div > div.close > button'
        closeModal = wait. \
            until(
                EC.visibility_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        modalElemnt
                    )
                )
            )
        closeModal.click()
        
        stop = True
        # Scrolling until element not found
        while stop:
            scrolled = await scrollUntilDown(drv, wait)
            
            if scrolled == False:
                print('Not Found...')
                break
            
            print('Load More...')
            drv.implicitly_wait(5)

        await asyncio.sleep(0.30) # Wait load page

        soup = BeautifulSoup(drv.page_source, "lxml")

        allItems = []
        productIds = soup.body.select(
            selector='#root > div > div > div > div > main > div > div:nth-child(2) > div > div > div > section > div > div > div.row > div.w12.relative > div > div > article > a'
        ) # Get amount of items
        
        ids = [
            re.findall(pattern, p.get('href'))
            for p
            in productIds
        ] # Get all ids and filter with pattern

        # Make sure there are ids
        if ids:
            id = list(
                chain.from_iterable(
                    ids
                )
            ) # Merge all list into one

            allItems.extend(id) # Update all items based on id

        else:
            pass

        return allItems

    finally:
        drv.quit()
        del drv
        gc.collect()

async def writeID(ids):
    newIDS = list(
        dict.fromkeys(ids)
    ) # Remove duplicates from list

    # Write to file
    with open('data/unique-ids.txt', 'w') as f:
        for item in newIDS:
            f.write(f'{item}\n')

    f.close()

    print('Done')

async def main():
    # Required Inizialization
    ENDPOINT = os.getenv("URL")
    CATEGORY = os.getenv("CATEGORIES")
    
    convertCategory = literal_eval(
        CATEGORY
    ) # Convert string representation of list to list

    """ GENERATE URL """
    generateUrl = map(
        lambda catalog: generate(ENDPOINT, catalog),
        convertCategory
    ) # Generate urls and result coruntines

    resultUrl = await asyncio.gather(
        *generateUrl
    ) # Get all results

    """ GET ALL LINKS """
    getAllLinks = map(
        lambda url: getAllLink(url),
        resultUrl
    ) # Get all links

    resultAllLinks = await asyncio.gather(
        *getAllLinks
    ) # Get all results

    finalURL = await catalogURL(
        resultAllLinks
    ) # Transform list to dictionary

    del generateUrl, \
         resultUrl, \
         getAllLinks, \
         resultAllLinks # Remove unused value

    """ GET ALL ITEMS """
    resultItems = []

    for idx, val in enumerate(finalURL.values()):
        task = map(
            lambda url: getIds(url),
            val
        ) # Get all items

        resultAmount = await asyncio.gather(
            *task
        ) # Get all results

        resultItems.extend(
            resultAmount
        ) # Update value of resultItems

    items = list(
        filter(
            None,
            resultItems
        )
    ) # Cleaning value from None

    newItems = list(
        chain.from_iterable(
            items
        )
    ) # Combines all the list sections in the list into 1

    await writeID(newItems) # Write id to file

    del finalURL, \
        resultItems, \
        items, \
        newItems # Remove unused value
    
    gc.collect()

asyncio.run(main())