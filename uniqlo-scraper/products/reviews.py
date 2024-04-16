from fake_useragent import UserAgent
from dotenv import load_dotenv
from itertools import starmap
import pandas as pd
import requests
import asyncio
import os
import gc
import re
import logging

load_dotenv()

logging.basicConfig(
    filename='logs/info-record.log', 
    level=logging.INFO, 
    filemode='w'
) # Log config

async def generateURL(id):
    masterID = re.findall(r'\d+', id.split('-')[0])
    return reviewURL.format(masterID[0])

async def amountData(link):
    try:
        session = requests.Session()
        ua = UserAgent()

        params = {
            'offset': 0,
            'limit': 1
        }

        req = session.get(
            link, 
            headers={'User-Agent': ua.random},
            params=params
        )

        data = req.json()['result']['pagination']['total']
        logging.info(f'Amount data: {asyncio.current_task().get_name()} -> {req.status_code}')
        return data
    
    except requests.exceptions.JSONDecodeError:
        logging.info(f'Amount data: {asyncio.current_task().get_name()} -> {req.status_code}')
        pass

async def searchData(dt):
    # Extract data from reviews
    data = {
        'Title': None,
        'Rating': None,
        'Size': None,
        'Gender': None,
        'Comment': None,
        'Rate Product': dt.get('rating', {}).get('count'),
        'Avarage Rate': dt.get('rating', {}).get('average'),
    }
    
    if dt.get('reviews'):
        for rev in dt.get('reviews'):
            data['Title'] = rev.get('title')
            data['Rating'] = rev.get('rate')
            data['Size'] = rev.get('purchasedSize')
            data['Gender'] = rev.get('gender', {}).get('name')
            data['Comment'] = rev.get('comment')

    logging.info(f'{asyncio.current_task().get_name()} -> Success extract data')
    return pd.DataFrame(data, index=[0])

 
async def getData(total_data, link):
    try:
        session = requests.Session()
        ua = UserAgent()

        params = {
            'offset': 0,
            'limit': total_data,
        }

        req = session.get(
            link, 
            headers={'User-Agent': ua.random},
            params=params
        )
        data = req.json()['result']

        logging.info(f'Get data: {asyncio.current_task().get_name()} -> {req.status_code}')
        logging.info(f'URL: {req.url}')
        return data
     
    except requests.exceptions.JSONDecodeError:
        logging.info(f'Get data: {asyncio.current_task().get_name()} -> {req.status_code}')
        logging.info(f'URL: {req.url}')
        pass
    except KeyError:
        logging.info(f'Structure data: {req.json()}')
        raise KeyError('Structure data not found')
    
async def loadData(final_data):
    productData = pd.read_csv(
        'data/products.csv'
    ) # Get data from products
    
    combineData = pd.concat(
        final_data, 
        ignore_index=True,
        sort=False
    ).fillna('') # Combine all data from reviews

    mergeData = productData.assign(
        **combineData
    ) # Merge data

    if os.path.exists('data/products.csv'):
        mergeData.to_csv(
            'data/new-products.csv', 
            mode='w', 
            index=False
        ) # If exists then append
        logging.info(f'{asyncio.current_task().get_name()} -> Success append data')
    else:
        mergeData.to_csv(
            'data/new-products.csv', 
            index=False
        ) # If not exists then create
        logging.info(f'{asyncio.current_task().get_name()} -> Success create data')

async def main():
    with open('data/unique-ids.txt', 'r') as f:
        id = (i.strip() for i in f.readlines()) # Generator Expression
    
    """ GET REVIEWS """
    urls = map(
        generateURL,
        id
    ) # Generate URL based on unique id

    resultUrl = await asyncio.gather(
        *urls
    )

    getTotalReviews = map(
        amountData,
        resultUrl
    ) # Get total reviews based on URL

    totalReviews = await asyncio.gather(
        *getTotalReviews
    )

    with open('data/total-product.txt', 'w') as f:
        for item in totalReviews:
            f.write(f'{item}\n')

    del urls,\
        getTotalReviews, \
        getTotalReviews, \
        totalReviews # Delete data after get total reviews

    await asyncio.sleep(60 * 5) # Wait 5 minutes, for recovery server from downtime

    with open('data/total-product.txt', 'r') as f:
        totalReviews = list(i.strip() for i in f.readlines())

    """ GET DATA """
    taskData = list(
        starmap(
            getData,
            zip(totalReviews, resultUrl)
        )
    ) # Get all data based on URL and total reviews

    del urls, \
        resultUrl, \
        totalReviews # Delete data after get data review

    data = await asyncio.gather(
        *taskData
    )

    del taskData # Delete data after create task data

    search = map(
        searchData,
        data
    ) # Search for the desired data

    result = await asyncio.gather(
        *search
    )

    await loadData(result) # Load data to csv

    del data, \
        search, \
        result # Delete data after load data
    
    gc.collect()

reviewURL = os.getenv('REVIEW')
asyncio.run(main())