from fake_useragent import UserAgent
from dotenv import load_dotenv
from itertools import starmap
from random import choice
import pandas as pd
import requests
import asyncio
import os
import gc
import logging

load_dotenv()
logging.basicConfig(
    filename='logs/products.log', 
    level=logging.INFO, 
    filemode='w'
)

async def searchData(dt):
    data = {
        'Name': dt['name'],
        'Prices': dt['prices']['base']['value'].split('.')[0],
        'Gender': dt['genderName'],
        'Description': dt['shortDescription'],
        'Information Product': dt['freeInformation'],
    }
    logging.info(f'{asyncio.current_task().get_name()} -> Success extract data')
    return pd.DataFrame(data, index=[0])

async def getData(path):
    try:
        trackTask = asyncio.current_task() # To track the current task

        target_url = os.getenv('API')

        params = {
            'productIds': path,
        }

        session = requests.Session()
        req = session.get(
            target_url, 
            headers={'User-Agent': choice(uas)},
            params=params
        )

        data = req.json()['result']['items']

        logging.info(f'Get data: {trackTask.get_name()} -> {req.status_code}')
        return data
    except requests.exceptions.JSONDecodeError:
        logging.info(f'Get data: {trackTask.get_name()} -> {req.status_code}')
        pass

async def loadData(finalData):
    combineData = pd.concat(finalData, ignore_index=True)

    if os.path.exists('data/products.csv'):
        combineData.to_csv('data/products.csv', mode='w', index=False)
        logging.info(f'{asyncio.current_task().get_name()} -> Success append data')
    else:
        combineData.to_csv('data/products.csv', index=False)
        logging.info(f'{asyncio.current_task().get_name()} -> Success create data')

async def main():
    with open('data/unique-ids.txt', 'r') as f:
        id = [i.strip() for i in f.readlines()]
    
    tasks = map(
        getData, id
    ) # Generate all task

    resultsTask = await asyncio.gather(
        *tasks
    )
    
    resultData = list(
        starmap(
            searchData,
            resultsTask
        )
    ) # Generate all task for get data

    data = await asyncio.gather(
        *resultData
    )

    await loadData(data)

    del id, \
        tasks, \
        resultsTask, \
        resultData, \
        data # Delete unused data

    gc.collect()
        
ua = UserAgent().data_browsers
uas = [a.get('useragent') for a in ua]
asyncio.run(main())