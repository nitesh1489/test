from typing import List
from businessindia.helpers.models import Article
from businessindia.helpers.handlers import ChecksumHandler
from datetime import datetime
import logging
import json
import pandas as pd

logger=logging.getLogger(__name__)


def export_csv(df:pd.DataFrame,outputPath:str,filename:str):
    todaysdt=datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
    filename=f'{todaysdt}_{filename}.csv'
    finalPath=f'{outputPath}/{filename}'
    if not df.empty:
        df.to_csv(finalPath,sep='|')
        logger.info(f'Exported to:{finalPath}')
    else:
        logger.info('No data to export as csv')



def export_json(output:List[Article],outputPath:str,filename:str):
    todaysdt=datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
    filename=f'{todaysdt}_{filename}.json'
    finalPath=f'{outputPath}/{filename}'
    if output:
        final_res=[article.to_dict() for article in output]
        with open(finalPath,'w') as f:
            json.dump(final_res,f,indent=4)
        logger.info(f'Exported to:{finalPath}')
    else:
        logger.info('No data to export as csv')

def export_df(output:List[Article]):
    if output:
        df=pd.DataFrame(output)
        return df
    else:
        logger.warning('No Data in Dataframe returning empty')
        return pd.DataFrame()

def export_checksum_db(df:pd.DataFrame,tblname:str):
    if not df.empty:
        to_drop=[
            'thumbnail_src',
            'news_title',
            'news_content',
            'authors',
            'publisher_name'
        ]
        df=df.drop_duplicates(subset='news_url')
        df=df.dropna()
        df=df.drop(columns=to_drop,axis=1)
        checksum=ChecksumHandler()
        checksum.push_to_business_table(df=df,tablename=tblname)