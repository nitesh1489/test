from dataclasses import dataclass
from datetime import datetime,date
import pytz
import dateparser
from typing import Union
import pandas as pd
from sqlalchemy import Column,Integer,DateTime,Text,TIMESTAMP,MetaData,Table
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import OperationalError
from businessindia.helpers.exceptions import InvalidDateFormatException
import os
import logging


logger=logging.getLogger(__name__)

class DateHandler:
    @staticmethod
    def parse_date(datevalue:Union[datetime,date,str],return_string:bool=False,return_time:bool=False,use_DMY_order:bool=True):
        parser_settings={
            'DATE_ORDER': 'DMY',
            'TIMEZONE': 'UTC',
            'RETURN_AS_TIMEZONE_AWARE': True
        }
        if datevalue is None:
            parsed_datetime=datetime.utcnow()
            if return_string:
                if return_time:
                    return parsed_datetime.strftime('&d-%m-%Y-%H:%M')
                return parsed_datetime.date().strftime('%d-%m-%Y')
            else:
                if return_time:
                    return  parsed_datetime
                return parsed_datetime.date()

        if isinstance(datevalue,str):
            try:
                if not use_DMY_order:
                    parser_settings.pop('DATE_ORDER')
                parsed_datetime=dateparser.parse(datevalue,settings=parser_settings)
                if return_string:
                    if return_time:
                        return parsed_datetime.strftime('%d-%m-%Y-%H:%M')
                    return parsed_datetime.date().strftime('%d-%m-%Y')
                else:
                    if return_time:
                        return parsed_datetime
                    return parsed_datetime.date()
            except AttributeError:
                raise InvalidDateFormatException(f'Pass valid date in dd-mm-yyyy format only. Got:{datevalue}')

        if isinstance(datevalue,datetime) or isinstance(datevalue,date):
            if isinstance(datevalue,date):
                datevalue=datetime.combine(datevalue,datetime.min.time())
            localizeddt=pytz.utc.localize(datevalue)
            if return_string:
                if return_time:
                    return localizeddt.strftime('%d-%m-%Y-%H:%M')
                return localizeddt.date().strftime('%d-%m-%Y')
            else:
                if return_time:
                    return localizeddt
                return localizeddt.date()


    @staticmethod
    def parse_db_date(datevalue:str):
        try:
            date=datetime.strptime(datevalue,'%Y-%m-%d %H:%M:%S.%f').date()
            return date
        except AttributeError:
            raise InvalidDateFormatException('Unable to parse the database datetime format try changing it.')


class ChecksumHandler:
    def __init__(self,conn_string:str=None) -> None:
        self.conn_string=os.environ.get('CHECKSUM_DB_CONN_STRING')
        if self.conn_string is None:
            self.conn_string=conn_string if conn_string else 'sqlite:///./checksum.db'
        logger.info('Connected to Checksum Database')
        self.engine=create_engine(self.conn_string)

    def fetch_latest_date(self,org_url:str,datecolname:str='published_date',tablename:str='checksum_business'):
        self.create_non_exist_table(tablename)
        try:
            unique_identifier=org_url.strip()
            query=f"SELECT MAX({datecolname}) FROM {tablename} WHERE org_url='{unique_identifier}'"
            with self.engine.connect() as conn:
                max_date=None
                for res in conn.execute(query):
                    max_date=res[0]
                return max_date
        except Exception as e:
            logger.info(f'Unable to fetch latest date returning None Exception:{e}')
            return None

    def get_unique_csums(self,data:pd.DataFrame,tablename:str='checksum_business'):
        #Generate csums for every provided data as hash of str and str and remove those that match in db and keep those that does not match
        res=pd.read_sql(f'SELECT * FROM {tablename}',self.engine)
        df = pd.merge(data,res,how='left',on=['news_url'],suffixes=('','_db'),indicator=True)
        df=df[[c for c in df.columns if not c.endswith('_db')]]
        df=df.loc[df._merge=='left_only',:]
        df=df.drop(['_merge'],axis=1)
        df=df.drop_duplicates().reset_index(drop=True)
        final=df
        final.columns=final.columns.str.strip()
        return final

    def create_non_exist_table(self,tablename:str):
        meta=MetaData()
        checksumtb=Table(
                    tablename,
                    meta,
                    Column('id',Integer,primary_key=True,autoincrement=True),
                    Column('org_url',Text,index=True),
                    Column('news_url',Text,index=True),
                    Column('published_date',DateTime,index=True),
                    Column('created_date',DateTime,server_default='now()')
                    )
        meta.create_all(self.engine,checkfirst=True)

    def push_to_business_table(self,df:pd.DataFrame,tablename:str='checksum_business'):
        df=df.rename(columns={'org_url':'org_url','news_url':'news_url','published_date':'published_date'})
        df['published_date']=pd.to_datetime(df['published_date'])
        df['created_date']=datetime.utcnow()
        #############
        try:
            final_df=self.get_unique_csums(df)
        except OperationalError:
            final_df=df
        #print(final_df.shape)
        df=final_df
        ##################
        df.to_sql(tablename,self.engine,chunksize=1000,if_exists='append',index=False)
        logger.info(f'Pushed to checksumdb df of shape {df.shape}')



class ProdDBPushHandler:
    def __init__(self,conn_string:str=None) -> None:
        self.conn_string=os.environ.get('PROD_DB_CONN_STRING')
        if not self.conn_string:
            self.conn_string=conn_string if conn_string else 'sqlite:///./prod.db'
        logger.info('Connected to Production Database')

