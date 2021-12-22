from dataclasses import dataclass, field,asdict
import dateparser
from datetime import datetime,date
from businessindia.helpers.handlers import DateHandler
import pytz






@dataclass(order=True)
class Article:
    news_url:str=field(compare=False,repr=False)
    thumbnail_src:str=field(compare=False,repr=False)
    news_title:str=field(compare=False)
    published_date:datetime=field(compare=True)
    news_content:str=field(compare=False,repr=False)
    authors:str=field(compare=False,repr=False)
    publisher_name:str=field(compare=False,repr=False)
    org_url:str=field(compare=False,repr=False)

    def __post_init__(self):
        self.published_date=self.parse_date(self.published_date)

    @staticmethod
    def parse_date(datestring):
        return DateHandler.parse_date(datestring,return_string=True,return_time=True)
        #parser_settings={
        #    'DATE_ORDER': 'DMY',
        #    'TIMEZONE': 'UTC',
        #    'RETURN_AS_TIMEZONE_AWARE': True
        #}
        #if isinstance(datestring,str):
        #    return dateparser.parse(datestring,settings=parser_settings)
        #
        #if isinstance(datestring,datetime) or isinstance(datestring,date):
        #    return pytz.utc.localize(datestring)
    def to_dict(self):
        return asdict(self)