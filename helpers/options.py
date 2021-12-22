from enum import Enum, auto

class ScrapeType(Enum):
    ALL='ALL'
    DATE='DATE'
    LATEST='LATEST'

ScrapeTypeMapped:dict={
    0:ScrapeType.ALL,
    1:ScrapeType.DATE,
    2:ScrapeType.LATEST
}