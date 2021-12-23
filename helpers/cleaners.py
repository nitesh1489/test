'''cleaner'''

from typing import Union

import codecs

class TextCleaners:

    @staticmethod

    def parse_num_from_string(text:str) -> str:

        text=text.strip().lower()

        text=''.join([alpha for alpha in text if alpha.isdigit()])

        num=int(text)

        return num



    @staticmethod

    def strip_lines_n_space(text:Union[str,list]) -> str:

        if isinstance(text,str):

            clean=text.strip('\n').strip()

            return clean

        if isinstance(text,list):

            clean=' '.join([subtext.strip('\n').strip() for subtext in text])

            return clean

        

    @staticmethod

    def remove_ascii(text:str):

        string=text.encode(encoding = 'UTF-8')

        string=codecs.decode(string, 'UTF-8')

        return string



"hello This has been updated"
