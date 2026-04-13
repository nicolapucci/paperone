from datetime import datetime
import re
import requests
from bs4 import BeautifulSoup

from services.logger import logger  
from services.redis_client import get_changelog_releases,set_changelog_releases

"""
    Il codice raffigurato fa nel complesso tre cose:
    -registro storico delle fw e
    -scraper del changelog(interrogazione continua della fonte)
"""
tmp_release_mapper = {
    '4.0.0':datetime(2015,11,17),
    '4.0.1':datetime(2015,11,19),
    '4.0.2':datetime(2015,11,27),
    '4.0.3':datetime(2015,12,11),
    '4.0.4':datetime(2015,12,23),
    '4.0.5':datetime(2016,1,15),
    '4.0.6':datetime(2016,1,29),
    '4.0.7':datetime(2016,2,15),
    '4.0.8':datetime(2016,3,4),    
    '4.0.9':datetime(2016,5,10),
    '4.0.10':datetime(2016,5,27),
    '4.1.1':datetime(2016,4,6),
    '4.1.2':datetime(2016,4,28),
    '4.1.3':datetime(2016,5,2),
    '4.1.4':datetime(2016,5,10),
    '4.1.5':datetime(2016,5,31),
    '4.1.6':datetime(2016,6,23),
    '4.1.7':datetime(2016,7,5),
    '4.2.0':datetime(2016,7,15),
    '4.2.1':datetime(2016,7,29),
    '4.2.2':datetime(2016,9,6),
    '4.2.3':datetime(2016,9,28),
    '4.2.4':datetime(2016,10,7),
    '4.2.5':datetime(2016,11,14),
    '4.2.6':datetime(2016,12,15),
    '4.2.7':datetime(2017,2,16),
    '4.3.0':datetime(2016,8,9),
    '4.3.1':datetime(2016,9,5),
    '4.3.2':datetime(2016,9,13),
    '4.3.3':datetime(2016,9,30),
    '4.3.4':datetime(2016,10,17),
    '4.3.5':datetime(2016,11,8),
    '4.3.6':datetime(2016,11,28),
    '4.3.7':datetime(2017,1,10),
    '4.3.8':datetime(2017,2,6),
    '4.3.9':datetime(2017,3,9),
    '4.3.10':datetime(2017,3,17),
    '4.3.11':datetime(2017,4,13),
    '4.4.0':datetime(2017,5,3),
    '4.4.1':datetime(2017,5,29),
    '4.4.2':datetime(2017,8,31),
    '4.5.0':datetime(2017,5,22),
    '4.5.1':datetime(2017,5,17),
    '4.5.2':datetime(2017,6,29),
    '4.5.3':datetime(2017,7,3),
    '4.5.4':datetime(2017,8,30),
    '4.5.5':datetime(2017,10,25),
    '4.5.6':datetime(2017,11,29),
    '4.5.7':datetime(2017,1,19),
    '4.5.8':datetime(2018,3,12),
    '4.5.9':datetime(2018,5,21),
    '4.5.10':datetime(2018,5,29),
    '4.5.11':datetime(2018,6,6),
    '4.5.12':datetime(2018,6,25),
    '4.5.13':datetime(2018,7,2),
    '4.5.14':datetime(2018,7,13),
    '4.5.15':datetime(2018,7,16),
    '4.5.16':datetime(2018,7,24),
    '4.5.17':datetime(2018,7,31),
    '4.6.0':datetime(2018,9,19),
    '4.6.1':datetime(2018,10,23),
    '4.6.2':datetime(2019,1,16),
    '4.7.0':datetime(2018,10,2),
    '4.7.1':datetime(2018,10,11),
    '4.7.2':datetime(2018,10,25),
    '4.7.3':datetime(2018,11,27),
    '4.7.4':datetime(2018,12,11),
    '4.7.5':datetime(2019,1,17),
    '4.7.6':datetime(2019,1,31),
    '4.7.7':datetime(2019,2,18),
    '4.7.8':datetime(2019,2,25),
    '4.7.9':datetime(2019,3,7),
    '4.7.10':datetime(2019,2,28),
    '4.7.11':datetime(2019,4,4),
    '4.7.12':datetime(2019,6,6),
    '4.7.13':datetime(2019,7,9),
    '4.7.14':datetime(2019,7,18),
    '4.7.15':datetime(2019,7,22),
    '4.7.16':datetime(2019,8,8),
    '4.7.17':datetime(2019,9,19),
    '4.8.0':datetime(2019,12,2),
    '4.8.1':datetime(2019,12,3),
    '4.8.2':datetime(2019,12,23),
    '4.8.3':datetime(2020,1,13),
    '4.8.4':datetime(2020,4,30),
    '4.8.5':datetime(2020,5,18),
    '4.9.0':datetime(2019,10,31),
    '4.9.1':datetime(2019,11,12),
    '4.9.2':datetime(2019,12,17),
    '4.9.3':datetime(2019,12,19),
    '4.9.4':datetime(2020,1,28),
    '4.9.5':datetime(2020,2,3),
    '4.9.6':datetime(2020,2,27),
    '4.9.7':datetime(2020,3,6),
    '4.9.8':datetime(2020,4,26),
    '4.9.9':datetime(2020,5,6),
    '4.10.0':datetime(2020,7,21),
    '4.10.1':datetime(2020,10,27),
    '4.10.2':datetime(2020,11,17),
    '4.11.0':datetime(2020,6,5),
    '4.11.1':datetime(2020,7,3),
    '4.11.2':datetime(2020,8,12),
    '4.11.3':datetime(2020,10,10),
    '4.11.4':datetime(2020,10,23),
    '4.11.5':datetime(2020,11,17),
    '4.11.6':datetime(2020,11,19),
    '4.11.7':datetime(2020,12,1),
    '4.11.8':datetime(2020,12,22),
    '4.11.9':datetime(2020,12,28),
    '4.11.10':datetime(2021,2,8),
    '4.11.11':datetime(2021,3,9),
    '4.11.12':datetime(2021,3,17),
    '4.12.0':datetime(2021,4,28),
    '4.12.1':datetime(2021,7,16),
    '4.13.0':datetime(2021,6,23),
    '4.13.1':datetime(2021,9,13),
    '4.13.2':datetime(2021,9,29),
    '4.13.3':datetime(2021,12,4),
    '4.13.4':datetime(2021,12,23),
    '4.13.5':datetime(2021,12,30),
    '4.13.6':datetime(2022,2,14),
    '4.13.7':datetime(2022,4,5),
    '4.13.8':datetime(2022,5,5),
    '4.14.0':datetime(2022,5,27),
    '4.14.1':datetime(2023,1,26),
    '4.15.0':datetime(2022,6,8),
    '4.15.1':datetime(2022,9,29),
    '4.15.2':datetime(2022,10,4),
    '4.15.3':datetime(2022,11,25),
    '4.15.4':datetime(2023,3,12),
    '4.15.5':datetime(2023,4,14),
    '4.15.6':datetime(2023,5,24),
    '4.15.7':datetime(2023,7,19),
    '4.15.8':datetime(2023,10,12),
    '4.15.9':datetime(2023,10,27),
    '4.15.10':datetime(2023,11,13),
    '4.15.11':datetime(2024,1,5),
    '4.15.12':datetime(2024,1,25),
    '4.15.13':datetime(2024,2,1),
    '4.15.14':datetime(2024,2,15),
    '4.16.0':datetime(2024,2,22),
    '4.16.1':datetime(2024,4,5),
    '4.16.2':datetime(2024,5,22),
    '4.17.0':datetime(2024,6,5),
    '4.17.1':datetime(2024,7,22),
    '4.17.2':datetime(2024,9,2),
    '4.17.3':datetime(2024,11,7),
    '4.17.4':datetime(2024,12,11),
    '4.17.5':datetime(2024,12,17),
    '4.17.6':datetime(2025,1,31),
    '4.17.7':datetime(2025,3,12),
    '4.17.8':datetime(2025,3,20),
    '4.17.9':datetime(2025,5,6),
    '4.17.10':datetime(2025,5,20),
    '4.17.11':datetime(2025,6,4),
    '4.17.12':datetime(2025,10,3),
    '4.17.13':datetime(2025,12,10),
}

wiki_changelog_url = "https://wiki.kalliope.com/it/latest/Changelog.html"


class ProductRepository:
    @staticmethod
    def rc0_releases():
        return tmp_release_mapper

    @staticmethod
    def changelog_releases():

        releases = get_changelog_releases()

        if releases:
            return releases

        response = requests.get(wiki_changelog_url)

        response.raise_for_status()

        html = response.text
        soup = BeautifulSoup(html,'html.parser')

        versions = {}

        for item in soup.find_all('a', class_='reference internal'):
            href = item.get('href',None)

            pattern = r'#firmware-(\d+-\d+-\d+)-(\d{1,2})-(\d{1,2})-(\d{4})'

            match = re.search(pattern,href) if href else None

            if match:
                version = re.sub(r'-','.',match.group(1))
                day = int(match.group(2))
                month = int(match.group(3))
                year = int(match.group(4))

                date = datetime(year,month,day)

                versions[version] = date

        
        set_changelog_releases(versions)

        return versions
