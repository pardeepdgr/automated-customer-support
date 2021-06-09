import requests
from bs4 import BeautifulSoup
from pyspark.sql.functions import lit

tag = "div"
parser = "html.parser"
element = "maincounter-number"
url = "https://www.worldometers.info/coronavirus/"


def total_cases():
    res = requests.get(url)
    total_case_count = BeautifulSoup(res.text, parser).find_all(tag, class_=element)[0].text.strip()
    return lit(total_case_count)
