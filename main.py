import requests
import asyncio
from time import time
import json
import concurrent.futures
from bs4 import BeautifulSoup
import pandas as pd
import asyncio

import re
import unicodedata
from datetime import datetime, timedelta


def no_accent_vietnamese(s):
    s = re.sub(u'Đ', 'D', s)
    s = re.sub(u'đ', 'd', s)
    return unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode("utf-8")


loop = asyncio.get_event_loop()
columns_file = open("columns.txt")
columns = list(map(lambda x: x.replace("\n", ""), columns_file.readlines()))

df = pd.DataFrame(columns=columns)

def get_data_from_minhchinh(date):
    df = pd.DataFrame(columns=columns)
    URL = f"https://www.minhchinh.com/ket-qua-xo-so-mien-trung/{date.strftime('%d-%m-%Y')}.html"

    resps = requests.get(URL)
    soup = BeautifulSoup(resps.content, 'html.parser')

    day_of_week = no_accent_vietnamese(soup.find("td", class_="thu").get_text())
    if day_of_week in ["Thu nam", "Thu bay"]:
        table = soup.find('table', class_='kqxsmiennam miennam4cot')
        number = 3
    else:
        table = soup.find('table', class_='kqxsmiennam miennam3cot')
        number = 2

    for i in range(number):
        item = {
            "date": date.strftime('%Y-%m-%d'),
            "day_of_week": day_of_week,
            "location":  no_accent_vietnamese(table.find_all("td", class_="tentinh")[i].get_text()),
            "giai_tam": str(table.find_all("td", class_="giai_tam")[i].get_text()),
            "giai_bay": str(table.find_all("td", class_="giai_bay")[i].get_text()),
            "giai_sau_1": str(table.find_all("td", class_="giai_sau")[i].find("div", class_="lq_1").get_text()),
            "giai_sau_2": str(table.find_all("td", class_="giai_sau")[i].find("div", class_="lq_2").get_text()),
            "giai_sau_3": str(table.find_all("td", class_="giai_sau")[i].find("div", class_="lq_3").get_text()),
            "giai_nam": str(table.find_all("td", class_="giai_nam")[i].get_text()),
            "giai_tu_1": str(table.find_all("td", class_="giai_tu")[i].find("div", class_="lq_1").get_text()),
            "giai_tu_2": str(table.find_all("td", class_="giai_tu")[i].find("div", class_="lq_2").get_text()),
            "giai_tu_3": str(table.find_all("td", class_="giai_tu")[i].find("div", class_="lq_3").get_text()),
            "giai_tu_4": str(table.find_all("td", class_="giai_tu")[i].find("div", class_="lq_4").get_text()),
            "giai_tu_5": str(table.find_all("td", class_="giai_tu")[i].find("div", class_="lq_5").get_text()),
            "giai_tu_6": str(table.find_all("td", class_="giai_tu")[i].find("div", class_="lq_6").get_text()),
            "giai_tu_7": str(table.find_all("td", class_="giai_tu")[i].find("div", class_="lq_7").get_text()),
            "giai_ba_1": str(table.find_all("td", class_="giai_ba")[i].find("div", class_="lq_1").get_text()),
            "giai_ba_2": str(table.find_all("td", class_="giai_ba")[i].find("div", class_="lq_2").get_text()),
            "giai_nhi": str(table.find_all("td", class_="giai_nhi")[i].get_text()),
            "giai_nhat": str(table.find_all("td", class_="giai_nhat")[i].get_text()),
            "giai_dac_biet": str(table.find_all("td", class_="giai_dac_biet")[i].get_text()),
        }
        df = df.append(item, ignore_index=True)

    print(date)
    return df


def get_all_data(date):
    global df
    df_resp = get_data_from_minhchinh(date=date)
    df = pd.concat([
        df, df_resp
    ], ignore_index=True).reset_index(drop=True)


if __name__ == "__main__":
    datetime_object = datetime.strptime('01-03-2012', '%d-%m-%Y')
    target_object = None
    # target_object = datetime.strptime('11-03-2012', '%d-%m-%Y')
    now = datetime.now()
    delta = (target_object or now) - datetime_object
    date_range = []
    date_index = datetime_object
    for i in range(delta.days):
        date_range.append(date_index)
        date_index += timedelta(days=1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(get_all_data, date_range)

    # for item in date_range:
    #     get_all_data(item)

    df['date'] = pd.to_datetime(df.date, format="%Y-%m-%d")
    df = df.drop_duplicates(subset=["date", "location"])
    df = df.sort_values('date')
    df.to_csv(f'xskt_mientrung.csv', index=False)
