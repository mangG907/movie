import requests
import os
import pandas as pd

def save2df():
    df = list2df()
    df['load_dt'] = '20120101'
    print(df.head(5))
    df.to_parquet('~/tmp/test_parquet', partition_cols=['load_dt'])
    return df

def list2df():
    l = req2list()
    df = pd.DataFrame(l)
    return df

def req2list() -> list:
    _, data = req()
    l = data['boxOfficeResult']['dailyBoxOfficeList']
    return l

def get_key():
    """영화진흫위원회 가입 및 AIP 키 생성 후 환경변수 선언 필요"""
    key = os.getenv('MOVIE_API_KEY')
    return key

def req(dt='20200101'):
    url = gen_url(dt)
    r = requests.get(url)
    code = r.status_code
    data = r.json()
    print(data)
    return code, data

def gen_url(dt="20120101"):
    base_url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    key = get_key()
    url = f"{base_url}?key={key}&targetDt={dt}"
    return url
