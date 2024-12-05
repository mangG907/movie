import requests
import os
import pandas as pd


def echo(yaho):
    return yaho

def apply_type2df(load_dt="20120101", path="~/tmp/test_parquet"):
    df = pd.read_parquet(f'{path}/load_dt={load_dt}')
    df['rnum'] = pd.to_numeric(df['rnum'])
    df['rank'] = pd.to_numeric(df['rank'])
    num_cols = ['rnum', 'rank', 'rankInten', 'salesAmt', 'audiCnt',  
                'audiAcc', 'scrnCnt', 'showCnt', 'salesShare', 'salesInten', 
                'salesChange', 'audiInten', 'audiChange']
    for c in num_cols:
        df[c] = pd.to_numeric(df[c])
        
    #df[num_cols] = df[num_cols].apply(pd.to_numeric)    
    return df

def save_data(ds_nodash):
    from mov.api.call import apply_type2df

    df = apply_type2df(load_dt=ds_nodash)

    #print("*" * 33)
    #print(df.head(10))
    #print("*" * 33)

    #print(df.dtypes)

    # 개봉일 기준 그룹핑 누적 관객수 합
    g = df.groupby('openDt')
    sum_df = g.agg({'audiCnt' : 'sum'}).reset_index()
    print(sum_df)



def save2df(load_dt='20120101', url_param={}):
    """airflow 호출 지점"""
    df = list2df(load_dt, url_param)
    df['load_dt'] = load_dt
    #print(df.head(5))
    df.to_parquet('~/tmp/test_parquet', partition_cols=['load_dt'])
    return df

def list2df(load_dt='20120101', url_param={}):
    l = req2list(load_dt, url_param)
    df = pd.DataFrame(l)
    return df

def req2list(load_dt='20120101', url_param={}) -> list:
    _, data = req(load_dt, url_param)
    l = data['boxOfficeResult']['dailyBoxOfficeList']
    df = pd.DataFrame(l)
    return df 

def get_key():
    """영화진흥위원회 가입 및 AIP 키 생성 후 환경변수 선언 필요"""
    key = os.getenv('MOVIE_API_KEY')
    #print(key)
    return key

def req(load_dt='20120101', url_param={}):
    url = gen_url(load_dt, url_param)
    r = requests.get(url)
    code = r.status_code
    data = r.json()
    print(data)
    return code, data

def gen_url(load_dt='20120101', url_param={}):
    base_url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    key = get_key()
    url = f"{base_url}?key={key}&targetDt={load_dt}"
    for key, value in url_param.items():
        url = url + f"&{key}={value}"

    #print("*" * 10)
    #print(url)
    #print("*" * 10)
    return url
