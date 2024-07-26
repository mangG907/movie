import requests
import os
import pandas

def list2df():
    l = req2dataframe()
    df = pd.DataFrame(l)
    return df

def req2dataframe():
    _, data = req()
    l = data['boxOfficeResult']['dailyBoxOfficeList']
    pf = list2df(l)
    return pf

def get_key():
    key = os.getenv('MOVIE_API_KEY')
    return key

def req():
    url = gen_url()
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
