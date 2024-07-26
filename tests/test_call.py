# 테스트 코드는 검증하고 싶은 경로의 .py의 함수들을 import 한다. 

from mov.api.call import gen_url, req, get_key


def test_비밀키숨기기():
    key = get_key()
    assert key

def test_gen_url():
    url = gen_url()
    assert "http" in url 
    assert "kobis" in url

def test_req():
    code, data = req()
    assert code == 200
