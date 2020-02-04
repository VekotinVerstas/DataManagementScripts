
API_URL = 'https://api.example.com/api/'
API_KEY = 'XXXXXXXX-YYYY-ZZZZ-AAAA-BBBBBBBBBBBB'

# House name; Street address; Measuring point code
KPL = """House name; Example street 3;1234 
House name; Second street 5;4334"""

KPLIST = [x.split(';')[-1] for x in KPL.splitlines()]

