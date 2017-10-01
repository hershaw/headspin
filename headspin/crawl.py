import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import requests
from elasticsearch import Elasticsearch
es = Elasticsearch()


_index = 'headspin'
doc_type = 'entry'


def _count(timestamp, domain):

    return es.count(index=_index, doc_type=doc_type, body={
        'query': {
            'bool': {
                'filter': [
                    {'term': {'domain': domain}},
                    {'term': {'dt': _es_time_format(timestamp)}}
                ]
            }
        }
    })['count']


def _get_sites():
    with open('./sites.txt') as fh:
        sites_to_crawl = list(map(str.strip, fh.readlines()))
    return sites_to_crawl


def _create_bs(url):
    r = requests.get(url)
    r.raise_for_status()
    return BeautifulSoup(r.text)


def _clean_entry(text):
    return text.strip().replace('\n', '')


def _get_clean_list(tag, bs):
    return list(filter(None, [
        _clean_entry(x.get_text())
        for x in bs.find_all(tag)
    ]))


def _fetch_phrases(bs):
    tags_to_scrape = {
        'h1',
        'h2',
        'h3',
        'h4',
        'p',
        'li',
    }
    return {
        _type: _get_clean_list(_type, bs)
        for _type in tags_to_scrape
    }


def _has_wayback_content(bs):
    return True


def _store_phrases(timestamp, domain, phrases):
    print('storing phrases', domain)
    for key in phrases:
        es.index(
            index=_index, doc_type=doc_type, body={
                'dt': _es_time_format(timestamp),
                'domain': domain,
                'tag': key,
                'contents': phrases[key]
            }
        )


def _es_time_format(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H')


def _wayback_query(timestamp, site):
    time = datetime.datetime.fromtimestamp(timestamp).strftime('%Y%m%d%H')
    r = requests.get(f'http://web.archive.org/web/{time}/{site}')
    return BeautifulSoup(r.text, 'html.parser')


def crawl_sites(timestamp):
    for site in _get_sites():
        domain = urlparse(site).netloc
        bs = _wayback_query(timestamp, site)
        if _has_wayback_content(bs) and _count(timestamp, domain) == 0:
            _store_phrases(timestamp, domain, _fetch_phrases(bs))


crawl_sites(datetime.datetime(year=2015, month=1, day=1).timestamp())
