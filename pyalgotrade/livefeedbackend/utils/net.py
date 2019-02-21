import gzip
import json
import logging

import requests
import six

import six.moves.http_cookiejar as cookielib
import six.moves.urllib.parse as parse
import six.moves.urllib.request as request

logger = logging.getLogger('pyalgotrade.livefeedbackend')


class WebRequest(object):

    def __init__(self, base_url, encoding='utf-8'):
        self._base_url = base_url
        self.param_dict = {}
        self._header = {
            'User-Agent':
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
        }
        self.encoding = encoding

    def add_param(self, key, val):
        self.param_dict[key] = val

    def add_header(self, key, val):
        self._header[key] = val

    def download_page(self):
        ''' real time quote
        '''
        try:
            if self.param_dict == {}:
                params = ''
            else:
                params = parse.urlencode(self.param_dict)
            req = request.Request(self._base_url + params,
                                  headers=self._header)
            #logger.warning(self._base_url + params)
            response = request.urlopen(req)
            if response.info().get('Content-Encoding') == 'gzip':
                buf = six.BytesIO(response.read())
                f = gzip.GzipFile(fileobj=buf)
                data = f.read()
            else:
                data = response.read()
            return data.decode(self.encoding)
        except Exception as e:
            logger.error('error downloading page %s' % str(e))
            return None


class WebRequest2(object):
    BASIC_HEADERS = {
        'User-Agent':
            ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6)' +
             ' AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'),
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'keep-alive',
    }

    def __init__(self, encoding='utf-8'):
        self.cj = cookielib.CookieJar()
        self.opener = request.build_opener(
            request.HTTPCookieProcessor(self.cj))
        self.encoding = encoding

    def download_page(self, url, param_dict=None, headers=None):
        try:
            if headers is None:
                headers = {}
            if param_dict == {} or param_dict is None:
                params = ''
            else:
                params = parse.urlencode(param_dict)
            tmp_headers = headers.copy()
            tmp_headers.update(WebRequest2.BASIC_HEADERS)
            req = request.Request(
                url + params, headers=tmp_headers)
            response = self.opener.open(req)
            if response.info().get('Content-Encoding') == 'gzip':
                buf = six.BytesIO(response.read())
                f = gzip.GzipFile(fileobj=buf)
                data = f.read()
            else:
                data = response.read()
            return data.decode(self.encoding)
        except Exception as e:
            logger.error('error downloading page %s' % str(e))
            return None


def jsondata2dict(data):
    #logger.debug('data: %s' % data)
    if not data:
        return None
    try:
        return json.loads(data)
    except Exception:
        logger.error('error converting json data to dict')
        return None
