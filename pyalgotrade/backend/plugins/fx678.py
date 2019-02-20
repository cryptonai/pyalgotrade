#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import datetime as dt
import json
import random

import pandas as pd
import six

import six.moves.urllib.parse as parse
import six.moves.urllib.request as request
from pyalgotrade.backend.const.col_names import StockColumns
from pyalgotrade.backend.const.market import MarketCommonSymbols
from pyalgotrade.backend.tools.date import date2timestamp
from pyalgotrade.backend.query_layer import register_agent, MarketDataProvider
from pyalgotrade.bar import Frequency

logger = logging.getLogger('pyalgotrade.backend')


HIST_BASE_URL = 'http://api.q.fx678.com/tradingview/api/history?'
QUOTE_BASE_URL = 'http://api.q.fx678.com/getQuote.php?'
HEADERS = {
    'User-Agent':
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0',
    'Origin': 'http://quote.fx678.com',
    'Host': 'api.q.fx678.com',
    #-H 'Referer: http://quote.fx678.com/HQApi/XAU'
    }

# uncomment the following line to register
@register_agent
class Fx678Provider(MarketDataProvider):

    supported_symbols = {
        MarketCommonSymbols.GOLD_SPOT: ['XAU', 'WGJS'],
        MarketCommonSymbols.SILVER_SPOT: ['XAG', 'WGJS'],
        MarketCommonSymbols.OIL_WTI: ['CONC', 'NYMEX'],
        MarketCommonSymbols.USD_INDEX: ['USD', 'WH'],
        MarketCommonSymbols.NASDAQ: ['NASDAQ', 'GJZS'],
        MarketCommonSymbols.DOWJOHNS: ['DJIA', 'GJZS'],
        MarketCommonSymbols.GOLD_COMEX: ['GLNC', 'COMEX'],
        MarketCommonSymbols.SILVER_COMEX: ['SLNC', 'COMEX'],
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _build_headers(self, symbol):
        tmp = HEADERS.copy()
        tmp.update({'Referer': 'http://quote.fx678.com/HQApi/%s' % symbol})
        #print(tmp)
        return tmp

    def _support_symbol(self, symbol):
        return symbol in self.supported_symbols.keys()

    def quote(self, symbol):
        if not self._support_symbol(symbol):
            raise NotImplementedError()
        match = []
        orig_symbol = symbol
        for i in self.supported_symbols.keys():
            if i.lower().find(symbol.lower()) != -1:
                match.append(i)
        symbol = self.supported_symbols[match[0]][0]
        exchange_name = self.supported_symbols[match[0]][1]
        api_quote_dict = self._build_quote(symbol, exchange_name)
        if not api_quote_dict:
            raise Exception('invalid quote')
        req = request.Request(
            QUOTE_BASE_URL + parse.urlencode(api_quote_dict),
            headers=self._build_headers(symbol))
        #logger.debug(QUOTE_BASE_URL + parse.urlencode(api_quote_dict))
        response = request.urlopen(req)
        data = response.read()
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        jsondata = json.loads(data)
        assert 's' in jsondata.keys()
        assert 't' in jsondata.keys()
        assert 'c' in jsondata.keys()
        assert 'o' in jsondata.keys()
        assert 'h' in jsondata.keys()
        assert 'l' in jsondata.keys()
        assert 'p' in jsondata.keys()
        assert 'v' in jsondata.keys()
        assert 'b' in jsondata.keys()
        assert 'se' in jsondata.keys()
        if jsondata['s'] != 'ok':
            raise Exception('download data failed')
        # print(jsondata)
        raw_data = jsondata
        df = self._get_dataframe(raw_data)
        return pd.Panel({orig_symbol: df})


    def history(self, symbol, start, end, time_series):
        if not self._support_symbol(symbol):
            raise NotImplementedError()
        match = []
        orig_symbol = symbol
        for i in self.supported_symbols.keys():
            if i.lower().find(symbol.lower()) != -1:
                match.append(i)
        assert(len(match) > 0)
        symbol = self.supported_symbols[match[0]][0]
        if start is None:
            start = dt.datetime.fromtimestamp(0)
        else:
            start = dt.datetime.fromtimestamp(date2timestamp(start))
        if end is None:
            end = dt.datetime.now()
        else:
            end = dt.datetime.fromtimestamp(date2timestamp(end))
        exchange_name = self.supported_symbols[match[0]][1]
        api_query_dict = self._build_query(symbol, start, end, time_series)
        if not api_query_dict:
            raise Exception('invalid query')
        logger.debug(HIST_BASE_URL + parse.urlencode(api_query_dict))
        req = request.Request(HIST_BASE_URL + parse.urlencode(api_query_dict),
                              headers=self._build_headers(symbol))
        response = request.urlopen(req)
        data = response.read()
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        data = json.loads(data)
        jsondata = json.loads(data)
        assert 't' in jsondata.keys()
        assert 'o' in jsondata.keys()
        assert 'h' in jsondata.keys()
        assert 'l' in jsondata.keys()
        assert 'c' in jsondata.keys()
        assert 's' in jsondata.keys()
        if jsondata['s'] != 'ok':
            raise Exception('download data failed')
        raw_data = jsondata
        df =  self._get_dataframe(raw_data)
        return pd.Panel({orig_symbol: df})


    def search_symbol(self, value):
        raise NotImplementedError()


    def _build_quote(self, symbol, exchange_name):
        '''
        Build a quote string from symbol
        Example:
        exchName=WGJS&symbol=XAU&st=0.8725721536786181
        '''
        apidict = dict()
        apidict['symbol'] = symbol
        apidict['exchName'] = exchange_name
        apidict['st'] = random.random()
        return apidict


    def _build_query(self, symbol, start, end, time_series):
        '''
        Build a query string from symbol and time series
        :param sym: commodity symbol
        :param time_series: time series enum
        :return: an encoded string URL
        '''

        apidict = dict()
        resolution = [
            [Frequency.MINUTE, '1'],
            [Frequency.HOUR, '60'],
            [Frequency.DAY, 'D'],
            [Frequency.WEEK, 'W'],
            [Frequency.MONTH, 'M']
        ]
        apidict['symbol'] = symbol
        apidict['resolution'] = None
        for i in resolution:
            if time_series == i[0]:
                apidict['resolution'] = i[1]
        assert apidict['resolution'] is not None
        apidict['from'] = start.strftime('%s')
        apidict['to'] = end.strftime('%s')
        return apidict


    def _get_dataframe(self, raw_data):
        """
        Convert raw json data to dataframe
        :return:
        """
        if raw_data is None:
            raise Exception('no data downloaded')
        df = pd.DataFrame()
        if len(raw_data['t']) == 0:
            return None
        tmp = zip(raw_data['t'], raw_data['o'],
                  raw_data['h'], raw_data['l'],
                  raw_data['c'], raw_data['v'])
        for i in tmp:
            t, o, h, l, c, v = i
            df.loc[t, StockColumns.OPEN] = o
            df.loc[t, StockColumns.HIGH] = h
            df.loc[t, StockColumns.LOW] = l
            df.loc[t, StockColumns.CLOSE] = c
            df.loc[t, StockColumns.VOLUME] = v
        df.index.name = StockColumns.DATE_IDX
        df.iloc[:, :] = df.iloc[:, :].astype(float)
        df.iloc[:, 4] = df.iloc[:, 4].astype(int)
        df.index = pd.to_datetime(df.index, utc=True, unit='s')
        return df.sort_index()


def test():
    test = Fx678Provider()
    rtn = test.history(MarketCommonSymbols.GOLD_SPOT, None, None, Frequency.DAY)
    print(rtn)
