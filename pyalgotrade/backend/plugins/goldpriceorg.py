#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import datetime as dt
import json
import six.moves.urllib.parse as parse
import six.moves.urllib.request as request

from pyalgotrade.backend.const.col_names import StockColumns
from pyalgotrade.backend.const.market import MarketCommonSymbols
from pyalgotrade.backend.tools.date import date2timestamp
from pyalgotrade.backend.query_layer import register_agent, MarketDataProvider
from pyalgotrade.bar import Frequency
from pyalgotrade.backend.utils import net

logger = logging.getLogger('pyalgotrade.backend')



@register_agent
class GoldPriceOrgProvider(MarketDataProvider):
    REQUEST_URL = 'https://data-asg.goldprice.org/dbXRates/USD,USD,USD,USD,USD'

    lookup_table = { MarketCommonSymbols.GOLD_SPOT: 'xau', MarketCommonSymbols.SILVER_SPOT: 'xag'}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def quote(self, symbol):
        try:
            if not isinstance(symbol, str):
                logger.error('symbol is not string')
                return None
            return self._quote(symbol)
        except Exception as e:
            logger.error('error %s' % str(e))
            return None

    def _quote(self, symbol):
        assert isinstance(symbol, str)
        symbol_orig = symbol
        if symbol in GoldPriceOrgProvider.lookup_table.keys():
            symbol = GoldPriceOrgProvider.lookup_table[symbol]
        if symbol.lower() != 'xau' and symbol.lower() != 'xag':
            logger.warning('%s symbol %s is not supported' %
                        (self.__class__.__name__, symbol))
            return None
        req = net.WebRequest(GoldPriceOrgProvider.REQUEST_URL)
        ret = req.download_page()
        ret = net.jsondata2dict(ret)
        if not ret:
            logger.error('no data received')
            return None
        for i in ['ts', 'tsj', 'date', 'items']:
            if i not in ret:
                logger.error('invalid data format %s' % i)
                return None
        if len(ret['items']) != 1:
            logger.error('invalid data length')
            return None
        items = ret['items'][0]
        for i in ['curr', 'xauPrice', 'xagPrice', 'chgXau', 'chgXag',
                  'pcXau', 'pcXag', 'xauClose', 'xagClose']:
            if i not in items:
                logger.error('invalid data format %s' % i)
                return None
        t = int(ret['tsj'])
        df = pd.DataFrame()
        if symbol.lower() == 'xau':
            # XAU
            df.loc[t, StockColumns.CLOSE] = items['xauPrice']
            df.loc[t, StockColumns.CHANGE_PERCENT] = items['pcXau']
        else:
            # XAG
            df.loc[t, StockColumns.CLOSE] = items['xagPrice']
            df.loc[t, StockColumns.CHANGE_PERCENT] = items['pcXag']
        df.index.name = StockColumns.DATE_IDX
        df.iloc[:, :] = df.iloc[:, :].astype(float)
        df.index = pd.to_datetime(df.index, utc=True, unit='ms')
        return pd.Panel({symbol_orig: df})

    def history(self, symbol, start, end, time_series):
        raise NotImplementedError()

    def search_symbol(self, value):
        raise NotImplementedError()


