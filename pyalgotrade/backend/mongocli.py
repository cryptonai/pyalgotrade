#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime as dt
import json

import pandas as pd

import pymongo
import pytz
from pymongo import ASCENDING, DESCENDING, IndexModel, MongoClient
import logging
from pyalgotrade.bar import Frequency

logger  = logging.getLogger('pyalgotrade.backend')

class MongoHistoryData(object):
    ''' This is the class to save history data and load history data
        from mongodb
    '''
    def __init__(self, db_name, collection_name, drop_if_exists=False):
        self.client = MongoClient()
        self.collection = self.client[db_name][collection_name]
        if not collection_name in self.client[db_name].list_collection_names():
            self.collection.create_index([('date', ASCENDING),
                            ('symbol', ASCENDING),
                            ('freq', ASCENDING),
                            ('source', ASCENDING)], unique=True)
        else:
            if drop_if_exists:
                self.collection.drop()
                self.collection.create_index([('date', ASCENDING),
                                ('symbol', ASCENDING),
                                ('freq', ASCENDING),
                                ('source', ASCENDING)], unique=True)
        return super(MongoHistoryData, self).__init__()

    def first_date(self, symbol, freq, source=''):
        return self._find_date(symbol, freq, source, False)
    
    def last_date(self, symbol, freq, source=''):
        return self._find_date(symbol, freq, source, True)

    def _find_date(self, symbol, freq, source, last):
        condition = {}
        if symbol:
            condition['symbol'] = {'$eq': symbol}
        if freq:
            if isinstance(freq, int):
                condition['freq'] = {'$eq': freq}
            else:
                condition['freq'] = {'$eq': int(freq)}
        if source:
            condition['source'] = {'$eq': source}
        if last:
            cursor = self.collection.find(condition).sort([('date', -1)]).limit(1)
        else:
            cursor = self.collection.find(condition).sort([('date', 1)]).limit(1)
        if cursor.count() == 0:
            return None
        tmp = cursor[0]
        timezone = None
        if 'timezone' in tmp:
            tzstr = tmp['timezone']
            timezone = pytz.timezone(tzstr)
        tmp = pd.to_datetime(tmp['date'], unit='ms')
        if timezone:
            tmp = tmp.tz_localize(timezone)
        return tmp

    def save(self, df, symbol, freq, source='', update_days=5, validate=False):
        ''' save dataframe to mongodb
        '''
        if df is None:
            return
        assert isinstance(df, pd.DataFrame)
        if df.empty:
            return
        tmp = df.copy()
        tmp.reset_index(level=0, inplace=True)
        timezone = tmp['date'].iloc[0].tz
        if timezone:
            tzstr = str(timezone.zone)
        tmpjson = json.loads(tmp.to_json(orient='records'))
        for i in tmpjson:
            #print(i)
            i['symbol'] = symbol
            if freq:
                if isinstance(freq, int):
                    i['freq'] = freq
                else:
                    i['freq'] = int(freq)
            i['timezone'] = tzstr
            i['source'] = source
            try:
                self.collection.insert_one(i)
            except pymongo.errors.DuplicateKeyError:
                if isinstance(i['date'], int):
                    try:
                        tmpdatetime = dt.datetime.fromtimestamp(i['date'] / 1000.0)
                    except ValueError:
                        tmpdatetime = dt.datetime.fromtimestamp(i['date'])
                if (dt.datetime.utcnow() - tmpdatetime).days <= update_days:
                    if '_id' in i:
                        i.pop('_id')
                    self.collection.update_one(
                        {k: i[k] for k in ('symbol', 'freq', 'source', 'date')},
                        {'$set': i}
                    )
                    logger.debug('update one record!')
                else:
                    if validate:
                        cursor = self.collection.find({
                            'symbol': {'$eq': i['symbol']},
                            'freq': {'$eq': i['freq']},
                            'source': {'$eq': i['source']},
                            'date': {'$eq': i['date']},
                        })
                        if cursor.count() >= 1:
                            obj = cursor[0]
                            for k, v in i.items():
                                if k != '_id':
                                    if v != obj[k]:
                                        logger.error('potential data corruption!')


    def load(self, symbol, freq, start=None, end=None, source='', cleanup=True):
        ''' load data from mongodb and save to dataframe
        '''
        tmp = []
        condition = {}
        if symbol:
            condition['symbol'] = {'$eq': symbol}
        if freq:
            if isinstance(freq, int):
                condition['freq'] = freq
            else:
                condition['freq'] = int(freq)
        if source:
            condition['source'] = {'$eq': source}
        if start:
            if isinstance(start, dt.datetime):
                condition['date'] = {'$gte': start.timestamp() * 1000}
            else:
                raise Exception('not supported time %s' % type(start))
        if end:
            if isinstance(end, dt.datetime):
                if 'date' in condition:
                    condition['date'].update({'$lt': end.timestamp() * 1000})
                else:
                    condition['date'] = {'$lt': end.timestamp() * 1000}
            else:
                raise Exception('not supported time %s' % type(end))
        #print(condition)
        cursor = self.collection.find(condition)
        for i in cursor:
            tmp.append(i)
        tmpdf = pd.DataFrame.from_dict(tmp)
        if not tmpdf.empty and cleanup:
            timezone = None
            if 'timezone' in tmpdf.columns:
                tzstr = tmpdf['timezone'].iloc[0]
                timezone = pytz.timezone(tzstr)
            #print(tmpdf)
            tmpdf.drop(columns=['_id', 'symbol', 'freq',
                                'timezone'], inplace=True)
            tmpdf.set_index('date', inplace=True)
            tmpdf.index = pd.to_datetime(tmpdf.index, unit='ms')
            if timezone:
                tmpdf.index = tmpdf.index.tz_localize(timezone)
        return tmpdf
