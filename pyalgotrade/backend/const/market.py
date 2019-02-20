#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data column names constants
"""
from enum import Enum


class MarketCommonSymbols:
    """
    Market common symbols such as gold and silver
    """
    GOLD_COMEX = 'comex_gold'
    GOLD_SPOT = 'spot_gold'
    SILVER_SPOT = 'spot_silver'
    SILVER_COMEX = 'comex_silver'
    OIL_WTI = 'wti_oil'
    USD_INDEX = 'usd_index'
    EURO_USD = 'euro/usd'
    NASDAQ = 'nasdaq'
    DOWJOHNS = 'dow'

    USTREASURY_YIELD_10YEAR = 'us_treasury_yield_10year'
    USTREASURY_YIELD_2YEAR  = 'us_treasury_yield_2year'

    FRED_GDP                    = 'fred_gdp'
    FRED_M1                     = 'fred_m1'
    FRED_M2                     = 'fred_m2'
    FRED_UNEMPLOYMENT_RATE      = 'fred_unemployment_rate'
    FRED_10YEAR_INFLATION_RATE  = 'fred_10year_breakeven_inflation_rate'

    BITCOIN                     = 'bitcoin'