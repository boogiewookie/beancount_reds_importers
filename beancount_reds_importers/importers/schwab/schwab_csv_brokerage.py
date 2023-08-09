""" Schwab Brokerage .csv importer."""

import re
from beancount_reds_importers.libreader import csvreader
from beancount_reds_importers.libtransactionbuilder import investments


class Importer(csvreader.Importer, investments.Importer):
    IMPORTER_NAME = 'Schwab Brokerage CSV'

    def custom_init(self):
        self.year, self.month = self.config.get("ym", "").split('-', 2)
        self.max_rounding_error = 0.04
        self.filename_pattern_def = '.*_Transactions_'
        self.header_identifier = rf'"Transactions  for account {self.config.get("account_number", "")}.*'
        self.get_ticker_info = self.get_ticker_info_from_id
        self.date_format = '%m/%d/%Y'
        self.funds_db_txt = 'funds_by_ticker'
        self.get_payee = lambda ot: ot.type
        self.header_map = {
            "Action":      'type',
            "Date":        'date',
            "Description": 'memo',
            "Symbol":      'security',
            "Quantity":    'units',
            "Price":       'unit_price',
            "Amount":      'amount',
            # "tradeDate":   'tradeDate',
            # "total":       'total',
            "Fees & Comm": 'fees',
            }
        self.transaction_type_map = {
            'Bank Interest':                'income',
            'Bond Interest':                'income',
            'CD Interest':                  'income',
            'Bank Transfer':                'transfer',
            'Buy':                          'buystock',
            'Reinvestment Adj':             'buystock',
            'Cash Dividend':                'dividends',
            'Credit Interest':              'income',
            'Div Adjustment':               'dividends',
            'Long Term Cap Gain Reinvest':  'capgainsd_lt',
            'Non-Qualified Div':            'dividends',
            'Pr Yr Non-Qual Div':           'dividends',
            'Misc Credits':                 'transfer',
            'MoneyLink Deposit':            'transfer',
            'MoneyLink Transfer':           'transfer',
            'Pr Yr Div Reinvest':           'dividends',
            'Qualified Dividend':           'dividends',
            'Reinvest Dividend':            'dividends',
            'Reinvest Shares':              'buystock',
            'Sell':                         'sellstock',
            'Short Term Cap Gain Reinvest': 'capgainsd_st',
            'Wire Funds Received':          'transfer',
            'Wire Received':                'transfer',
            'Funds Received':               'transfer',
            'Stock Split':                  'transfer',
            'Spin-off':                     'transfer',  # TODO: not handled correctly
            'Cash In Lieu':                 'transfer',  # TODO: not handled correctly
            'CD Deposit Adj':               'transfer',
            'CD Deposit Funds':             'transfer',
            }
        self.skip_transaction_types = ['Journal']
        self.skip_head_rows = 1
        self.acctmap = {
                "...164": "0000440000810164",
                "Kevet Inherited IRA ...020":   "25835020",
                "Kevet Inherited IRA XXXX-5020":"25835020",
                "Kevet PledgedAsset ...972":    "30589972",
                "Kevet PledgedAsset XXXX-9972": "30589972",
                "Kevet RothIRA ...218":         "90481218",
                "Kevet RothIRA XXXX-1218":      "90481218",
                "Kevet Schwab One ...772":      "29939772",
                "Kevet Schwab One XXXX-9772":   "29939772",
                "Kevet SchwabIRA ...775":       "29939775",
                "Kevet SchwabIRA XXXX-9775":    "29939775",
                "Sandy IRA ...635":             "29943635",
                "Sandy-SchwabIRA ...635":       "29943635",
                "Sandy IRA XXXX-3635":          "29943635",
                "Sandy Inh IRA ...348":         "68841348",
                "Sandy-Inh.IRA ...348":         "68841348",
                "Sandy Inh IRA XXXX-1348":      "68841348",
                "Sandy-InheritedIRA ...348":    "68841348",
                "Sandy Schwab One ...874":      "80268874",
                "Sandy Schwab One XXXX-8874":   "80268874",
                "Sandy-SchwabTinman ...420":    "94073420",
                "Sandy-SchwabTaxable ...420":   "94073420",
                "Sandy-SchwabTaxable XXXX-3420":"94073420",
                }

    def prepare_table(self, rdr):
        if '' in rdr.fieldnames():
            rdr = rdr.cutout('')  # clean up last column

        def cleanup_date(d):
            """'11/16/2018 as of 11/15/2018' --> '11/16/2018'"""
            return d.split(' ', 1)[0]

        
        rdr = rdr.convert('Date', cleanup_date)
        rdr = rdr.select('Date', lambda v: (v.startswith(self.month) and v.endswith(self.year)))
        rdr = rdr.addfield('tradeDate', lambda x: x['Date'])
        rdr = rdr.addfield('total', lambda x: x['Amount'])
        return rdr

    def deep_identify(self, file):
        head = file.head()
        want = self.config.get("account_number", "")
        for raw, acct in self.acctmap.items():
            if acct == want and re.match(rf'"Transactions  for account {raw}.*', head):
                return True
        return False
