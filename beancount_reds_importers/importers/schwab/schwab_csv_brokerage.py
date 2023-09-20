""" Schwab Brokerage .csv importer."""

import re
from decimal import Decimal
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
            'CD Deposit Adj':               'selldebt',
            'CD Deposit Funds':             'selldebt',
            'CXL Redemption Adj':           'selldebt',
            'Full Redemption':              'selldebt',
            'Full Redemption Adj':          'selldebt',
            'Redemption Adj':               'selldebt',
            }
        self.skip_transaction_types = ['Journal']
        self.skip_head_rows = 0
        self.acctmap = self.config.get("acctmap", None)

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
        return re.search(
            r'"Date","Action","Symbol","Description","Quantity","Price","Fees & Comm","Amount"',
            head) and re.search(self.config.get("filename_pattern", ""), file.name)

    def get_transactions(self):
        half_sale = {}
        for ot in self.rdr.namedtuples():
            if self.skip_transaction(ot):
                continue
            if ot.type == 'selldebt':
                key = f"{ot.tradeDate}_{ot.security}"
                if key not in half_sale:
                    half_sale[key] = ot
                    continue
                else:
                    if not ot.units:
                        ot = ot._replace(units=half_sale[key].units)
                    elif not ot.total:
                        ot = ot._replace(total=half_sale[key].total)
                    if not ot.unit_price:
                        ot = ot._replace(unit_price=Decimal('1.00'))
                    del half_sale[key]
            yield ot

        # just in case there are any unclosed halves left
        for ot in half_sale.values():
            yield ot

    def prepare_raw_file(self, rdr):
        # pre 2023-08 history files have a title line, later do not
        if rdr[0][0].startswith('Transactions'):
            rdr = rdr.skip(1)
        return rdr

