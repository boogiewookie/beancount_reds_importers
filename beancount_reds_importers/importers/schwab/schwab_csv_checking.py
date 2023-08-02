""" Schwab Checking .csv importer."""

from beancount_reds_importers.libreader import csvreader
from beancount_reds_importers.libtransactionbuilder import banking
from collections import namedtuple
import datetime


class Importer(csvreader.Importer, banking.Importer):
    IMPORTER_NAME = 'Schwab Checking account CSV'

    def custom_init(self):
        self.year, self.month = self.config.get("ym", "").split('-', 2)
        self.max_rounding_error = 0.04
        self.filename_pattern_def = '.*_Checking_Transactions_'
        self.header_identifier = rf'"Transactions  for Checking account {self.config.get("account_number", "")}.*'
        self.date_format = '%m/%d/%Y'
        self.skip_comments = '# '
        self.header_map = {
            "Date":           "date",
            "Type":           "type",
            "Check #":        "checknum",
            "Description":    "payee",
            "Withdrawal (-)": "withdrawal",
            "Deposit (+)":    "deposit",
            "RunningBalance": "balance"
        }
        self.transaction_type_map = {
            "INTADJUST": 'income',
            "TRANSFER": 'transfer',
            "ACH": 'transfer'
        }
        self.skip_transaction_types = ['Journal']
        self.skip_head_rows = 1
        self.skip_data_rows = 2

    def prepare_table(self, rdr):
        rdr = rdr.select('Date', lambda v: (v.startswith(self.month) and v.endswith(self.year)))
        rdr = rdr.addfield('amount',
                           lambda x: "-" + x['Withdrawal (-)'] if x['Withdrawal (-)'] != '' else x['Deposit (+)'])
        rdr = rdr.addfield('memo', lambda x: '')
        return rdr

    def get_balance_statement(self, file=None):
        """Return the balance on the first and last dates"""

        max_date = self.get_max_transaction_date()
        if max_date:
            for row in [self.rdr.namedtuples()[0], self.rdr.namedtuples()[len(self.rdr) - 2]]:
                date = row.date.date() + datetime.timedelta(days=1)
                # See comment in get_max_transaction_date() for explanation of the above line
                Balance = namedtuple('Balance', ['date', 'amount'])
                yield Balance(date, row.balance)
