""" Discover bank .csv importer."""

from beancount_reds_importers.libreader import csvreader
from beancount_reds_importers.libtransactionbuilder import banking
from beancount.core.number import D


class Importer(csvreader.Importer, banking.Importer):
    IMPORTER_NAME = """ Discover savings .csv importer."""

    def custom_init(self):
        self.max_rounding_error = 0.04
        self.filename_pattern_def = 'Discover.*'
        self.header_identifier = 'Transaction Date,Transaction Description,Transaction Type,Debit,Credit,Balance'
        self.date_format = '%m/%d/%Y'
        self.header_map = {
            "Transaction Date": 'date',
            "Transaction Description": 'memo',
            "Balance": 'balance',
            }

    def skip_transaction(self, ot):
        return False

    def prepare_processed_table(self, rdr):
        rdr = rdr.addfield('payee', '')
        rdr = rdr.addfield('amount',
                lambda x: D(x['Credit'].replace('$', '')) - D(x['Debit'].replace('$', '')))
        return rdr
