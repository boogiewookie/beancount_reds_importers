"""Amex credit card and savings ofx importer for beancount."""

from beancount_reds_importers.libreader import ofxreader
from beancount_reds_importers.libtransactionbuilder import banking


class Importer(banking.Importer, ofxreader.Importer):
    IMPORTER_NAME = 'American Express'

    def custom_init(self):
        if not self.custom_init_run:
            self.max_rounding_error = 0.04
            self.filename_pattern_def = '.*amex'
            self.flip_debit_sign = self.config.get("flip_debit_sign", False)
            self.custom_init_run = True

    def get_transactions(self):
        """Make the amount match the type."""
        for ot in self.ofx_account.statement.transactions:
            if ot.type == 'debit' and self.flip_debit_sign:
                ot.amount = -ot.amount
            yield ot

