import pandas
import datetime
from collections import defaultdict
from decimal import Decimal
from typing import List
import csv


class Lot:

    def __init__(self, purchase_time: datetime.datetime, asset: str, quantity: float, basis: float):

        self.purchase_time = purchase_time
        self.asset = asset
        self.quantity = quantity
        self.basis = basis


class TaxableEvent:

    def __init__(self,
                 asset: str,
                 purchase_time: datetime.datetime,
                 sold_time: datetime.datetime,
                 basis: float,
                 proceeds: float):

        self.asset = asset
        self.purchase_time = purchase_time
        self.sold_time = sold_time
        self.basis = basis
        self.proceeds = proceeds


class Transaction:

    def __init__(self, asset: str, quantity: Decimal, value: Decimal, tx_time: datetime):
        self.asset = asset
        self.quantity = quantity
        self.value = value
        self.tx_time = tx_time

    def __repr__(self):
        return f'asset={self.asset}, quantity={self.quantity}, value={self.value}, tx_time={self.tx_time}'


class Buy(Transaction):

    def __init__(self, asset: str, quantity: Decimal, value: Decimal, tx_time: datetime):
        super(Buy, self).__init__(asset, quantity, value, tx_time)

    def __repr__(self):
        base = super(Buy, self).__repr__()
        return f'op=BUY, {base}'


class Sell(Transaction):

    def __init__(self, asset: str, quantity: Decimal, value: Decimal, tx_time: datetime):
        super(Sell, self).__init__(asset, quantity, value, tx_time)

    def __repr__(self):
        base = super(Sell, self).__repr__()
        return f'op=SELL, {base}'


class CryptoTaxCalculator:

    def __init__(self):
        pass

    def generate_lifo_taxable_events(self, transactions: List[Transaction]):

        portfolio = defaultdict(list)
        tax_events = []

        for tx in transactions:

            if isinstance(tx, Buy):
                lot = Lot(purchase_time=tx.tx_time,
                          asset=tx.asset,
                          quantity=tx.quantity,
                          basis=tx.value)

                portfolio[tx.asset].append(lot)

            else:
                # we have a sale, which is a taxable event
                asset_lots = portfolio[tx.asset]

                asset_lots = asset_lots.copy()
                asset_lots.reverse()

                remaining_quantity = tx.quantity
                remaining_value = tx.value
                for index, lot in enumerate(asset_lots.copy()):
                    if remaining_quantity == 0:
                        break

                    if lot.quantity <= remaining_quantity:
                        # if the sold amount is greater than what's in this current lot
                        # decrement the count

                        percentage = lot.quantity / remaining_quantity
                        lot_proceeds = remaining_value * percentage

                        remaining_quantity = remaining_quantity - lot.quantity
                        remaining_value = remaining_value - lot_proceeds

                        tax_event = TaxableEvent(asset=tx.asset,
                                                 purchase_time=lot.purchase_time,
                                                 sold_time=tx.tx_time,
                                                 basis=lot.basis,
                                                 proceeds=lot_proceeds)

                        tax_events.append(tax_event)

                        # remove the first element as per fifo
                        asset_lots.pop(0)
                    else:
                        # else, there is more in the lot than there is in the sold transaction
                        percentage = remaining_quantity / lot.quantity
                        lot_basis = lot.basis * percentage

                        lot.quantity = lot.quantity - remaining_quantity
                        lot.basis = lot.basis - lot_basis
                        remaining_quantity = Decimal(0)

                        tax_event = TaxableEvent(asset=tx.asset,
                                                 purchase_time=lot.purchase_time,
                                                 sold_time=tx.tx_time,
                                                 basis=lot_basis,
                                                 proceeds=remaining_value)

                        tax_events.append(tax_event)

        return tax_events

    def generate_fifo_taxable_events(self, transactions: List[Transaction]):

        portfolio = defaultdict(list)
        tax_events = []

        for tx in transactions:

            if isinstance(tx, Buy):
                lot = Lot(purchase_time=tx.tx_time,
                          asset=tx.asset,
                          quantity=tx.quantity,
                          basis=tx.value)

                portfolio[tx.asset].append(lot)

            else:
                # we have a sale, which is a taxable event
                asset_lots = portfolio[tx.asset]

                remaining_quantity = tx.quantity
                remaining_value = tx.value
                for index, lot in enumerate(asset_lots.copy()):
                    if remaining_quantity == 0:
                        break

                    if lot.quantity <= remaining_quantity:
                        # if the sold amount is greater than what's in this current lot
                        # decrement the count

                        percentage = lot.quantity / remaining_quantity
                        lot_proceeds = remaining_value * percentage

                        remaining_quantity = remaining_quantity - lot.quantity
                        remaining_value = remaining_value - lot_proceeds

                        tax_event = TaxableEvent(asset=tx.asset,
                                                 purchase_time=lot.purchase_time,
                                                 sold_time=tx.tx_time,
                                                 basis=lot.basis,
                                                 proceeds=lot_proceeds)

                        tax_events.append(tax_event)

                        # remove the first element as per fifo
                        asset_lots.pop(0)
                    else:
                        # else, there is more in the lot than there is in the sold transaction
                        percentage = remaining_quantity / lot.quantity
                        lot_basis = lot.basis * percentage

                        lot.quantity = lot.quantity - remaining_quantity
                        lot.basis = lot.basis - lot_basis
                        remaining_quantity = Decimal(0)

                        tax_event = TaxableEvent(asset=tx.asset,
                                                 purchase_time=lot.purchase_time,
                                                 sold_time=tx.tx_time,
                                                 basis=lot_basis,
                                                 proceeds=remaining_value)

                        tax_events.append(tax_event)

        return tax_events

    def approximate_tax_bill(self,
                             taxable_events: List[TaxableEvent],
                             long_term_cap_gains_rate=0.15,
                             short_term_cap_gains_rate=0.29):

        long_term_amt = Decimal(0)
        short_term_amt = Decimal(0)
        for event in taxable_events:
            position_time = event.sold_time - event.purchase_time
            profit = event.proceeds - event.basis
            if position_time.days >= 365:
                long_term_amt += Decimal(profit)
            else:
                short_term_amt += Decimal(profit)

        long_term_gains = long_term_amt * Decimal(long_term_cap_gains_rate)
        short_term_gains = short_term_amt * Decimal(short_term_cap_gains_rate)

        tax_bill = short_term_gains + long_term_gains

        print(tax_bill, long_term_amt, long_term_gains, short_term_amt, short_term_gains)


    def parse_binance_transactions(self, txs: pandas.DataFrame) -> List[Transaction]:

        transactions = []
        for index, row in txs.iterrows():

            operation = row['Operation']
            transaction_time = row['Time']

            transaction_time = datetime.datetime.strptime(transaction_time, '%m/%d/%y %I:%M %p')
            if operation not in {'Buy', 'Sell'}:
                continue

            category = row['Category']
            if category == 'Spot Trading':
                asset_type = row['Base_Asset']
                asset_qty = Decimal(row['Realized_Amount_For_Base_Asset'])
            else:
                asset_type = row['Quote_Asset']
                asset_qty = Decimal(row['Realized_Amount_For_Quote_Asset'])

            asset_usd_raw = str(row['Realized_Amount_For_Quote_Asset_In_USD_Value'])
            asset_usd_raw = asset_usd_raw.replace(',', '')
            # print(row['Realized_Amount_For_Quote_Asset_In_USD_Value'])
            asset_usd = Decimal(asset_usd_raw)

            if operation == 'Buy':
                tx = Buy(asset=asset_type,
                         quantity=asset_qty,
                         value=asset_usd,
                         tx_time=transaction_time)

            else:
                tx = Sell(asset=asset_type,
                          quantity=asset_qty,
                          value=asset_usd,
                          tx_time=transaction_time)

            transactions.append(tx)

        return transactions

    def write_taxable_events_to_csv(self, events: List[TaxableEvent], filename: str):

        columns = ['Currency Name', 'Purchase Date', 'Cost Basis', 'Date Sold', 'Proceeds']
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for event in events:
                writer.writerow([
                    event.asset,
                    event.purchase_time.date(),
                    event.basis,
                    event.sold_time.date(),
                    event.proceeds])


def main(filename: str):

    df = pandas.read_csv(filename)

    calculator = CryptoTaxCalculator()
    transactions = calculator.parse_binance_transactions(df)
    fifo_taxable_events = calculator.generate_fifo_taxable_events(transactions)
    lifo_taxable_events = calculator.generate_lifo_taxable_events(transactions)
    calculator.approximate_tax_bill(fifo_taxable_events)
    calculator.approximate_tax_bill(lifo_taxable_events)

    calculator.write_taxable_events_to_csv(lifo_taxable_events, 'lifo_transactions.csv')

if __name__ == '__main__':

    filename = 'data/crypto_transactions.csv'
    main(filename)


