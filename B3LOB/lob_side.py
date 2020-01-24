#!/usr/bin/env python3

import math
import numpy as np
from .types import Order, DBOrder

B3DEBUG = False

class LobSide:
    def __init__(self, psup, ticksize, side):
        self.side = side
        self.ticksize = ticksize
        self.booksize = math.ceil(psup / ticksize)
        self.book = np.zeros(self.booksize, dtype = 'int')
        self.db = {}
        self.psup = psup
        self.debug = []

    def index(self, price):
        """Returns the index of a given price"""
        # XXX trocar por np.floor()?
        return(math.floor(price / self.ticksize))

    def price(self, index):
        """Returns the lowest price of a given index"""
        return(index * self.ticksize)

    def add_debug(self, msg, order):
        self.debug.append((msg, order))

    def add(self, order):
        """Add an order to the database and to the book"""

        if (self.side == 'sell') and (order.price == 0):
            self.add_debug('sell-price-zero', order)
            if B3DEBUG:
                print('sell order with price = 0 ({})'.format(order))
            return

        # Database
        dborder = DBOrder(size = order.size, executed = order.executed,
                          price = order.price, side = order.side)
        self.db[order.seq] = dborder
        
        # Book
        pidx = self.index(dborder.price)
        self.book[pidx] += (order.size - order.executed)

    def remove(self, seq):
        """Removes an order from the database and from the book"""

        # Get current info about the order (from the DB)
        dborder = self.db[seq]
        dbprice = dborder.price
        dbremaining = dborder.size - dborder.executed
        pidx = self.index(dbprice)

        # Book
        if self.book[pidx] < dbremaining:
            self.add_debug('order-neg-size', order)
            raise Exception('negative order amount (seq = {})'.format(seq))

        self.book[pidx] -= dbremaining

        # Database
        del self.db[seq]

    def process_new(self, order):

        if order.seq in self.db:
            self.add_debug('new-order-in-db', order)
            print('new order already in db ({})'.format(order))
            self.remove(order.seq)

        if order.executed != 0:
            self.add_debug('new-order-with-executed', order)
            raise Exception('new order with >0 executed({})'.format(order))

        self.add(order)

    def process_update(self, order):

        if order.seq not in self.db:
            self.add_debug('update-not-in-db', order)
            # print('update not in db ({})'.format(order))
            self.add(order)
            return

        if self.db[order.seq].executed != order.executed:
            self.add_debug('executed-changed-in-update', order)
            # raise Exception(
            #     'executed amount changed in update ({})'.format(order))
        
        self.remove(order.seq)
        self.add(order)
        
    def process_cancel(self, order):

        if order.seq not in self.db:
            self.add_debug('cancel-not-in-db', order)
            return

        self.remove(order.seq)
        
    def process_trade(self, order):

        if order.seq not in self.db:
            self.add_debug('trade-not-in-db', order)
            self.add(order)
            return
        
        if self.db[order.seq].size != order.size:
            self.add_debug('size-change-in-trade', order)
            raise Exception('size changed in trade ({})'.format(order))

        if self.db[order.seq].price != order.price:
            self.add_debug('price-change-in-trade', order)

        self.remove(order.seq)
        self.add(order)

    def process_order(self, order):

        if order.executed > order.size:
            raise Exception('executed > size ({})'.format(order))

        if order.event == 'new':
            self.process_new(order)

        elif order.event == 'update':
            self.process_update(order)

        elif order.event == 'cancel':
            self.process_cancel(order)

        elif order.event == 'trade':
            self.process_trade(order)
            
        elif order.event == 'reentry':
            pass

        elif order.event == 'expire':
            self.process_cancel(order)

        else:
            self.add_debug('unknown-event', order)
            print('unknown event ({})'.format(order))

    def get_best_price_idx(self):
        if self.side == 'buy':
            best_price_idx = max(np.where(self.book > 0)[0])
        elif self.side == 'sell':
            best_price_idx = min(np.where(self.book > 0)[0])

        return best_price_idx

    def get_liquidity(self):
        best_price_idx = self.get_best_price_idx()
        
        if self.side == 'buy':
            idx_t = np.arange(best_price_idx, 0, -1)
        elif self.side == 'sell':
            idx_t = np.arange(best_price_idx, len(self.book))

        idx = idx_t[np.where(self.book[idx_t] > 0)]
            
        return self.price(idx), self.book[idx], np.cumsum(self.book[idx])

    def get_snapshot(self, max_size):
        prices, liq, cum_liq = self.get_liquidity()
        total_liquidity = cum_liq[-1]
        total_size = min(max_size, total_liquidity)

        cum_size = 0
        marg_prices = np.zeros(total_size)
        
        for (cur_price, cur_liq) in zip(prices, liq):
            size_at_cur_price = min(total_size - cum_size, cur_liq)
            cur_price_idx = np.arange(cum_size, cum_size + size_at_cur_price)
            marg_prices[cur_price_idx] = cur_price
            cum_size += size_at_cur_price

        quantity = 1 + np.arange(0, total_size)
        eff_prices = np.cumsum(marg_prices) / quantity
        best_price = prices[0]

        if self.side == 'buy':
            marg_price_impact = marg_prices - best_price
            eff_price_impact = eff_prices - best_price
        elif self.side == 'sell':
            marg_price_impact = best_price - marg_prices
            eff_price_impact = best_price - eff_prices

        return {'best_price' : best_price,
                'eff_prices' : eff_prices,
                'marg_prices' : marg_prices,
                'eff_price_impact' : eff_price_impact,
                'marg_price_impact' : marg_price_impact,
                'quantity' : quantity}
