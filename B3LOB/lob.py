#!/usr/bin/env python3
import os
import gzip
import math
import csv
from datetime import datetime, timedelta
import pickle
import numpy as np

from .types import Order, events, sides, states
from .lob_side import LobSide

class Lob:
    def __init__(self, psup = 12000, ticksize = 1, datadir = None):
        self.psup = psup
        self.booksize = math.ceil(psup / ticksize)
        self.ticksize = ticksize

        self.last_mod = None
        self.session_date = None
        self.orders = []

        self.snapshots = []
        self.snapshot_times = []
        self.snapshot_size = 0
        self.next_snapshot_idx = 0

        if datadir is None:
            self.datadir = os.getcwd()
        else:
            self.datadir = datadir

        self.side = {'buy' : LobSide(psup, ticksize, 'buy'),
                     'sell' : LobSide(psup, ticksize, 'sell')}

    def line_to_order(self, line):
        ticker = line[1].strip()
        prio_date = datetime.strptime('{} {}'.format(line[11], line[6]),
                                      '%Y-%m-%d  %H:%M:%S.%f')
        session_date = line[0]
        seq = int(line[3])
        gen_id = int(line[4])
        side = sides[line[2]]
        event = events[int(line[5])]
        state = states[line[13]]
        condition = int(line[14])
        size = int(int(line[9]) / 100)
        executed = int(int(line[10]) / 100)
        price = int(100 * float(line[8].strip()))

        order = Order(prio_date, session_date, seq, side, event, state,
                      condition, price, size, executed, gen_id)

        return order, ticker
            
    def read_orders_from_files(self, ticker, fnames):
        for fname in fnames:
            full_fname = os.path.join(self.datadir, fname)
            with gzip.open(full_fname, mode = 'rt') as csvfile:
                filtered = filter(lambda line: ticker in line, csvfile)
                csvreader = csv.reader(filtered, delimiter = ';')
                for line in csvreader:
                    if len(line) < 15:
                        continue
                    
                    order, order_ticker = self.line_to_order(line)
                    
                    if self.session_date == None:
                        self.session_date = order.session_date
                    elif self.session_date != order.session_date:
                        raise Exception(
                            'Orders from more than one sesssion ({})'.format(
                                order))

                    if order_ticker == ticker:
                        self.orders.append(order)
                        
            self.orders.sort(key = lambda order: order.prio_date)

    def save_orders_to_pickle(self, fname):
        abs_fname = os.path.join(self.datadir, fname)
        with open(abs_fname, 'wb') as orders_file:
            pickle.dump(self.orders, orders_file)

    def read_orders_from_pickle(self, fname):
        abs_fname = os.path.join(self.datadir, fname)
        with open(abs_fname, 'rb') as orders_file:
            self.orders = pickle.load(orders_file)

        self.session_date = self.orders[0].session_date

    def set_snapshot_times(self, snapshot_times):
        self.snapshot_times = []
        for s_time in snapshot_times:
            if isinstance(s_time, datetime):
                self.snapshot_times.append(s_time)
            elif isinstance(s_time, str):
                self.snapshot_times.append(datetime.strptime(
                    '{} {}'.format(self.session_date, s_time),
                    '%Y-%m-%d %H:%M:%S'))
        self.snapshot_times.sort()

    def set_snapshot_freq(self, interval, max_size = 1000,
                          start = '10:15:00', end = '16:49:00'):

        self.snapshot_size = max_size
        
        t0 = datetime.strptime('{} {}'.format(self.session_date, start),
                               '%Y-%m-%d %H:%M:%S')
        T  = datetime.strptime('{} {}'.format(self.session_date, end),
                               '%Y-%m-%d %H:%M:%S')

        s_times = []
        t = t0
        delta = timedelta(seconds = interval)
        
        while t <= T:
            s_times.append(t)
            t = t + delta

        self.set_snapshot_times(s_times)

    def clean_liquidity(self, b_prices, b_liq, s_prices, s_liq):
        while b_prices[0] >= s_prices[0]:
            trade_size = min(b_liq[0], s_liq[0])
            b_liq[0] -= trade_size
            s_liq[0] -= trade_size
            
            if b_liq[0] == 0:
                b_liq = b_liq[1:]
                b_prices = b_prices[1:]
            
            if s_liq[0] == 0:
                s_liq = s_liq[1:]
                s_prices = s_prices[1:]
                
        return b_prices, b_liq, s_prices, s_liq

    def get_snapshot(self):
        # buy_snapshot = self.side['buy'].get_snapshot(max_size)
        # sell_snapshot = self.side['sell'].get_snapshot(max_size)

        bp, bl = self.side['buy'].get_liquidity()
        sp, sl = self.side['sell'].get_liquidity()
        c_bp, c_bl, c_sp, c_sl = self.clean_liquidity(bp, bl, sp, sl)
        
        buy_snapshot = self.side['buy'].get_eff_prices(c_bp, c_bl, self.snapshot_size)
        sell_snapshot = self.side['sell'].get_eff_prices(c_sp, c_sl, self.snapshot_size)

        best_sell_price = sell_snapshot['best_price']
        best_buy_price = buy_snapshot['best_price']
        bas =  best_sell_price - best_buy_price
        mid_price = (best_sell_price + best_buy_price) / 2
        cum_mos_net = buy_snapshot['cum_mos'] - sell_snapshot['cum_mos']
        cum_mos_abs = buy_snapshot['cum_mos'] + sell_snapshot['cum_mos']

        return {'bas' : bas,
                'mid_price' : mid_price,
                'cum_mos_net': cum_mos_net,
                'cum_mos_abs' : cum_mos_abs,
                'buy_snapshot' : buy_snapshot,
                'sell_snapshot' : sell_snapshot}
        
    def check_snapshot(self, order):
        if len(self.snapshot_times) <= self.next_snapshot_idx:
            return
        
        while order.prio_date > self.snapshot_times[self.next_snapshot_idx]:
            snapshot = self.get_snapshot()
            t = self.snapshot_times[self.next_snapshot_idx]
            self.snapshots.append((t, snapshot))
            self.next_snapshot_idx += 1

    def process_orders(self, tlimit = '16:30'):
        limit = datetime.strptime(
            '{} {}'.format(self.session_date, tlimit),
            '%Y-%m-%d %H:%M')

        for order in self.orders:
            
            if order.prio_date > limit:
                break

            self.check_snapshot(order)

            if self.last_mod and self.last_mod > order.prio_date:
                raise Exception('out of order order ({})'.format(order))
            
            self.side[order.side].process_order(order)

            # if (self.last_mod == None) or (order.prio_date > self.last_mod):
            #     self.last_mod = order.prio_date
            #     if (self.cur_bas != None):
            #         self.bas.append(self.cur_bas)

            # if self.status == 'open':
            #     best_bid_idx = np.where(self.lob['buy'].book > 0)[0][-1]
            #     best_ask_idx = np.where(self.lob['sell'].book > 0)[0][0]

            #     self.cur_bas = (order.prio_date,
            #                     self.lob['buy'].price(best_bid_idx),
            #                     self.lob['buy'].book[best_bid_idx],
            #                     self.lob['sell'].price(best_ask_idx),
            #                     self.lob['sell'].book[best_ask_idx])
