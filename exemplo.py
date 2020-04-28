from B3LOB import Lob
import numpy as np
import os
from datetime import datetime
import matplotlib.pyplot as plt

lob = Lob(datadir='/home/eduardo/MarketData/')
fnames = ['OFER_CPA_20191127.gz', 'OFER_VDA_20191127.gz']
lob.read_orders_from_files('PETR3', fnames)
lob.set_snapshot_freq(60)
lob.process_orders()

snap_times = [e[0] for e in lob.snapshots]
idx_15h = snap_times.index(datetime(2019, 11, 27, 15, 0))

snap_15h = lob.snapshots[idx_15h][1]

size = 1000

bb = snap_15h['buy_snapshot']['book']
buy_idx = np.where(np.cumsum(bb[0,:]) > size)[0][0] + 1
buy_sizes = bb[0,0:buy_idx] * 100
buy_sizes_cum = np.cumsum(buy_sizes)
buy_prices = bb[1,0:buy_idx] / 100

sb = snap_15h['sell_snapshot']['book']
sell_idx = np.where(np.cumsum(sb[0, :]) > size)[0][0] + 1
sell_sizes = sb[0, 0:sell_idx] * 100
sell_sizes_cum = np.cumsum(sell_sizes)
sell_prices = sb[1, 0:sell_idx] / 100

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

ax1.bar(buy_prices, buy_sizes, label='Ordens de Compra',
        color='steelblue', width=0.008)
ax1.bar(sell_prices, sell_sizes, label='Ordens de Venda',
        color='tomato', width=0.008)
ax1.legend(loc='lower left')
ax1.set_title('Livro de ordens da PETR3 às 15h')

ax2.bar(buy_prices, buy_sizes_cum,
        label='Ordens de Compra',
        color='steelblue', width=0.008)
ax2.bar(sell_prices, sell_sizes_cum,
        label='Ordens de Venda',
        color='tomato', width=0.008)
ax2.legend(loc='lower left')
ax2.set_title('Livro de ordens (acumulado) da PETR3 às 15h')

fig.show()
