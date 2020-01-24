#!/usr/bin/env python3
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Order:
    prio_date: datetime
    session_date: str
    seq: int
    side: str
    event: str
    state: str
    condition: int
    price: int
    size: int
    executed: int
    gen_id: int

@dataclass
class DBOrder:
    size: int
    executed: int
    price: int
    side: str

events = {1 : 'new', 2 : 'update', 3 : 'cancel', 4 : 'trade',
          5 : 'reentry', 6 : 'newstop', 7 : 'reject',
          8 : 'removed', 9 : 'stopped', 11 : 'expire'}

sides = {'1' : 'buy', '2' : 'sell'}

states = {'0' : 'new', '1' : 'partial', '2' : 'executed',
          '4' : 'cancelled', '5' : 'modified', '8' : 'rejected',
          'C' : 'expired'}
