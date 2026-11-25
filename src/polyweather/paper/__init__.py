"""Paper trading: simulated fills against the recorded order books, and the ledgers.

Nothing here can place an order. Every decision is a function of what the recorder
and the archives wrote to disk, so a ledger can be recomputed from scratch for any
recorded day and must come out the same.
"""
