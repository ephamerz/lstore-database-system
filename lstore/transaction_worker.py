from lstore.table import Table, Record
from lstore.index import Index
import threading
import time, random


class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = list(transactions)
        self.result = 0
        pass

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        # here you need to create a thread and call __run
        self.thread = threading.Thread(target = self.__run)
        self.thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        #when thread has been started, wait before continuing
        if self.thread != None:
            self.thread.join()


    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            
            #For an aborted transaction, the thread should keep trying to execute it until it gets committed.
            committed = transaction.run()           
            while committed == False and getattr(transaction, "last_abort_retryable", True): # check if the transaction was aborted due to a logical error that should not be retried
                # print(f"Transaction {transaction.transaction_id} aborted. Retrying...")
                time.sleep(0.001 + random.random() * 0.005)  # tiny random delay
                committed = transaction.run()

            self.stats.append(committed is True)
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
