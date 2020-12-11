# Code for creating a two Phase locking scheduler for history of transactions 
# by Project Group-1 Computer Science 5570 0001 Architecture Of Database Management Systems
import re
import sys
import pandas as pd


#Class used to define the Transaction
class Transaction():
    is_growing = True

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return 'Transaction - %s' % (self.value)
# This class defines the various operations performed by the transaction
class Operations():
    def __init__(self, transaction, action, resource):
        self.transaction = transaction
        self.action = action
        self.resource = resource

    def __str__(self):
        return 'Operation - %s - %s - %s' % (self.transaction,
            self.action, self.resource)

    #various methods for returning the actions are defined here(Read/write/commit)
    def is_writeOp(self):
        return self.action == 'w'

    def is_readOp(self):
        return self.action == 'r'

    def is_commitOp(self):
        return self.action == 'c'
    def formattohistory(self, delayed=False):
        delayment_str = 'Delayed' if delayed else ''
        if not self.resource:
            return '%s%s' % (self.action, self.transaction)
        return '%s%s%s[%s]' % (delayment_str, self.action,self.transaction, self.resource)

# Class used for the implementation of Locking
class Locking():
  # Method to initialize the variables
    def __init__(self, transaction, exclusive, resource, released=False):
        self.transaction = transaction
        self.exclusive = exclusive
        self.resource = resource
        self.released = released
        print(resource)
        print('Transaction ' + (transaction))
   # Formatting the history of transactions to 
    def formattohistory(self):
        operation = 'l' if not self.released else 'u'
        lock_type = 'x' if self.exclusive else 's'
        return '%s%s%s[%s]' % (operation, lock_type, self.transaction,self.resource)

    def __str__(self):
        return 'Lock - %s - %s - %s' % (self.transaction,
            'exclusive' if self.exclusive else 'shared', self.resource)

# Two-Phase locking Scheduler class for executing the list of transactions
class twoPLScheduler():
   # Method defined to execute the transactions
    def executeHistory(self, history):
        self.locks = []
        self.execution_list = []
        self.final_history = []
        self.operations = []
        self.delayed_operations = []
        self.transactions = {}
        self.counter = 0
        self.parse_history(history)
        print(' Original history is: %s' % (history))
        self.run_operations()
        print('*************************************************************')
  #This method is used to parse the input string using regex string to compare the ideal input(for ex: r1[x], w2[z], c2 etc.)
    def parse_history(self, history):
        substrings = history.split(u' ')
        for substring in substrings:
            str_match = None
            if re.match('r(\d+)\[([a-z]+)\]', substring):
                str_match = re.match('r(\d+)\[([a-z]+)\]', substring)
                self.operations.append(Operations(str_match.group(1), 'r', str_match.group(2)))
            elif re.match('w(\d+)\[([a-z]+)\]', substring):
                str_match = re.match('w(\d+)\[([a-z]+)\]', substring)
                self.operations.append(Operations(str_match.group(1), 'w', str_match.group(2)))
            elif re.match('c(\d+)', substring):
                str_match = re.match('c(\d+)', substring)
                self.operations.append(Operations(str_match.group(1), 'c', None))
            else:
                raise Exception('Invalid input provided')
            if str_match.group(1) not in self.transactions.keys():
                self.transactions[str(str_match.group(1))] = Transaction(str_match.group(1))

    #method to check before adding lock to the operation
    def haslock(self, operation):
        for lock in self.locks:
            if lock.resource == operation.resource and lock.transaction == operation.transaction and ((lock.exclusive and operation.action == 'w') or (not lock.exclusive and operation.action == 'r')):
                return True
        return False
   #this method defines which lock to be applied based on the type of operation i.e x for Write and s for Read
    def canlock(self, operation):
      
        relevant_locks = [lock for lock in self.locks if lock.resource == operation.resource]
        for lock in relevant_locks:
            if not lock.exclusive:
                if lock.transaction == operation.transaction and len(relevant_locks) == 1 and operation.action == 'w':
                    return True
                elif lock.transaction != operation.transaction and operation.action == 'r':
                    return True
            return False
        return True
     #this method checks for releasing  which locks(x or s)
    def release_locks(self, transaction):
        original_locks = list(self.locks)
        self.locks[:] = [lock for lock in self.locks\
            if not lock.transaction == transaction]
        for released_lock in set(original_locks).difference(set(self.locks)):
            lock = Locking(released_lock.transaction, released_lock.exclusive, released_lock.resource, True)
            self.final_history.append(lock)
   #method defined to add locks to the operations in transactions         
    def addinglock(self, operation):
        exclusive = True if operation.action == 'w' else False
        lock = Locking(operation.transaction, exclusive, operation.resource)
        self.locks.append(lock)
        self.final_history.append(lock)
        transaction = self.transactions[operation.transaction]

   
 # method defined to check for deadlock
    def has_deadlock(self):
        conflicts = []
        for delayed_operation in self.delayed_operations:
            for lock in self.locks:
                if delayed_operation.transaction != lock.transaction and delayed_operation.resource == lock.resource:
                        conflicts.append((delayed_operation.transaction, lock.transaction))
        conflicts_copy = list(conflicts)
        for conflict in conflicts:
            for conflict_copy in conflicts_copy:
                if conflict[0] == conflict_copy[1] and conflict[1] == conflict_copy[0]:
                    return conflict
        return False
 #checks if the transaction is growing or not
    def can_growtransaction(self, transaction_value):
        return self.transactions[str(transaction_value)].is_growing
#if a deadlock is found, this method delays the transaction responsible
    def delay_transaction(self, transaction):
        self.delayed_operations[:] = [delayed_operation for delayed_operation in self.delayed_operations if not delayed_operation.transaction == transaction]
        self.final_history[:] = [item for item in self.final_history if not item.transaction == transaction]
        self.locks[:] = [lock for lock in self.locks if not lock.transaction == transaction]
        decrementercntr = 0
        for index, operation in enumerate(self.execution_list):
            if operation.transaction == transaction and index <= self.counter:
                decrementercntr += 1
        self.execution_list[:] = [operation for operation in self.execution_list if not operation.transaction == transaction]
        self.counter -= decrementercntr
        for operation in self.operations:
            if operation.transaction == transaction:
                self.execution_list.append(operation)
        print('A deadlock was found. The transaction %s was delayed to resolve it.' %(transaction))
   #method defined to check if transaction can commit
    def cancommitTrans(self, transaction):
        pending_operations = []
         
        pending_operations[:] = [operation for operation in self.delayed_operations if operation.transaction == transaction]
        return len(pending_operations) == 0
 #method checks if there are any delayed operations in the history
    def has_delayed_operation(self, transaction):
        
        return True if [operation for operation in self.delayed_operations if operation.transaction == transaction] else False
  #run the delayed operation if any
    def run_delayedoperations(self):
        if self.delayed_operations:
            redelayed_operations = []
            for delayed_operation in self.delayed_operations:
                redelayed_operation = self.run_operation(delayed_operation)
                if redelayed_operation:
                    redelayed_operations.append(redelayed_operation)
            self.delayed_operations = redelayed_operations
  #method checks if any operation would be executed or not based on which locking phase its transaction is in
  #also checks if commit can be executed after all operations are completed
    def run_operation(self, operation):
        if operation.is_writeOp() or operation.is_readOp():
            if self.can_growtransaction(operation.transaction):
                if self.haslock(operation):
                    self.final_history.append(operation)
                elif self.canlock(operation):
                    self.addinglock(operation)
                    self.final_history.append(operation)
                else:
                    return operation
            else:
                print('The operation %s will be ignored as its transaction is in shrinking phase.' %(operation.formattohistory()))
        elif operation.is_commitOp():
            if self.cancommitTrans(operation.transaction):
                self.final_history.append(operation)
                self.release_locks(operation.transaction)
                self.transactions[operation.transaction].is_growing = False
            else:
                print('Not possible to commit the transaction %s '\
                    'as there are pending operations.' %\
                    (operation.transaction))
 #method defined to run all the operations (check for delay, check for deadlock)  
    def run_operations(self):
        self.execution_list = list(self.operations)
        while self.counter < len(self.execution_list):
            if self.has_delayed_operation(self.execution_list[self.counter].transaction):
                self.delayed_operations.append(\
                    self.execution_list[self.counter])
                print('The operation %s was delayed.' %\
                    (self.execution_list[self.counter].formattohistory()))
            else:
                operation = self.run_operation(self.execution_list[self.counter])
                if operation:
                    self.delayed_operations.append(operation)
                    print('Delayed the operation %s.' %(operation.formattohistory()))
                    deadlock = self.has_deadlock()
                    if deadlock:
                        self.delay_transaction(operation.transaction)
                self.run_delayedoperations()
            self.counter += 1
            if self.counter == len(self.execution_list):
                for delayed_operation in self.delayed_operations:
                    self.execution_list.append(delayed_operation)
                self.delayed_operations = []
        self.print_final_history()

    # method to print the final history
    def print_final_history(self):
        operations_text = ''
        for item in self.final_history:
            if isinstance(item, Operations) or isinstance(item, Locking):
                operations_text += '%s, ' % (item.formattohistory())
        if operations_text:
            print('The final history: %s' % (operations_text.strip(', ')))

#Executing the Scheduler class
if __name__ == '__main__':
    scheduler = twoPLScheduler()
    #print('A history with no conflicts')
    #scheduler.executeHistory('r1[z] r2[y] r1[y] w2[x] r3[z] c1 c2 c3')

    #print(' A history with conflicts')
    #scheduler.executeHistory('r1[z] r1[x] w3[x] r2[y] w2[x] c1 c3 c2')

    #print(' A history with conflicts')
    #scheduler.executeHistory('r1[z] r1[x] w3[x] r2[y] w2[x] w4[z] c1 c3 c4 c2')
    
    #print('History with an operation that needs to be delayed')
    #scheduler.executeHistory('r1[x] w1[x] w2[x] c1 c2')
    
    #print('History with a deadlock')
   # scheduler.executeHistory('r1[x] w2[y] r1[y] w2[x] c1 c2')
   
    #print('History with more than one operation that has to be delayed')
    #scheduler.executeHistory('r1[y] w1[x] w2[x] r2[y] w2[y] c1 c2')

    print('A history with an operation which can\'t be executed')
    scheduler.executeHistory('r1[x] r2[y] r1[y] c1 r1[x] w2[x] c2')
    
