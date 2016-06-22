## Testing ##
--------------------------------
Use bin/test to test.
Please reserve port 9999 for paxos library.

## Paxos Library Testing ##
--------------------------------
The codes for testing paxos library is contained in paxos.py. 
To test the paxos library, run "python3 paxos\_peer.py".


## Testing Note ##
--------------------------------
If local requests are sent concurrently even if with relative big delay, the order may still be changed. Some test based on timing may fail because of this.
