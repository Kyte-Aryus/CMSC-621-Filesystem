# Imports.
import os
import json
import time
import argparse
import signal
import random


# For handling interruptions.
def signal_handler(signal, frame):
    global interrupted
    interrupted = True
signal.signal(signal.SIGINT, signal_handler)
interrupted = False


# Directory where operations are executed.
datadir = './tmp/'  # TODO: change to `/mountpoint/` when ready.


# Parse arguments.
parser = argparse.ArgumentParser(description='Simulate client interactions.')
parser.add_argument('action', type=str, help='simulate this action')
parser.add_argument('sleep', type=float, help='sleep time (seconds) between actions')
parser.add_argument('output', type=str, help='output file to write to')
parser.add_argument('--fs', dest='fs', type=int, default=1, help='file size (MB) (default: 1)')
args = parser.parse_args()


# Store metrics.
results = {
    'timestamp': [],  
    'complete_time': [],
    'complete_success': []
}


# Action definitions.
def toy():
    if random.random() < 0.1:
        return 1
    else:
        return 0


# Make sure we use a valid action.
assert args.action in ['toy'], 'invalid action'

# Run loop.
while True:

    # Run action.
    start_time = time.time()
    if args.action == 'toy':
        complete_success = toy()
    complete_time = time.time()-start_time

    # Log results.
    results['timestamp'].append(start_time)
    results['complete_time'].append(complete_time)
    results['complete_success'].append(complete_success)

    # Sleep.
    time.sleep(args.sleep)

    # Check if interrupted.
    if interrupted:
        break



# Create a results directory if doesn't exist.
if not os.path.exists('./results/'):
    os.makedirs('./results/')

# Save results.
with open(os.path.join('./results/', args.output), 'w') as outfile:  
    json.dump(results, outfile) 
