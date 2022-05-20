#! usr/bin/env python3

"""
assignment1.py
First assignment of Big Data Computing
Calculates the average PHRED score per base position for all reads within a fastq file.
"""

_author_ = "IJsbrand Pool"
_version_ = 2.0

import argparse as ap
import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
import csv
import sys
import time
import queue


POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
IP = ''
PORTNUM = 5381
AUTHKEY = b'wortelwachtwoord'


def make_server_manager(port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=('127.0.0.1', port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager


def chunks(number, mysize):
    """ Returns the chunks. """
    mychunks = []
    for i in range(mysize):
        start = int(i * len(number) / mysize)
        end = int((i + 1) * len(number) / mysize)
        mychunks.append(number[start:end])

    return mychunks


def read_fastq(fastq_file):
    """ Reads the files"""
    quality = []
    qual = True

    with open(fastq_file, encoding='UTF-8') as fastq:
        while qual:
            header = fastq.readline()
            nucleotides = fastq.readline()
            strand = fastq.readline()
            qual = fastq.readline().rstrip()
            if qual:
                quality.append(qual)
            if header or nucleotides or strand:
                pass

    return quality


def calculate_quals(quality):
    """ Calculates quality scores """
    results = []
    for qual in quality:
        for item, checker in enumerate(qual):
            try:
                results[item] += ord(checker) - 33
            except IndexError:
                results.append(ord(checker) - 33)
    return results


def generate_output(average_phredscores, csvfile):
    """ Generates the output for the file('s) """
    if csvfile is None:
        csv_writer = csv.writer(sys.stdout, delimiter=',')
        for i, score in enumerate(average_phredscores):
            csv_writer.writerow([i, score])

    else:
        with open(csvfile, 'w', encoding='UTF-8', newline='') as myfastq:
            csv_writer = csv.writer(myfastq, delimiter=',')
            for i, score in enumerate(average_phredscores):
                csv_writer.writerow([i, score])


def runserver(fn, data):
    # Start a shared manager server and access its queues
    manager = make_server_manager(PORTNUM, b'wortelwachtwoord')
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not data:
        print("Gimme something to do here!")
        return

    print("Sending data!")
    for d in data:
        shared_job_q.put({'fn': fn, 'arg': d})

    time.sleep(2)

    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            print("Got result!", result)
            if len(results) == len(data):
                print("Got all results!")
                break
        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaand we're done for the server!")
    manager.shutdown()
    print(results)


def make_client_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (ip, port))
    return manager


def runclient(num_processes):
    manager = make_client_manager(IP, PORTNUM, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)


def run_workers(job_q, result_q, num_processes):
    processes = []
    for p in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temP)
        temP.start()
    print("Started %s workers!" % len(processes))
    for temP in processes:
        temP.join()


def peon(job_q, result_q):
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print("Aaaaaaargh", my_name)
                return
            else:
                try:
                    result = job['fn'](job['arg'])
                    print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                    result_q.put({'job': job, 'result': result})
                except NameError:
                    print("Can't find yer fun Bob!")
                    result_q.put({'job': job, 'result': ERROR})

        except queue.Empty:
            print("sleepytime for", my_name)
            time.sleep(1)


if __name__ == '__main__':
    # argparse
    argparser = ap.ArgumentParser(description="Script for assignment 1 of Big Data Computing")
    # Mode arguments
    mode = argparser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-s", action="store_true", help="Run the program in Server mode; see extra options needed below")
    mode.add_argument("-c", action="store_true", help="Run the program in Client mode; see extra options needed below")
    # Server arguments
    server_args = argparser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument("-o", action="store", dest="CSVfile",
                           required=False, help="CSV file to save the output.")
    server_args.add_argument("fastq_files", action="store",
                           nargs='+', help="At least 1 ILLUMINA fastq file to process")
    # Client arguments
    client_args = argparser.add_argument_group(title="Arguments when run in client mode")
    argparser.add_argument("-n", action="store",
                           dest="n", required=False, type=int,
                           help="Amount of cores to be used")
    client_args.add_argument("--host", action="store", type=str, help="The hostname where the Server is listening")
    client_args.add_argument("--port", action="store", type=int, help="The port on which the Server is listening")

    args = argparser.parse_args()

    qualities = read_fastq(args.fastq_files[0])
    qual_chunked = chunks(qualities, 4)
    server = mp.Process(target=runserver, args=(calculate_quals, qual_chunked))
    time.sleep(1)
    server.start()


    #for file in args.fastq_files:
    #    qualities = read_fastq(file)
    #    qual_chunked = chunks(qualities, 4)
#
    #    with mp.Pool(args.n) as pool:
    #        phredscores = pool.map(calculate_quals, qual_chunked)
#
    #    phredscores_avg = [sum(i) / len(qualities) for i in zip(*phredscores)]
#
    #    if len(args.fastq_files) > 1:
    #        if args.CSVfile is None:
    #            sys.stdout.write(file + "\n")
    #            CSV = None
    #        else:
    #            CSV = f'{file}.{args.CSVfile}'
    #    else:
    #        CSV = args.CSVfile
#
    #    generate_output(phredscores_avg, CSV)
