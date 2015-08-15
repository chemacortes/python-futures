# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 18:29:33 2015

@author: chema
"""

from __future__ import print_function

import os
import time
from os.path   import join, getsize
from itertools import islice
from random    import random

###
#
# NOTE:
# Python3's standart library includes the package "futures".
# With python2, you must install the backport with pip or conda
#

from concurrent.futures import (ThreadPoolExecutor,
                                ProcessPoolExecutor,
                                TimeoutError,
                                as_completed)
#
###



def proc_file(root, dirs, files):

    try:
        size = sum(getsize(join(root, name)) for name in files)
        num = len(files)
        res = "{:8} bytes in {:3} files from '{}'".format(size, num, root)

        # simulating timeouts
        if random() > 0.9:
            wait = 10*random()
            time.sleep(wait)
            res = "*"*int(wait) + res  #one '*' by second
    except:
        res = "ERROR"

    return res
    

def main(dir_input):
    
    pool = ThreadPoolExecutor(40)
#    pool = ProcessPoolExecutor()
    
    with pool as ex:

        futures = (ex.submit(proc_file, root, dirs, files)
                    for (root, dirs, files) in os.walk(dir_input))
                    
        while True:

            # In order to not collapse the process,
            # the iterator is consumed in chunks of 100 items
            print("\n\n----------- NEW CHUNK -------------\n")
            chunk = list(islice(futures, 100))
            if len(chunk)==0:
                break
            
            try:
                for future in as_completed(chunk, timeout=8):
                    print(future.result())
            except TimeoutError:
                print("\n### ERROR: timeout exception\n")
                # Some chunks has not been successfully completed (nothing will be printed)
                # Next try with another chunk
                #
                # All these incompleted processes will be eliminated within
                # the executor.shutdown, at the exit of the 'with' block.
                # In a production system, the "shutdown" will be called more frequently.
                # A good strategy will be a with-block by chunk in order to sanitize the memory.
 

if __name__ == '__main__':
    dir_input = "/home/chema/repos" # a lot of files inside
    main(dir_input)
