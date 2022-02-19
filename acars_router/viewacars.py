#!/usr/bin/env python3

import os
import sys
import json
import argparse
import socket
import time
import curses

def main(stdscr):
    # Clear screen
    stdscr.clear()

    # This raises ZeroDivisionError when i == 10.
    for i in range(0, 11):
        v = i-10
        stdscr.addstr(i, 0, '10 divided by {} is {}'.format(v, 10/v))

    stdscr.refresh()
    stdscr.getkey()

if __name__ == "__main__":
    curses.wrapper(main)
