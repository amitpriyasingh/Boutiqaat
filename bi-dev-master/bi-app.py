# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import sys


import warnings
from flask.exthook import ExtDeprecationWarning
warnings.simplefilter('ignore', ExtDeprecationWarning)

#To make the superset folder callable - nihal
cwd = os.getcwd()
sys.path.append(cwd)

from superset.cli import manager

if __name__ == "__main__":
    manager.run()


