# -*- coding: UTF-8 -*-

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

from collections import Counter
import os
import pickle

from bs4 import BeautifulSoup

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

INPUT_CONNECTION_DIR = '../data/round_1/connections'

ROUND_1_LIST = '../data/round_1/round_1_seed.txt'
BLACKLIST = '../data/blacklist.txt'

ROUND_2_LIST = '../data/round_2/round_2_channels_list.txt'

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

c = Counter( )

files = sorted( os.listdir( INPUT_CONNECTION_DIR ) )

for file in files:

  with open( os.path.join( INPUT_CONNECTION_DIR, file ), 'rb' ) as f:
    _c = pickle.load( f )

  # print( c )
  c += _c

channel_list = list( c.keys( ) )

channel_set = set( channel_list )

with open( BLACKLIST, 'r' ) as f:
  blacklist = set( f.read( ).splitlines( ) )

with open( ROUND_1_LIST, 'r' ) as f:
  round_1_set = set( f.read( ).splitlines( ) )

print( len( channel_list ) )

channel_set = channel_set - blacklist
channel_set = channel_set - round_1_set

channel_list = list( channel_set )
channel_list = sorted( list( filter( None, channel_list ) ) )

# #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

os.makedirs( os.path.dirname( ROUND_2_LIST ), exist_ok = True )

with open( ROUND_2_LIST, 'w' ) as f:
  for channel in channel_list:
    f.write( channel + '\n' )

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
