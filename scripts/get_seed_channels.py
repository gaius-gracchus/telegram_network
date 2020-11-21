# -*- coding: UTF-8 -*-

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

import re
import os

from bs4 import BeautifulSoup

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

ROUND_0_DIR = '../data/round_0/'

PATTERN = r'https://t.me/(.*)?'

ROUND_1_DIR = '../data/round_1/'

ROUND_1_LIST = os.path.join( ROUND_1_DIR, 'round_1_seed.txt' )

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

all_channels = list( )

seed_files = sorted( os.listdir( ROUND_0_DIR ) )
seed_files = [ os.path.join( ROUND_0_DIR, file ) for file in seed_files if file.endswith( '.html' ) ]

for file in seed_files:

  with open( file, 'r' ) as f:
    soup = BeautifulSoup( f.read( ), features = 'lxml' )

  hrefs = soup.find_all( 'a', href = True )
  hrefs = [ h[ 'href' ] for h in hrefs ]

  channels = [ ]
  for href in hrefs:
    s = re.search( PATTERN, str( href ) )
    try:
      channel = s.groups( )[ 0 ]
      if not channel.startswith( 'PublicLeaderBoard/' ):
        channels.append( channel )
    except AttributeError:
      pass

  all_channels.extend( channels )

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

os.makedirs( ROUND_1_DIR, exist_ok = True )

all_channels = sorted( list( set( all_channels ) ) )

with open( ROUND_1_LIST, 'w' ) as f:
  for channel in all_channels:
    f.write( channel + '\n' )

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
