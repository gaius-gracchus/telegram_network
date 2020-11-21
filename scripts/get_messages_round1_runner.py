# -*- coding: UTF-8 -*-

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

import argparse
import os
import subprocess

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

SCRIPT = 'get_messages_round1_single.py'

INPUT_FILE = '../data/round_1/round_1_seed.txt'

MSG_OUTPUT_DIR = '../data/round_1/messages'

ERROR = 'telethon.errors.rpcerrorlist.UsernameNotOccupiedError:'

BLACKLIST_FILE = '../data/blacklist.txt'

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

os.makedirs( MSG_OUTPUT_DIR, exist_ok = True )

with open( INPUT_FILE, 'r' ) as f:
  username_list = f.read( ).splitlines( )

with open( BLACKLIST_FILE, 'r' ) as f:
  blacklist = f.read( ).splitlines( )

already_extracted = (
  set( os.listdir( MSG_OUTPUT_DIR ) ) | set( blacklist ) )
already_extracted = { username.lower( ) for username in already_extracted }

username_list = [ username for username in username_list if username.lower( ) not in already_extracted ]

for username in username_list:

  process = subprocess.run(
    args = [ 'python', SCRIPT, '--username', username ],
    stdout = subprocess.PIPE,
    stderr = subprocess.PIPE )

  stderr = process.stderr.decode( 'utf-8' )
  stdout = process.stdout.decode( 'utf-8' )

  if len( stderr ) > 0:
    print( f'FAILED    {username}')
    with open( BLACKLIST_FILE, 'a' ) as f:
      f.write( username + '\n' )
  else:
    print( f'{username}' )

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#