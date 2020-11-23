# -*- coding: UTF-8 -*-

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

import argparse
import pickle
import os
import asyncio
from collections import Counter
import time
import sqlite3

import numpy as np
import pandas as pd

from telethon import TelegramClient, errors
from telethon.tl.types import ChannelParticipantsSearch, PeerChannel
from telethon.tl.functions.channels import GetParticipantsRequest, GetFullChannelRequest
from telethon.tl.functions.messages import GetHistoryRequest

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

print( 'parsing args' )

parser = argparse.ArgumentParser(
  description = 'Download Telegram messages for a specified channel' )

parser.add_argument(
  '--username',
  type = str,
  help = 'Channel username to download messages of' )

parser.add_argument(
  '--output_dir',
  type = str,
  help = 'Directory to write message files to' )

parser.add_argument(
  '--credentials',
  type = int,
  default = 1,
  help = 'Set of API credentials to use' )

args = parser.parse_args( )

username = args.username
output_dir = args.output_dir
credentials = args.credentials

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

API_ID = int( os.getenv(f'TELEGRAM_API_ID_{credentials}') )
API_HASH = os.getenv(f'TELEGRAM_API_HASH_{credentials}')
USERNAME = os.getenv(f'TELEGRAM_USERNAME_{credentials}')
PHONE = os.getenv(f'TELEGRAM_PHONE_{credentials}')

SQLITE_DB = f'../data/{USERNAME}.session'

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

async def main( username ):

  # Create the client and connect
  client = TelegramClient( USERNAME, API_ID, API_HASH )
  await client.start( )
  print("Client Created")
  # Ensure you're authorized
  if not await client.is_user_authorized():
    await client.send_code_request( PHONE )
    try:
      await client.sign_in( PHONE, input('Enter the code: '))
    except errors.SessionPasswordNeededError:
      await client.sign_in(password=input('Password: '))

  #---------------------------------------------------------------------------#

  df = pd.DataFrame( columns = [ 'id', 'username', 'name' ] )

  sqlite_db_list = [
    f'{os.getenv("TELEGRAM_USERNAME_1")}.session',
    f'{os.getenv("TELEGRAM_USERNAME_2")}.session',
    f'{os.getenv("TELEGRAM_USERNAME_3")}.session',
    f'../data/{os.getenv("TELEGRAM_USERNAME_1")}.session' ]

  dfl = list( )

  for sqlite_db in sqlite_db_list:

    try:

      cnx = sqlite3.connect( sqlite_db )

      _df = pd.read_sql_query( "SELECT * FROM entities", cnx )
      _df = _df[ [ 'id', 'username', 'name' ] ]

      cnx.close( )

      dfl.append( _df )

    except:
      pass

  df = pd.concat( dfl )
  df = df.dropna( )
  df = df.drop_duplicates( )

  id_dict = dict( zip( df[ 'username' ], df[ 'id' ] ) )

  #---------------------------------------------------------------------------#

  channel = None

  try:

    channel_id = id_dict.get( username )

    if channel_id is not None:

      try:
        channel = await client.get_entity( PeerChannel( int( str( channel_id )[ 4: ] ) ), )

      except errors.rpcerrorlist.UsernameNotOccupiedError:
        channel = None

    else:

      try:
        channel = await client.get_entity( f'https://t.me/{username}' )

      except errors.rpcerrorlist.UsernameNotOccupiedError:
        channel = None

    if channel is not None:

      posts = await client( GetHistoryRequest(
        peer = channel,
        limit = 100,
        offset_date = None,
        offset_id = 0,
        max_id = 0,
        min_id = 0,
        add_offset = 100,
        hash = 0 ) )

      msg_count = posts.count
      msg_iterations = msg_count // 100 + 1

      print( username, msg_count, '\n' )

      messages = [ ]

      for i in range( msg_iterations ):
        posts = await client( GetHistoryRequest(
          peer = channel,
          limit = 100,
          offset_date = None,
          offset_id = 0,
          max_id = 0,
          min_id = 0,
          add_offset = i * 100,
          hash = 0 ) )

        messages.extend( posts.messages )

      messages = [ message.to_dict( ) for message in messages ]

      with open( os.path.join( output_dir, username), 'wb' ) as f:
        pickle.dump( messages, f )

      channel_id = None

  except errors.FloodWaitError as e:
    print( '**********Have to sleep', e.seconds, 'seconds**********' )
    time.sleep(e.seconds)

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

asyncio.run( main( username ) )

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
