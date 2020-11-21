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

parser = argparse.ArgumentParser(
  description = 'Download Telegram messages for a specified channel' )

parser.add_argument(
  '--username',
  type = str,
  help = 'Channel username to download messages of' )

args = parser.parse_args( )

username = args.username

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

API_ID = int( os.getenv('TELEGRAM_API_ID') )
API_HASH = os.getenv('TELEGRAM_API_HASH')
USERNAME = os.getenv('TELEGRAM_USERNAME')
PHONE = os.getenv('TELEGRAM_PHONE')

SQLITE_DB = f'../data/{USERNAME}.session'

MSG_OUTPUT_DIR = '../data/round_1/messages'

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

  cnx = sqlite3.connect( SQLITE_DB )

  df = pd.read_sql_query( "SELECT * FROM entities", cnx )
  df = df[ [ 'id', 'username', 'name' ] ]
  df = df.dropna( )

  cnx.close( )

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

      with open( os.path.join( MSG_OUTPUT_DIR, username), 'wb' ) as f:
        pickle.dump( messages, f )

      channel_id = None

  except errors.FloodWaitError as e:
    print( '**********Have to sleep', e.seconds, 'seconds**********' )
    time.sleep(e.seconds)

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

asyncio.run( main( username ) )

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
