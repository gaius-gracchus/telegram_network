# -*- coding: UTF-8 -*-

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

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

API_ID = int( os.getenv('TELEGRAM_API_ID') )
API_HASH = os.getenv('TELEGRAM_API_HASH')
USERNAME = os.getenv('TELEGRAM_USERNAME')
PHONE = os.getenv('TELEGRAM_PHONE')

SQLITE_DB = f'../data/{USERNAME}.session'

INPUT_FILE = '../data/round_1/round_1_seed.txt'

MSG_OUTPUT_DIR = '../data/round_1/messages'

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

BLACKLIST = set( [
  'ChannelCollection',
  'CheckRestrictionsBot',
  'ChristianLoungeBot',
  'acceleratethehate',
  'AlpinePost',
  'AltRightBrasil_HATE',
  'Byzantium_Insider',
  'DachChan',
  'FaithInRace',
  'Fuhrerious88',
  'IOTBW2019',
  'NSRINewsChannel',
  'OnlyWhitePeopleGoToHeaven',
  'SwedenNatNews',
  'afinils',
  'albertschannel',
  'alpinepost',
  'ay_tone',
  'allrounderhs',
  'altrightbrasil_hate',
  'beatwomen',
  'blackonwhitecrime',
  'boogalooprep',
  'brotherhoodoc',
  'brothermillennius',
  'channelcollection',
  'checkrestrictionsbot',
  'christianloungebot',
  'christtelegram',
  'cringewaffenofficial',
  'doomermeetups',
  'fashymemeslut',
  'hans_pythonbot',
  'hansrwbs',
  'hreinhart',
  'intelligencereports',
  'ishouldtrytomakemusic',
  'lterror_88',
  'memesnshiet',
  'moncarnetdebord',
  'morememes',
  'national_justice',
  'nehalxo',
  'newsbeastbot',
  'newworldorderexposed',
  'nordicfrontierchat',
  'noticingthings',
  'now70s',
  'nwo_exposure',
  'patrickcaseymoney',
  'purehate88',
  'pwmd1',
  'rapekriegdivision',
  'rapetibbygram',
  'reich1488',
  'romaniaimpotrivaprogresistilor',
  'santashops',
  'slovaksiegeshack',
  'smokd',
  'swamptownboogaloo',
  'talmudicidentity',
  'theethnostate',
  'tolkientalk',
  'trspodcasts',
  'vinnishrebel',
  'vivelemort',
  'wargus_christi',
  'whitejesusganggang',
  'whitetruth',
  'wignatclique',
  'wignatrecords',
  'zyklon1488',
  'zyklon_1488'] )

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

async def main( ):

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

  id_dict = dict( zip( df[ 'username' ] , df[ 'id' ] ) )

  #---------------------------------------------------------------------------#

  os.makedirs( MSG_OUTPUT_DIR, exist_ok = True )

  already_extracted = (
    set( os.listdir( MSG_OUTPUT_DIR ) ) | BLACKLIST )

  with open( INPUT_FILE, 'r' ) as f:
    username_list = f.read( ).splitlines()

  username_list = [ username for username in username_list if username not in already_extracted ]

  for username in username_list:

    print( username )

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

        print( username, msg_count )

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

asyncio.run( main( ) )

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
