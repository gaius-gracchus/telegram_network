# -*- coding: UTF-8 -*-

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

import pickle
import os
import asyncio
from collections import Counter

from telethon import TelegramClient, errors
from telethon.tl.types import PeerChannel
from telethon.tl.functions.channels import GetParticipantsRequest, GetFullChannelRequest
from telethon.tl.functions.messages import GetHistoryRequest

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

API_ID = int( os.getenv( 'TELEGRAM_API_ID' ) )
API_HASH = os.getenv( 'TELEGRAM_API_HASH' )
USERNAME = os.getenv( 'TELEGRAM_USERNAME' )
PHONE = os.getenv( 'TELEGRAM_PHONE' )

MSG_INPUT_DIR = '../data/round_1/messages'

OUTPUT_CONNECTION_DICT = '../data/round_1/connections.pkl'

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

async def main( ):

  # Create the client and connect
  client = TelegramClient( USERNAME, API_ID, API_HASH )
  await client.start( )
  print("Client Created")

  # Ensure you're authorized
  if not await client.is_user_authorized( ):
    await client.send_code_request( PHONE )
    try:
      await client.sign_in( PHONE, input( 'Enter the code: ' ) )
    except errors.SessionPasswordNeededError:
      await client.sign_in( password = input( 'Password: ' ) )

  #---------------------------------------------------------------------------#

  username_list = sorted( os.listdir( MSG_INPUT_DIR ) )

  connection_dict = dict( )

  for username in username_list:

    print( f'getting forwards for channel {username}\n' )

    with open( os.path.join( MSG_INPUT_DIR, username), 'rb' ) as f:
      messages = pickle.load( f )

    forward_list = list( )

    for message in messages:

      try:

        channel_id = message[ 'fwd_from' ][ 'from_id' ][ 'channel_id' ]
        forward_channel = await client.get_entity( PeerChannel( channel_id ) )
        forward_username = forward_channel.username

        print( username, forward_username )

        if forward_channel is not None:

          forward_list.append( forward_username )

      except:

        pass

    print( f'forwards from {username}: {dict( Counter( forward_list ))}' )

    connection_dict[ username ] = dict( Counter( forward_list ) )

  with open( OUTPUT_CONNECTION_DICT, 'wb' ) as f:
    f.write( connection_dict )

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

asyncio.run( main( ) )

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
