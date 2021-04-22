# -*- coding: utf-8 -*-
# Copyright (C) 2015, 2016, 2017 NewStore, Inc. All rights reserved.
import os
import time
import json
import logging
import asyncio

from shopify_inventory_update.handlers.shopify_handler import ShopifyConnector
from shopify_inventory_update.handlers.sqs_handler import SqsHandler
from shopify_inventory_update.handlers.s3_handler import S3Handler

from param_store.client import ParamStore

LOGGER = logging.getLogger(__name__)
LOG_LEVEL_SET = os.environ.get('LOG_LEVEL', 'INFO') or 'INFO'
LOG_LEVEL = logging.DEBUG if LOG_LEVEL_SET.lower() in ['debug'] else logging.INFO
LOGGER.setLevel(LOG_LEVEL)
SQS_HANDLER = None
WORKER_TRIGGER_DEFAULT_NAME = 'shopify_availability_export_worker'
TENANT = os.environ['TENANT'] or 'frankandoak'
STAGE = os.environ['STAGE'] or 'x'


def handler(event, context):
    """
    Arguments:
        event {dic} -- Lambda event
        context {object} -- Lambda object

    Returns:
        string -- Result string of process
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(_sync_inventory(loop, context))
    loop.run_until_complete(future)
    loop.stop()
    loop.close()
    return 'Finished'


async def _sync_inventory(loop, context):
    """Syncs the invetory with shopify by pulling messages from a queue and
        updating the quantity of the variants in shopify.
        This handler can have more than one variant in each queue message but
        will always process on-by-one to shopify
    Arguments:
        loop {EventLoop} -- Async event loop
    """
    # Handler for AWS SQS
    param_store = ParamStore(TENANT, STAGE)
    shopify_config = json.loads(param_store.get_param('shopify'))

    global shopify_conector
    shopify_conector = ShopifyConnector(
        shopify_config['username'],
        shopify_config['password'],
        shopify_config['shop']
    )
    global sqs_handler
    sqs_handler = SqsHandler(
        os.environ.get("SQS_NAME")
    )

    message_count = await sqs_handler.get_messages_count()
    if message_count <= 0:
        LOGGER.info('No more messages to process... exiting->()')
        return
    LOGGER.info('%s available in the queue...', str(message_count))

    tasks = []
    while message_count > 0:
        messages = await sqs_handler.get_message()
        if messages.get('Messages'):
            failure_list = []

            for message in messages['Messages']:
                receipt_handle = message['ReceiptHandle']
                products = json.loads(message['Body'])
                '''
                {'34142712266787': {
                    'atp': 19,
                    'location_id': 61470441628
                }}
                '''
                updated_products = defining_product(products)
                # create list of task to process for each variant
                tasks += [asyncio.ensure_future(_update_variant_at_shopify(
                    updated_products, shopify_conector, receipt_handle))]

        message_count = await sqs_handler.get_messages_count()
        LOGGER.info('%s available in the queue...',
                    str(message_count))

    if len(tasks) > 0:
        await asyncio.wait(tasks)

    return 'Sync of inventory with shopify complete'


def defining_product(products):
    return [{
        'inventory_item_id': str(key),
        'atp': value['atp'],
        'location_id': value['location_id']
    } for key, value in products.items()]


async def _update_variant_at_shopify(products, shopify_conector, receipt_handle):
    global sqs_handler
    try:
        products = await _create_deltas_for_inventory(products)
        response = await shopify_conector.update_inventory_quantity_graphql(products, products[0]['location_id'])
        await sqs_handler.delete_message(receipt_handle)
        return response
    except Exception:
        LOGGER.exception('Failed to process bulk variant update')
        raise


async def _create_deltas_for_inventory(products):
    global shopify_conector
    LOGGER.info('Fetching inventory levels from Shopify')
    inventory_shopify = _transform_to_dictionary_inventory_map(await shopify_conector.get_inventory_quantity(products, products[0]['location_id']))
    LOGGER.info('Response from transform ')
    LOGGER.info(inventory_shopify)
    for product in products:
        formated_variant = f'gid://shopify/InventoryItem/{product["inventory_item_id"]}'
        if formated_variant in inventory_shopify:
            product['delta'] = product['atp'] - inventory_shopify[formated_variant]
    return products


def _transform_to_dictionary_inventory_map(inventory_shopify_response):
    result = {}
    for inventory in inventory_shopify_response:
        id_inventory = inventory.get('item', {}).get('id')
        if id_inventory:
            result[id_inventory] = inventory['available']
    return result

