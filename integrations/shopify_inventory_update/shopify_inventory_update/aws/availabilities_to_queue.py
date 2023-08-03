# -*- coding: utf-8 -*-
# Copyright (C) 2020-21 NewStore, Inc. All rights reserved.

import asyncio
import io
import json
import logging
import os
import zipfile
import aiohttp

from param_store.client import ParamStore
from newstore_adapter.connector import NewStoreConnector
from shopify_inventory_update.handlers.sqs_handler import SqsHandler
from shopify_inventory_update.handlers.events_handler import EventsHandler
from shopify_inventory_update.handlers.s3_handler import S3Handler
from shopify_inventory_update.handlers.lambda_handler import stop_before_timeout
from shopify_inventory_update.handlers.dynamodb_handler import (
    update_item,
    get_item,
    get_all
)

LOG_LEVEL_SET = os.environ.get('LOG_LEVEL', 'INFO') or 'INFO'
LOG_LEVEL = logging.DEBUG if LOG_LEVEL_SET.lower() in ['debug'] else logging.INFO
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(LOG_LEVEL)

WORKER_TRIGGER_DEFAULT_NAME = 'availabilities_to_queue_worker'
MAP_FILE_VARIANTS_SKU = 'sku_to_variant_id_map_dict.json'
DYNAMODB_TABLE_NAME = 'frankandoaknew-availability-job-save-state'
DYNAMODB_MAPPING_TABLE_NAME = 'frankandoak-product-inventory-mapping'
JOB_STATE_KEY = 'current_job_state'
LAST_VARIANT_KEY = 'last_variant'
LAST_UPDATED_KEY = 'last_updated_at'

BLOCK_CONCURRENT_EXECUTION = bool(os.environ.get('BLOCK_CONCURRENT_EXECUTION', 'False'))
CONCURRENT_EXECUTION_BLOCKED_KEY = 'push_to_queue_concurrent_execution_blocked'

PARAM_STORE = None
MAPPING_ENTRIES = None
STOP_BEFORE_TIMEOUT = 60000

TENANT = os.environ['TENANT'] or 'frankandoak'
STAGE = os.environ['STAGE'] or 'x'
REGION = os.environ['REGION'] or 'us-east-1'

def handler(event, context):
    """
    Arguments:
        event {dic} -- Lambda event
        context {object} -- Lambda object

    Returns:
        string -- Result string of process
    """

    LOGGER.info('Start of availablities_to_queue')
    LOGGER.info('Event: %s', event)
    LOGGER.info('Context: %s', context)

    if BLOCK_CONCURRENT_EXECUTION:
        if _is_blocked():
            return 'Execution blocked'
        else:
            _set_blocked_state(True)

    global PARAM_STORE
    if not PARAM_STORE:
        PARAM_STORE = ParamStore(TENANT, STAGE)

    result = None

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(_save_to_queue_availabilities(context, event))
        loop.stop()
        loop.close()
    finally:
        LOGGER.info('All done. Release the lock.')
        _set_blocked_state(False)

    LOGGER.info('End of availablities_to_queue')
    return result


def get_availability_job_id():
    key = {
        'identifier': str(JOB_STATE_KEY)
    }
    response = get_item(table_name=DYNAMODB_TABLE_NAME, item=key)
    return response


def get_last_variant():
    key = {
        'identifier': str(LAST_VARIANT_KEY)
    }

    response = get_item(table_name=DYNAMODB_TABLE_NAME, item=key)

    if response and 'last_variant' in response:
        return response['last_variant']

    return ''


def save_last_variant(product_sku):
    key = {
        'identifier': str(LAST_VARIANT_KEY)
    }

    expression_attribute_values = {
        ':value': str(product_sku)
    }

    update_expression = "set last_variant=:value"

    schema = {
        'Key': key,
        'UpdateExpression': str(update_expression),
        'ExpressionAttributeValues': expression_attribute_values
    }

    update_item(table_name=DYNAMODB_TABLE_NAME, schema=schema)


def save_last_updated_to_dynamo_db(last_updated_at):
    key = {
        'identifier': str(LAST_UPDATED_KEY)
    }

    expression_attribute_values = {
        ':value': str(last_updated_at)
    }

    update_expression = "set last_value=:value"

    schema = {
        'Key': key,
        'UpdateExpression': str(update_expression),
        'ExpressionAttributeValues': expression_attribute_values
    }

    update_item(table_name=DYNAMODB_TABLE_NAME, schema=schema)


def save_state_in_dynamodb(availability_export_id, export_job_state, upto_date):
    job_id = availability_export_id

    key = {
        'identifier': str(JOB_STATE_KEY)
    }

    job_state = export_job_state

    expression_attribute_values = {
        ':job_state': str(job_state),
        ':job_id': str(job_id),
        ':upto_date': str(upto_date)
    }

    update_expression = "set availability_export_id=:job_id, availability_job_state=:job_state, availability_data_upto_date=:upto_date"

    schema = {
        'Key': key,
        'UpdateExpression': str(update_expression),
        'ExpressionAttributeValues': expression_attribute_values
    }

    update_item(table_name=DYNAMODB_TABLE_NAME, schema=schema)


async def _save_to_queue_availabilities(context, event):
    """Validates the type of the request and sends it to respective handler

    Arguments:
        event {dic} -- lambda event dictionary

    Returns:
        dic -- run result. Can be a simple dic or a API gateway rest result
    """
    if 'availability_export_id' in event:
        LOGGER.info('Saving the availability job id in dynamodb and recalling again in 5 mins')
        save_state_in_dynamodb(event['availability_export_id'], 'processing', 'no')
        events_handler = EventsHandler()
        await events_handler.update_trigger(
            os.environ.get('TRIGGER_NAME', WORKER_TRIGGER_DEFAULT_NAME), "rate(5 minutes)", True)
        return {'result': 'Retrying again after 5 minute'}
    else:
        response = get_availability_job_id()
        if response['availability_export_id'] and response['availability_job_state']:
            LOGGER.info(
                f"Found current job from dynamo db table - as -- {str(response['availability_export_id'])} and previous state -- {response['availability_job_state']}")
            ## Possible inputs -- availability id respoinse
            return await wait_job_and_process_variants(context, response)
        else:
            LOGGER.info(
                'Could not find which method to process variants to queue; make sure it is a api gateway request or a lambdas invocation with the id inside.')


async def wait_job_and_process_variants(context, event):
    """Gets export job to see if it can process availabilities.
    If not finised waits 1 min and invokes this lambda to make another run.

    Arguments:
        event {dic} -- Lambda Event dic containig the export job id reference
    """
    if event['availability_data_upto_date'] == 'yes':
        LOGGER.info('Exported all the data to shopify -- ignoring the job.')
        return {'result': 'All variants exported to the queue'}

    events_handler = EventsHandler()
    newstore_credentials = json.loads(
        PARAM_STORE.get_param('newstore'))
    ns_handler = NewStoreConnector(tenant=newstore_credentials['tenant'], context=context,
                                   username=newstore_credentials['username'], password=newstore_credentials['password'],
                                   host=newstore_credentials['host'])

    '''
    Example
    {
        "availability_export_id": "684d48df-28e0-459b-bd1f-1d32035e72ca"
    }
    '''
    export_job = ns_handler.get_availability_export(event['availability_export_id'])

    if export_job.get('state') == 'processing':
        LOGGER.info('Job is still processing calling again in 5 mins')
        events_handler = EventsHandler()
        await events_handler.update_trigger(
            os.environ.get('TRIGGER_NAME', WORKER_TRIGGER_DEFAULT_NAME), "rate(5 minutes)", True)
        return {'result': 'Retrying again after 5 minute'}

    if 'last_updated_at' in export_job and int(export_job['last_updated_at']) == 0:
        LOGGER.info('Exported all the data to shopify -- ignoring the job.')
        save_state_in_dynamodb(event['availability_export_id'], 'finished', 'yes')
        await events_handler.update_trigger(
            os.environ.get('TRIGGER_NAME', WORKER_TRIGGER_DEFAULT_NAME), "rate(1 hour)", True)
        return {'result': 'All variants exported to the queue'}

    if export_job.get('state') == 'finished' and event['availability_data_upto_date'] == 'no':
        link = str(export_job['link'])
        last_updated_at = str(export_job['last_updated_at'])
        variants = await read_variants_from_export_file(link, last_updated_at)

        if await _process_variants_to_queue(variants, context):
            await events_handler.update_trigger(
                os.environ.get('TRIGGER_NAME', WORKER_TRIGGER_DEFAULT_NAME), "rate(1 hour)", True)
            save_last_variant('')
            save_state_in_dynamodb(event['availability_export_id'], 'finished', 'yes')
            LOGGER.info('Getting export job id...')
            return {'result': 'All variants exported to the queue'}
        else:
            LOGGER.info('Not all variants exported before timeout')
            return {'result': 'Not all variants exported before timeout'}


## Location where all the operations happen.
async def read_variants_from_export_file(file_url, last_updated_at):
    """Gets a file from a URl, saves it to an S3 and returns the file contents

    Arguments:
        file_url {string} -- The file URL
        last_updated_at {string} -- The last updated value used to save the file in s3

    Returns:
        dic -- Saved file data in dictionary
    """
    s3_handler = S3Handler(
        bucket_name=os.environ["S3_BUCKET_NAME"],
        key_name='helpers/exports/{last_updated_at}/inventory_levels.json'.
            format(last_updated_at=last_updated_at)
    )
    variants = None

    save_last_updated_to_dynamo_db(last_updated_at)

    file = await _get_file(url=file_url)

    LOGGER.info('Uploading file to s3')

    # s3_handler.put_object(data=file)

    variants = json.loads(file)

    s3_handler.put_object(data=json.dumps(variants))

    return variants


async def _process_variants_to_queue(variants, context):
    """Puts the variants into a message and drops it to a queue

    Arguments:
        variants {dic} -- Variants list

    Returns:
        object -- Result Drop to queue message object
    """
    LOGGER.info(f'Processing {len(variants)} variants from the currently exported job.')

    global MAPPING_ENTRIES
    MAPPING_ENTRIES = get_all(table_name=DYNAMODB_MAPPING_TABLE_NAME)

    sqs_handler = SqsHandler(queue_name=os.environ["SQS_NAME"])
    locations_map = json.loads(PARAM_STORE.get_param('shopify/dc_location_id_map'))

    result = True
    export_count = len(variants)
    mapped_inventory_ids = []
    shopify_inventory_item_list = []
    counter = 0

    LOGGER.info('Processing variants from the json')
    if export_count < 1000:
        LOGGER.debug(variants)

    LOGGER.info(f'Export items count to push: {export_count}')

    previous_product_id = None
    last_product_id = get_last_variant()

    for variant in variants:
        # Check that all products for all locations have been processed before we stop before the timeout
        if stop_before_timeout(context, STOP_BEFORE_TIMEOUT, LOGGER) \
            and previous_product_id is not None \
            and previous_product_id != variant.get('product_id'):
            LOGGER.info(
                f'Previous Product Id: {previous_product_id} - save next product id {variant.get("product_id")}')
            save_last_variant(variant.get('product_id'))
            result = False
            break

        if last_product_id != '':
            if variant.get('product_id') == last_product_id:
                LOGGER.info(f'Continue with Product Id: {last_product_id}')
                last_product_id = ''
            else:
                continue

        previous_product_id = variant.get('product_id')

        # Skip products with / to avoid invalid inventory update in Shopify caused by
        # duplicate products - special issue for F&O
        if '/' in variant.get('product_id'):
            LOGGER.info(f'Invalid product id, skip {variant.get("product_id")}')
            continue

        atp = int(variant['atp'])
        shopify_inventory_ids = {}
        current_location_ids = None
        fulfillment_node_id = variant.get('fulfillment_node_id', None)

        if 'external_identifiers' in variant:
            if len(variant['external_identifiers']) > 0:
                external_identifier = variant['external_identifiers'][0]
                identifiers = external_identifier['identifiers']

                if 'shopify_inventory_item_id' in identifiers:
                    shopify_inventory_ids['cad'] = identifiers['shopify_inventory_item_id']

        if not shopify_inventory_ids.get('cad'):
            product_id = variant.get('product_id')
            LOGGER.debug(f'Shopify inventory ID for CAD not present for "{product_id}", skipping variant')
            continue

        # Due to a data issue inventory records might be exported twice for a location
        # one for the english and one for the french catalog; and it could also be its only exported
        # for the english OR the french catalog - so we check here for duplicates
        if shopify_inventory_ids.get('cad') + '-' + fulfillment_node_id in mapped_inventory_ids:
            LOGGER.debug(
                f'Skip inventory record since its already in the list {shopify_inventory_ids.get("cad")}-{fulfillment_node_id}')
            continue
        mapped_inventory_ids.append(shopify_inventory_ids.get('cad') + '-' + fulfillment_node_id)

        shopify_inventory_id_usd = _get_usd_inventory_item_id(variant.get('product_id'))
        if shopify_inventory_id_usd:
            shopify_inventory_ids['usd'] = shopify_inventory_id_usd

        if fulfillment_node_id in locations_map:
            current_location_ids = locations_map[fulfillment_node_id]

        if current_location_ids is None:
            product_id = variant.get('product_id')
            LOGGER.debug(
                f'Fulfillment node ID "{fulfillment_node_id}" not mapped to location for "{product_id}", skipping variant')
            continue

        if len(shopify_inventory_item_list) == 100:
            await _drop_to_queue(sqs_handler, shopify_inventory_item_list)
            counter = counter + 1
            LOGGER.info(f'Pushed message number {counter} to queue {sqs_handler.queue_name}')
            mapped_inventory_ids = []
            shopify_inventory_item_list = []

        shopify_inventory_item_list.append({
            'atp': atp,
            'shopify_inventory_ids': shopify_inventory_ids,
            'location_ids': current_location_ids
        })

    pushed_count = counter * 100 + len(shopify_inventory_item_list)

    if len(shopify_inventory_item_list) > 0:
        await _drop_to_queue(sqs_handler, shopify_inventory_item_list)
        LOGGER.info(f'Pushed last message to queue {sqs_handler.queue_name}')
        mapped_inventory_ids = []
        shopify_inventory_item_list = []

    LOGGER.info(f'Export items count: {export_count}')
    LOGGER.info(f'Pushed items count: {pushed_count}')
    return result


async def _drop_to_queue(sqs_handler, message_availability):
    await sqs_handler.push_message(message=json.dumps(message_availability))


async def _get_file(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url=url) as response:
            response.raise_for_status()
            response_data = await response.read()
            if '.zip' in url:
                with io.BytesIO(response_data) as tf:
                    # rewind the file
                    tf.seek(0)
                    # Read the file as a zipfile and process the members
                    with zipfile.ZipFile(tf, mode='r') as zipf:
                        for subfile in zipf.namelist():
                            if subfile.startswith('__MACOSX'):
                                continue
                            # Assumes only one.
                            return zipf.read(subfile)

            LOGGER.info("Responding with data from zip file")
            return response_data


def _get_usd_inventory_item_id(product_sku):
    global MAPPING_ENTRIES

    mapping_entry = next(filter(lambda item: item['product_sku'] == product_sku, MAPPING_ENTRIES), None)

    if mapping_entry is None:
        LOGGER.debug('No mapping entry found for usd.')
        return None

    LOGGER.debug(f'Mapping entry found for usd: {mapping_entry}')
    return mapping_entry['shopify_inventory_item_id']


def _set_blocked_state(blocked):
    update_item(table_name=DYNAMODB_TABLE_NAME, schema={
        'Key': {
            'identifier': str(CONCURRENT_EXECUTION_BLOCKED_KEY)
        },
        'UpdateExpression': 'set blocked=:value',
        'ExpressionAttributeValues': {
            ':value': str(blocked)
        }
    })


def _is_blocked():
    response = get_item(table_name=DYNAMODB_TABLE_NAME, item={
        'identifier': str(CONCURRENT_EXECUTION_BLOCKED_KEY)
    })

    if response is None:
        return False

    if 'blocked' in response:
        return response['blocked'] == 'True'

    return False
