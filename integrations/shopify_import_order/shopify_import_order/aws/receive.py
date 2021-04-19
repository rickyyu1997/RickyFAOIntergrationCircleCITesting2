# -*- coding: utf-8 -*-
# Copyright (C) 2015, 2016 NewStore, Inc. All rights reserved.
import os
import boto3
import simplejson as json
import logging
from shopify_import_order.handlers.verify_webhook import verify_webhook
from shopify_import_order.handlers.utils import Utils

LOGGER = logging.getLogger(__name__)
LOG_LEVEL_SET = os.environ.get('LOG_LEVEL', 'INFO') or 'INFO'
LOG_LEVEL = logging.DEBUG if LOG_LEVEL_SET.lower() in ['debug'] else logging.INFO
LOGGER.setLevel(LOG_LEVEL)
TWO_DAYS = 172800
PROCESS_ORDER_LAMBDA_NAME = os.environ.get('PROCESS_ORDER_LAMBDA_NAME', '')


def handler(event, context): # pylint: disable=W0613
    """
    Start of the lambda handler
    :param event: Shopify webhook order json
    :param context: function context
    """
    try:
        LOGGER.debug(f'Received Shopify event: {json.dumps(event, indent=4)}')
        order = event['body']
        order_id = event['headers'].get('X-Shopify-Order-Id')

        LOGGER.debug(json.dumps(order))
        ## Making sure that the newstore -- tags are not added to the order.
        json_object = json.loads(order)
        existing_tags = [tag.strip() for tag in json_object.get('tags', '').split(',')]
        stop_retry_tags = os.environ.get('STOP_RETRY_TAGS', 'newstore_created').split(",")
        if any(tag in stop_retry_tags for tag in existing_tags):
            LOGGER.debug(f'Not processing Order with id {order_id} -- request because it has {existing_tags} - tags')
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Order already processed by newstore'})
            }
        shopify_config = json.loads(Utils.get_instance().get_parameter_store().get_param('shopify'))
        shopify_secret = shopify_config['shared_secret']

        hmac = event['headers'].get('X-Shopify-Hmac-Sha256')
        if not verify_webhook(order, shopify_secret, hmac):
            LOGGER.error(f'Error verifying Shopify HMAC: {json.dumps(event, indent=4)}', exc_info=True)
            return {'statusCode': 500}

        LOGGER.info(f'Hmac check passed, sending order {order_id} to processor lambda')

        process_order_call(PROCESS_ORDER_LAMBDA_NAME, order)

    except Exception:
        LOGGER.exception('Failed to create Order')

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Ack order received'})
    }


def process_order_call(function_name, payload, params=None):
    session = boto3.session.Session()
    lambda_cli = session.client('lambda')
    return lambda_cli.invoke(
        FunctionName=function_name,
        InvocationType='Event',
        Payload=json.dumps({'body': payload, 'queryStringParameters': params}),
    )