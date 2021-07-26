# pylint: disable=R0904
# -*- coding: utf-8 -*-
# Copyright (C) 2015, 2016, 2017 NewStore, Inc. All rights reserved.

import os
import json
import logging
import asyncio
import collections

# Runs startup processes
import netsuite.netsuite_environment_loader  # pylint: disable=W0611
import netsuite.api.customer as nsac
from netsuite.api.sale import (
    get_cash_sale,
    get_sales_order,
    get_return_authorizations,
    get_transaction_with_created_from_id,
    get_transaction_with_custom_field,
    initialize_record
)
from netsuite.api.refund import create_cash_refund, create_credit_memo, create_customer_refund
from netsuite.service import (
    CashRefundItemList,
    CreditMemoItemList,
    CustomerAddressbook,
    CustomerAddressbookList,
    RecordRef,
    CustomerRefundApplyList
)
from netsuite.utils import get_record_by_type
from zeep.helpers import serialize_object
from newstore_adapter.utils import verify_and_update_missing_payment_info
import netsuite_returns_injection.transformers.process_order_return as por
import netsuite_returns_injection.transformers.process_web_order_return as pwor
import netsuite_returns_injection.helpers.sqs_consumer as sqs_consumer
from netsuite_returns_injection.transformers.return_transformer import get_store_tz_by_customer_order
from netsuite_returns_injection.helpers.utils import Utils

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
SQS_QUEUE = os.environ['SQS_QUEUE']
TREAT_ALL_ORDERS_AS_HISTORICAL = bool(
    os.environ.get('TREAT_ALL_ORDERS_AS_HISTORICAL', False))


def handler(_, context):
    """
    Reads event payloads from an SQS queue. The payload is taken from the order
    event stream, detailed here:
        https://apidoc.newstore.io/newstore-cloud/hooks_eventstream.html
    Event Type: return.processed
    """
    LOGGER.info('Beginning queue retrieval...')
    # Initialize newstore connector
    Utils.get_newstore_conn(context)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = sqs_consumer.consume(process_return, SQS_QUEUE)
    loop.run_until_complete(task)
    loop.stop()
    loop.close()


async def process_return(message):
    LOGGER.info(f"Message to process: \n {json.dumps(message, indent=4)}")
    ns_return = message['payload']

    # The customer_order endpoint is defined in the `affiliate app` end points
    customer_order_envelope = Utils.get_newstore_conn(
    ).get_customer_order(ns_return['order_id'])
    if not customer_order_envelope:
        raise Exception(
            f'Customer order for order id {ns_return["order_id"]} not found on NewStore')
    LOGGER.info(
        f'customer_order_envelope: \n{json.dumps(customer_order_envelope, indent=4)}')

    customer_order = customer_order_envelope['customer_order']

    # Fix this field with the timestamp we find in the customer order's timeline
    # only if the order_timeline is present, otherwise utilize the original returned_at
    if customer_order.get('order_timeline'):
        ns_return['returned_at'] = get_return_timeline_timestamp(
            ns_return, customer_order)

    is_endless_aisle = Utils.is_endless_aisle(order_payload=customer_order)

    # get_payments retrieves a payment account which are assigned the same ID as
    # the entity it is assigned to, which is why we're passing in the order_id
    payments_info = Utils.get_newstore_conn(
    ).get_payments(ns_return['order_id'])
    if not payments_info:
        raise Exception(
            'Payment Account for order %s not found on NewStore' % ns_return['order_id'])
    LOGGER.info(f'payments_info: \n{json.dumps(payments_info, indent=4)}')

    # The lambda should not ignore returns to historical orders, it should just ignore historical returns
    if check_historic_return(payments_info, ns_return['id']):
        LOGGER.info('Skipping Historical Return...')
        return True

    store_tz = await get_store_tz_by_customer_order(customer_order)
    LOGGER.info(f'Timezone: {store_tz}')

    # In case of Endless Aisle channel_type is store, but it needs to be treated as web
    if customer_order['channel_type'] == 'web' or is_endless_aisle:
        payments_info = await _update_payments_info_if_needed(payments_info=payments_info,
                                                              customer_order=customer_order)
        return await _handle_web_order_return(customer_order, ns_return, payments_info)

    if customer_order['channel_type'] == 'store':
        return await _handle_store_order_return(customer_order, ns_return, payments_info, store_tz)

    return False


async def _update_payments_info_if_needed(customer_order, payments_info):
    if customer_order['channel_type'] != 'web':
        return payments_info
    updated_payments_info = verify_and_update_missing_payment_info(
        Utils.get_shopify_conn(), payments_info, customer_order)
    return updated_payments_info


async def _handle_store_order_return(customer_order, ns_return, payments_info, store_tz):
    cash_sale = None
    is_historical = Utils.get_extended_attribute(
        customer_order['extended_attributes'], 'is_historical')

    if not (is_historical and is_historical.lower() == 'true'):
        # Retrieve the associated order from NetSuite
        cash_sale = get_cash_sale(customer_order['sales_order_external_id'])
        if not cash_sale:
            raise Exception(
                f"CashSale for order {customer_order['sales_order_external_id']} not found on NetSuite")

        # LOGGER.info(f'CashSale: \n{cash_sale}')

        # In case of return for exchanged order, original order payment must be fetched
        original_order = await _get_original_order(cash_sale)
        if original_order and original_order['internalId'] != cash_sale['internalId']:
            ext_order = Utils.get_newstore_conn().get_external_order(original_order.externalId)
            payments_info = Utils.get_newstore_conn(
            ).get_payments(ext_order['order_uuid'])
            if not payments_info:
                raise Exception(
                    'Payment Account for original order %s not found on NewStore' % original_order.externalId)
            LOGGER.info(
                f'Original Order Payment Info: {json.dumps(payments_info, indent=4)}')

    return_parsed = await por.transform_order(cash_sale, ns_return, payments_info, customer_order, store_tz)
    LOGGER.info(
        f"Transformed return: \n{json.dumps(serialize_object(return_parsed), indent=4, default=Utils.json_serial)}")

    if 'customer' in return_parsed and return_parsed['customer']:
        customer = get_or_create_customer(return_parsed)
        if not customer:
            raise Exception('The customer could not be created')
        return_parsed['cash_refund']['entity'] = RecordRef(
            internalId=customer['internalId'])

    cash_refund = return_parsed['cash_refund']
    payment_items = return_parsed['payment_items']
    cash_refund_items = return_parsed['cash_refund_items']
    cash_refund['itemList'] = CashRefundItemList(
        cash_refund_items + payment_items)
    result, refund = create_cash_refund(cash_refund)
    if result:
        return result

    LOGGER.info('Error on creating Cash Refund. Cash Refund not created.')
    for status in refund.status.statusDetail:
        if status.code == 'DUP_RCRD':
            LOGGER.info(
                'This record already exists in NetSuite and will be removed from queue.')
            return True
    return result


async def _handle_web_order_return(customer_order, ns_return, payments_info):
    returned_product_ids = [item['product_id'] for item in ns_return['items']]
    # Verify if return was in store or via web, if it was via web returned_from must be DC
    # Either way it should have a sales_order
    order_id = ns_return['order_id']
    external_order_id = customer_order['sales_order_external_id']

    is_historical = Utils.get_extended_attribute(
        customer_order['extended_attributes'], 'is_historical')
    if not (is_historical and is_historical.lower() == 'true'):
        # When partner is NewStore, the search uses the order id
        sales_order = search_sales_order(order_id, external_order_id)
        if not sales_order:
            LOGGER.info(
                f'SalesOrder for order {ns_return["order_id"]} not found on NetSuite')
            return False

        # LOGGER.info(f'Sales Order: {sales_order}')

        netsuite_config = Utils.get_netsuite_config()
        if TREAT_ALL_ORDERS_AS_HISTORICAL or ns_return.get('is_historical'):
            LOGGER.info(
                'Return is historical. Proceed to create standalone CashRefund')
            return_parsed = await pwor.transform_web_order(customer_order, ns_return, payments_info, sales_order,
                                                           None)
        elif ns_return['return_location_id'].endswith(netsuite_config['dc_suffix']) or ns_return['return_location_id'] == 'USC':
            LOGGER.info(
                'Return is online, proceed to create a CustomerRefund from a ReturnAuthorization')
            result, return_authorizations = get_return_authorizations(
                sales_order.internalId)
            LOGGER.info(
                f'Result: {result}; \n Return Authorizations: {return_authorizations}')
            if not (result and return_authorizations and len(return_authorizations) > 0):
                LOGGER.info(f'ReturnAuthorization for order {ns_return["order_id"]} and SalesOrder {sales_order.tranId} not'
                            f' found on NetSuite')
                return False

            return_authorization = await get_return_authorization(return_authorizations, returned_product_ids)
            if not return_authorization:
                LOGGER.error(
                    'There\'s no ReturnAuthorization currently on NetSuite that matches this return.')
                return False
            return_parsed = await pwor.transform_web_order(customer_order, ns_return, payments_info, sales_order,
                                                           return_authorization)
            LOGGER.info(
                f'Parsed Transformed Online order:{return_parsed}')
        else:
            LOGGER.info(
                'Return is in store, proceed to create standalone CashRefund')
            return_parsed = await pwor.transform_web_order(customer_order, ns_return, payments_info, sales_order,
                                                           None)
            LOGGER.info(
                f'Parsed Transformed Store order:{return_parsed}')
            return inject_store_return(return_parsed)

    else:
        LOGGER.info('Order is historical, proceed to create standalone record.')
        return_parsed = await pwor.transform_web_order(customer_order, ns_return, payments_info, None,
                                                       None)
        LOGGER.info(
            f'Parsed Transformed Historical order:{return_parsed}')

    return inject_web_return(return_parsed, payments_info)


def get_return_timeline_timestamp(ns_return, customer_order):
    """
    Parses the order timeline in the customer order for the ns_return inside and returns its timestamp
    :param ns_return: The return looked for in the customer order
    :param customer_order: The order containing the order timeline to parse
    :return: The timestamp of the correlating return in the order timeline
    """
    ns_returned_at_date = ns_return.get('returned_at')
    if ns_returned_at_date is None:
        raise Exception(
            f'Could not find required key returned_at in return data: {ns_return}.')
    if len(ns_returned_at_date) == 0:
        raise Exception(
            f'Expected item ids in newstore return but none were found: {ns_return}.')

    ns_returned_items = ns_return.get('items')
    if ns_returned_items is None or len(ns_returned_items) == 0:
        raise Exception(
            f'Could not find items in the return data: {ns_return}.')

    ns_returned_item_ids = list(
        map(lambda item: item.get('product_id'), ns_returned_items))

    order_timeline = customer_order.get('order_timeline')
    if order_timeline is None:
        raise Exception(f'Could not find required key order_timeline in customer order data:'
                        f' {customer_order}.')
    if len(order_timeline) == 0:
        raise Exception(
            f'Expected key order_timeline in customer order to contain items to check for returns.')

    correct_returned_at_timestamp = None
    for timeline_item in order_timeline:
        timeline_returned_items = timeline_item.get(
            'payload', {}).get('returned_items')
        if timeline_returned_items is None or len(timeline_returned_items) == 0:
            continue

        timeline_returned_item_ids = list(
            map(lambda x: x.get('id'), timeline_returned_items))
        if collections.Counter(timeline_returned_item_ids) != collections.Counter(ns_returned_item_ids):
            continue

        LOGGER.debug(
            'Found order_timeline entry that matches the items in the ns_return.')
        correct_returned_at_timestamp = timeline_item.get('created_at')
        if correct_returned_at_timestamp is None:
            raise Exception(f'timeline item was found in customer order with matching items but did not have the '
                            f'expected created_at date. Timeline item: {timeline_item}')

    if correct_returned_at_timestamp is None:
        raise Exception(f'Failed to find the returned items in the order timeline.'
                        f' \nReturned item ids: {ns_returned_item_ids}'
                        f' \norder_timeline data: {json.dumps(order_timeline)}')
    return correct_returned_at_timestamp


async def _verify_return_authorization(return_authorization, returned_product_ids):
    return_authorization_items = []
    for item in return_authorization.itemList.item:
        product_id = get_product_id_by_netsuite_name(
            item.item.name, returned_product_ids)
        if product_id is not None:
            return_authorization_items.append(product_id)
    LOGGER.info(f'Returned product ids {returned_product_ids}')
    LOGGER.info(f'return_authorization_items: {return_authorization_items}')
    return_authorization_items.sort()
    returned_product_ids.sort()
    if not returned_product_ids == return_authorization_items:
        return False

    # All that's left is to verify if this return authorization was already utilized by another CashRefund
    result, cash_refund = get_transaction_with_created_from_id(
        return_authorization.internalId, '_cashRefund')
    if not result:
        raise Exception(
            'An error occurred while searching for a CashRefund created from the return authorization')

    if cash_refund:
        LOGGER.info(
            'This return authorization was already utilized in another CashRefund, go to next')
        return False
    return True


async def _get_original_order(cash_sale):
    original_order_cf_value = _get_custom_field_value(cash_sale['customFieldList'],
                                                      'custbody_exchange_nws_orginalorder')
    if original_order_cf_value:
        cash_sale = get_record_by_type(
            'cashSale', original_order_cf_value.internalId)
        _get_original_order(cash_sale)
    return cash_sale


def _get_custom_field_value(custom_fields_list, field):
    for custom_field in custom_fields_list['customField']:
        if custom_field['scriptId'] == field:
            return custom_field['value']
    return ''


def get_or_create_customer(order_data):
    customer = order_data['customer']
    shipping_addr = order_data.get('shipping_address')
    billing_addr = order_data.get('billing_address')

    addressbook = []
    if shipping_addr:
        addressbook.append(CustomerAddressbook(**shipping_addr))

    if billing_addr:
        addressbook.append(CustomerAddressbook(**billing_addr))

    if addressbook:
        addr_book_list = {
            'replaceAll': True,
            'addressbook': addressbook
        }
        customer_addr_book_list = CustomerAddressbookList(**addr_book_list)
        customer['addressbookList'] = customer_addr_book_list

    # If customer is found we update it, but only if it is the same subsidiary
    netsuite_customer_internal_id = nsac.lookup_customer_id_by_name_and_email(
        customer)
    if netsuite_customer_internal_id:
        LOGGER.info('Customer exists, we will update it')
        customer['internalId'] = netsuite_customer_internal_id
        return nsac.update_customer(customer)

    LOGGER.info('Create new Customer.')
    # This returns the Customer or None if there is any error
    return nsac.create_customer(customer)


def search_sales_order(order_id, external_order_id):
    sales_order = get_sales_order(order_id)
    if not sales_order:
        LOGGER.info(
            f'Search for SalesOrder using order id, {order_id}, not found, try with external order id.')
        # When partner is Dell Boomi, the search uses the external order id
        sales_order = get_sales_order(external_order_id)
        if not sales_order:
            LOGGER.info(
                f'Search for SalesOrder using external order id, {external_order_id} not found, try with custom field order number.')
            # If the SalesOrder was not found by the searchs above then we have a last way to search
            _, sales_order = get_transaction_with_custom_field(
                external_order_id,
                'custbody_nws_ecom_orderinternalid',
                '_salesOrder'
            )
    return sales_order


async def get_return_authorization(return_authorizations, returned_product_ids):
    # If more then one return is made we need to make sure we get the right return authorization
    # For that we check each return authorization returned
    return_authorization = None
    for authorization in return_authorizations:
        if await _verify_return_authorization(authorization, returned_product_ids):
            return_authorization = authorization
    return return_authorization

#
# Sends the store return as CashRefund  and CreditMemo to Netsuite.
# F&O wants that both objects are created.
#


def inject_store_return(return_parsed):
    if 'customer' in return_parsed and return_parsed['customer']:
        customer = get_or_create_customer(return_parsed)
        if not customer:
            raise Exception('The customer could not be created')
        return_parsed['cash_refund']['entity'] = RecordRef(
            internalId=customer['internalId'])

    cash_refund_result = inject_cash_refund(return_parsed)

    credit_memo_result = inject_credit_memo(return_parsed)

    return cash_refund_result and credit_memo_result

#
# Sends the web return as CustomerRefund and CreditMemo to Netsuite.
# F&O wants that both objects are created.
#


def inject_web_return(return_parsed, payments_info):
    if 'customer' in return_parsed and return_parsed['customer']:
        customer = get_or_create_customer(return_parsed)
        if not customer:
            raise Exception('The customer could not be created')
        return_parsed['cash_refund']['customer'] = RecordRef(
            internalId=customer['internalId'])

    credit_memo_result = inject_credit_memo(return_parsed)
    LOGGER.info(f'Credit memo result {credit_memo_result}')

    initialized_record = initialize_record(
        'customerRefund', 'creditMemo', credit_memo_result['internalId'])
    LOGGER.info(f'Initizalized CreditRefund Response {initialized_record}')

    customer_refund_payload = prepare_customer_refund_payload(
        credit_memo_result, initialized_record, payments_info)
    cash_refund_result = inject_customer_refund(
        customer_refund_payload, initialized_record)

    return credit_memo_result and cash_refund_result


#
# Injects a Customer Refund into Netsuite
#
def inject_customer_refund(customer_refund_payload, initialized_record):
    customer_refund = customer_refund_payload['customer_refund']
    refund_items = initialized_record['applyList']['apply']
    customer_refund['applyList'] = CustomerRefundApplyList(
        refund_items)
    result, response = create_customer_refund(customer_refund)

    if not result:
        for status in response.status.statusDetail:
            if status.code == 'DUP_RCRD':
                LOGGER.info(
                    'This Customer refund record already exists in NetSuite and will be removed from queue.')
                return True
        LOGGER.error(
            'Error on creating Customer Refund. Cash Refund not created.')

    return result


#
# Injects a Cash Refund into Netsuite
#
def inject_cash_refund(return_parsed):
    cash_refund = return_parsed['cash_refund']
    cash_refund_items = return_parsed['cash_refund_items']
    payment_items = return_parsed['cash_refund_payment_items']
    cash_refund_items_list = cash_refund_items + payment_items
    cash_refund['itemList'] = CashRefundItemList(cash_refund_items_list)
    result, response = create_cash_refund(cash_refund)

    if not result:
        for status in response.status.statusDetail:
            if status.code == 'DUP_RCRD':
                LOGGER.info(
                    'This cash refund record already exists in NetSuite and will be removed from queue.')
                return True
        LOGGER.error('Error on creating Cash Refund. Cash Refund not created.')

    return result


def prepare_customer_refund_payload(credit_memo_result, initialized_record, payments_info):
    if not payments_info['instruments']:
        raise Exception("No payment instrument in payment info from NewStore")
    if len(payments_info['instruments']) > 1:
        LOGGER.warning(
            'This use case wasnt discussed. We take the first payment method and use that to refund.')
    payment_method = payments_info['instruments'][0]['payment_method']
    payment_provider = payments_info['instruments'][0]['payment_provider']
    currency = payments_info['instruments'][0]['currency']
    customer_refund = {
        "tranDate": initialized_record['tranDate'],
        "location": RecordRef(internalId=credit_memo_result['location']['internalId']),
        "customForm": RecordRef(internalId=41),
        "customer": RecordRef(internalId=initialized_record['customer']['internalId']),
        "arAcct": RecordRef(internalId=initialized_record['arAcct']['internalId']),
        "currencyName": initialized_record['currencyName'],
        "paymentMethod": RecordRef(internalId=Utils.get_payment_item_id(payment_method, payment_provider, currency, 'methods')),
        "currency":  initialized_record['currency']['internalId'],
        "account": RecordRef(internalId=initialized_record['account']['internalId']),
        "toBePrinted": False,
        "tranId": initialized_record['tranId'],
        "subsidiary":  RecordRef(internalId=initialized_record['subsidiary']['internalId']),
        "ccApproved":  False
    }

    return {
        "customer_refund": customer_refund
    }

#
# Injects a Credit Memo into Netsuite
#


def inject_credit_memo(return_parsed):
    credit_memo = return_parsed['credit_memo']
    credit_memo_items = return_parsed['credit_memo_items']
    payment_items = return_parsed['credit_memo_payment_items']
    credit_memo_items_list = credit_memo_items + payment_items
    credit_memo['itemList'] = CreditMemoItemList(credit_memo_items_list)
    result, response, internal_id = create_credit_memo(credit_memo)
    credit_memo_trans_id = response['tranId']
    external_id = response['externalId']

    # Take all the attributes from the CreditMemo response
    LOGGER.info(
        f'InternalID: {internal_id}, CreditMemoTransID: {credit_memo_trans_id}, ExternalID {external_id}')

    if not result:
        for status in response.status.statusDetail:
            if status.code == 'DUP_RCRD':
                LOGGER.info(
                    'This credit memo record already exists in NetSuite and will be removed from queue.')
                return True
        LOGGER.error('Error on creating Credit Memo. Credit Memo not created.')

    return response


def check_historic_return(payments, return_id):
    for payment in payments['instruments']:
        for original_payment in payment['original_transactions']:
            if is_historical_return(original_payment, return_id):
                return True
    return False


def is_historical_return(original_payment, return_id):
    if original_payment['reason'] == 'refund' \
            and original_payment['correlation_id'] == return_id \
            and original_payment.get('metadata', {}):
        extended_attributes = original_payment.get(
            'metadata', {}).get('extended_attributes', [])
        is_historical = dict(next(
            (attribute for attribute in extended_attributes if attribute['name'] == 'is_historical'), {}))

        if is_historical.get('value', 'False').lower() in ('1', 'yes', 'true', 1):
            LOGGER.info(
                'This is a historical return and will not be processed. Discarding....')
            return True

    return False

#
# Parses the product id from the netsuite item name
#


def get_product_id_by_netsuite_name(netsuite_name, returned_product_ids):
    product_id = None
    for possible_item_id in netsuite_name.split(' '):
        if possible_item_id in returned_product_ids:
            product_id = possible_item_id
            break
    return product_id
