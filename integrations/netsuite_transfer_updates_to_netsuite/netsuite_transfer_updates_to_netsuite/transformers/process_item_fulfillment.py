import logging
from netsuite_transfer_updates_to_netsuite.helpers.utils import Utils
from netsuite.service import (
    RecordRef
)

LOGGER = logging.getLogger(__file__)
LOGGER.setLevel(logging.INFO)


def transform_item_fulfillment(asn_created_event, products_info, store, item_fulfillment):
    tran_date = Utils.format_datestring_for_netsuite(date_string=asn_created_event['shipment_date'],
                                                     time_zone=store['timezone'])
    return {
        **item_fulfillment,
        'tranDate': tran_date.date(),
        'customForm': RecordRef(internalId=int(Utils.get_netsuite_config()['item_fulfillment_custom_form_internal_id'])),
        'shipStatus': '_shipped',
        'itemList': {
            'item': map_item_fulfillment_items(products_info, item_fulfillment),
            'replaceAll': False
        },
        'customFieldList': None
    }


def map_item_fulfillment_items(products_info, item_fulfillment):
    output = []
    items_to_fulfill = {int(item['netsuite_internal_id']):item for item in products_info}
    for item in item_fulfillment['itemList']['item']:
        if int(item['item']['internalId']) in items_to_fulfill:
            output.append({
                **item,
                'itemReceive': True,
                'quantity': items_to_fulfill[int(item['item']['internalId'])]['quantity'],
                'customFieldList': None
            })
            del items_to_fulfill[int(item['item']['internalId'])]
    return output