#!/usr/bin/env python3
import os
import json
import re

import backoff
import requests
import singer
from singer.schema import Schema
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.transform import transform
from datetime import datetime, timedelta

REQUIRED_CONFIG_KEYS = ["start_date", "host_url", "client_id", "client_secret", "refresh_token"]
LOGGER = singer.get_logger()

# Exact Online server HOST is country dependent.
# countries = {Netherlands: ".nl", Belgium: ".be", Germany: ".de", United Kingdom: ".co.uk", USA: ".com", Spain: ".es"}
DEFAULT_HOST = "https://start.exactonline.nl"

URIS = {
    "me": "/api/v1/current/Me",
    "refresh": "/api/oauth2/token",
    "sales_invoices": "/api/v1/{division}/salesinvoice/SalesInvoices",
    "purchase_invoices": "/api/v1/{division}/purchase/PurchaseInvoices",
    "gl_accounts": "/api/v1/{division}/financial/GLAccounts",
    "bank_entry_lines": "/api/v1/{division}/financialtransaction/BankEntryLines",
    "general_journal_entry_lines": "/api/v1/{division}/generaljournalentry/GeneralJournalEntryLines"
}


class ExactRateLimitError(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def camel_to_snake_case(name):
    """
    AssimilatedVatBox  --> assimilated_vat_box
    """
    sn = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    sn = sn.replace("i_d", "id")
    sn = sn.replace("d_c", "dc")
    sn = sn.replace("f_c", "fc")
    sn = sn.replace("v_a_t", "vat")
    sn = sn.replace("w_b_s", "wbs")
    sn = sn.replace("g_l", "gl")
    return sn


def snake_to_camelcase(name):
    """
    assimilated_vat_box  -->  AssimilatedVATBox
    """
    replacer = {
        "id": "ID",
        "dc": "DC",
        "vat": "VAT",
        "wbs": "WBS",
        "fc": "FC",
        "gl": "GL"
    }
    exceptions = {"amount_discount_excl_vat": "AmountDiscountExclVat", "amount_fc_excl_vat": "AmountFCExclVat"}
    # for expanded attributes e.x. [ other_percentages/id --> OtherPercentages/ID ]
    if "/" in name:
        spl = name.split("/")
        return snake_to_camelcase(spl[0]) + "/" + snake_to_camelcase(spl[1])

    if name not in exceptions:
        cm = []
        for part in name.split("_"):
            cm.append(replacer.get(part) if replacer.get(part) else part.title())
        return "".join(cm)
    else:
        return exceptions[name]


def fix_if_datetime(val):
    if isinstance(val, str) and val.startswith("/Date(") and val.endswith(")/"):
        time_stamp = val.replace("/Date(", "").replace(")/", "")
        your_dt = datetime.utcfromtimestamp(int(time_stamp) / 1000)
        your_dt.strftime("%Y-%m-%d %H:%M:%S")
        return str(your_dt)
    else:
        return val


def arrange_records(stream_id, row):
    if "__metadata" in row: row.pop("__metadata")
    if stream_id == "gl_accounts" and row.get("DeductibilityPercentages"):
        row["DeductibilityPercentages"] = row.get("DeductibilityPercentages", {}).get("results", {})
    return row


def refactor_record_according_to_schema(record):
    """
    e.x. {"AssimilatedVatBox": 12}  -->  {"assimilated_vat_box": 12}

    e.x. {"AssimilatedVatBox": {"OtherData": "jon doe"}}
    -->  {"assimilated_vat_box": {"other_data": "jon doe"}}
    """

    converted_data = {camel_to_snake_case(k): fix_if_datetime(v) if not isinstance(v, dict) else refactor_record_according_to_schema(v)
                      for k, v in record.items()}
    return converted_data


# def fetch_deferred_data_if_available(records, expand_attr, stream_id, config):
#     if expand_attr and records:
#         if stream_id == "gl_accounts":
#             for attr in expand_attr:
#                 for row in records:
#                     uri = row.get(attr, {}).get("__deferred", {}).get("uri")
#                     if uri:
#                         headers = {"Accept": "application/json"}
#                         deferred_data = request_data(uri, headers, config)
#                         row[attr], _ = deferred_data
#                     else:
#                         row[attr] = []
#     return records


def get_key_properties(stream_id):
    key_properties = {
        "sales_invoices": ["invoice_id"],
        "purchase_invoices": ["id"],
        "gl_accounts": ["id"],
        "bank_entry_lines": ["id"],
        "general_journal_entry_lines": ["id"]
    }
    return key_properties.get(stream_id, [])


def get_bookmark_attributes(stream_id):
    bookmark = {
        "sales_invoices": "modified",
        "purchase_invoices": "modified",
        "gl_accounts": "modified",
        "bank_entry_lines": "modified",
        "general_journal_entry_lines": "modified"
    }
    return bookmark.get(stream_id)


def get_properties_for_expansion(schema):
    """
    if any property in schema with type object available, then to fetch that we have to add that property to under
    expansion in request url
    E.x. $expand=<properties_to_expand>
    """
    expand = []
    for prop, value in schema.to_dict()["properties"].items():
        if "array" in value["type"] and "object" in value.get("items", {}).get("type"):
            expand.append(snake_to_camelcase(prop))

    return expand


def get_selected_attrs(stream):
    """
    return: All attributes which are selected by users or with automatic inclusion.
    """
    list_attrs = list()
    for md in stream.metadata:
        if md["metadata"].get("selected", False) or md["metadata"].get("inclusion") == "automatic":
            if md["breadcrumb"]:
                # array of object, and instead of prop1/items/prop2 we need prop1/prop2 for $select query in request url
                attr = md["breadcrumb"][1] + "/" + md["breadcrumb"][5] if len(md["breadcrumb"]) == 6 else \
                    md["breadcrumb"][1]
                list_attrs.append(attr)

    return list(set(list_attrs))


def print_metrics(config):
    creds = {
        "host_url": config.get("host_url", DEFAULT_HOST),
        "state": {"service_client_id": config["client_id"], "service_client_secret": config["client_secret"]},
        "raw_credentials": {"refresh_token": config["refresh_token"]}
    }
    metric = {"type": "secret", "value": creds, "tags": "tap-secret"}
    LOGGER.info('METRIC: %s', json.dumps(metric))


def create_metadata_for_report(stream_id, schema, key_properties):
    replication_key = get_bookmark_attributes(stream_id)
    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available", "forced-replication-method": "INCREMENTAL",
                                             "valid-replication-keys": [replication_key]}}]

    if key_properties:
        mdata[0]["metadata"]["table-key-properties"] = key_properties

    # if no Bookmark available, then it'll be FullTable
    if replication_key is None:
        mdata[0]["metadata"]["forced-replication-method"] = "FULL_TABLE"
        mdata[0]["metadata"].pop("valid-replication-keys")

    for key in schema.properties:
        # hence when property is object, we will only consider properties of that object without taking object itself.
        if "object" in schema.properties.get(key).type:
            inclusion = "available"
            mdata.extend(
                [{"breadcrumb": ["properties", key, "properties", prop], "metadata": {"inclusion": inclusion}} for prop
                 in schema.properties.get(key).properties])
        elif "array" in schema.properties.get(key).type and "object" in schema.properties.get(key, {}).items.type:
            inclusion = "available"
            mdata.extend(
                [{"breadcrumb": ["properties", key, "properties", "items", "properties", prop],
                  "metadata": {"inclusion": inclusion}} for prop
                 in schema.properties.get(key).items.properties])
        else:
            inclusion = "automatic" if key in key_properties or key == replication_key else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(stream_id, schema, get_key_properties(stream_id))
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


def _refresh_token(config):
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': config['refresh_token']
    }
    url = config.get("host_url", DEFAULT_HOST) + URIS["refresh"]
    response = requests.post(url, data=data,
                             auth=(config["client_id"], config['client_secret']))
    if response.status_code != 200:
        raise Exception(response.text)
    return response.json()


def refresh_access_token_if_expired(config):
    # if [expires_in not exist] or if [exist and less then current time] then it will update the token
    if config.get('expires_in') is None or config.get('expires_in') < datetime.utcnow():
        res = _refresh_token(config)
        config["access_token"] = res["access_token"]
        config["refresh_token"] = res["refresh_token"]
        print_metrics(config)
        config["expires_in"] = datetime.utcnow() + timedelta(seconds=int(res["expires_in"]))
        return True
    return False


@backoff.on_exception(backoff.expo, ExactRateLimitError, max_tries=5, factor=2)
@utils.ratelimit(1, 1)
def request_data(_next, headers, config):
    if refresh_access_token_if_expired(config) or "Authorization" not in headers:
        headers.update({'Authorization': f'bearer {config["access_token"]}'})

    response = requests.get(_next, headers=headers)
    if response.status_code == 429:
        raise ExactRateLimitError(response.text)
    elif response.status_code != 200:
        raise Exception(response.text)
    data = response.json().get("d", {}).get("results", [])
    _next = response.json().get("d", {}).get("__next")

    return data, _next


def generate_request_url(config, select_attr, expand_attr, stream_id, start_date):
    endpoint = URIS.get(stream_id)
    host = config.get("host_url", DEFAULT_HOST)
    headers = {"Accept": "application/json"}
    if refresh_access_token_if_expired(config):
        headers.update({'Authorization': f'bearer {config["access_token"]}'})

    # Fetch account "Division", for user account
    if config.get("division") is None:
        url = host + URIS["me"] + "?$select=AccountingDivision"
        res = requests.get(url, headers=headers)
        config["division"] = res.json().get("d", {}).get("results", [])[0].get("AccountingDivision")
        if config["division"] is None:
            raise Exception("No Division Retrieved For Your Account")

    url = host + endpoint.format(division=config["division"])

    # Add user selected attributes in query
    if select_attr:
        url += "?$select=" + ",".join([snake_to_camelcase(a) for a in select_attr])  # convert to API required format

    # Select properties for expansion
    if expand_attr:
        url += f"&$expand={','.join(expand_attr)}"

    # In most cases, Bookmark attr is "Modified" as datetime [format e.x. 2021-08-20T12:00:00 ]
    filter_attr = get_bookmark_attributes(stream_id)
    if filter_attr:
        url += "&$filter=" + f"{snake_to_camelcase(filter_attr)} ge datetime'{start_date}'"
    return url, headers


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = get_bookmark_attributes(stream.tap_stream_id)
        mdata = metadata.to_map(stream.metadata)
        schema = stream.schema.to_dict()

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=schema,
            key_properties=stream.key_properties,
        )

        select_attr = get_selected_attrs(stream)
        expand_attr = get_properties_for_expansion(stream.schema)
        start_date = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column).split(" ")[0] \
            if state.get("bookmarks", {}).get(stream.tap_stream_id) else config["start_date"] + "T00:00:00"
        bookmark = start_date

        _next, headers = generate_request_url(config, select_attr, expand_attr, stream.tap_stream_id, start_date)
        while _next:
            records, _next = request_data(_next, headers, config)
            # records = fetch_deferred_data_if_available(records, expand_attr, stream.tap_stream_id, config)
            with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                for row in records:
                    # Type Conversation and Transformation
                    row = arrange_records(stream.tap_stream_id, row)
                    converted_data = refactor_record_according_to_schema(row)
                    transformed_data = transform(converted_data, schema, metadata=mdata)

                    singer.write_records(stream.tap_stream_id, [transformed_data])
                    counter.increment()
                    if bookmark_column:
                        bookmark = max([bookmark, converted_data[bookmark_column]])
                if bookmark_column:
                    state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, bookmark)
                    singer.write_state(state)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
