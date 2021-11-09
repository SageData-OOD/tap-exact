import re
import os
from datetime import datetime

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

    if sn == "h_id":
        sn = "hid"

    return sn


def snake_to_camelcase(name, stream_id):
    """
    assimilated_vat_box  -->  AssimilatedVATBox
    """
    replacer = {
        "hid": "HID",
        "id": "ID",
        "dc": "DC",
        "vat": "VAT",
        "wbs": "WBS",
        "fc": "FC",
        "gl": "GL"
    }
    exceptions = {"amount_discount_excl_vat": "AmountDiscountExclVat", "amount_fc_excl_vat": "AmountFCExclVat"}

    if stream_id == "receivables_list":
        exceptions["account_id"] = "AccountId"


    # for expanded attributes e.x. [ other_percentages/id --> OtherPercentages/ID ]
    if "/" in name:
        spl = name.split("/")
        return snake_to_camelcase(spl[0], stream_id) + "/" + snake_to_camelcase(spl[1], stream_id)

    if name not in exceptions:
        cm = []
        for part in name.split("_"):
            cm.append(replacer.get(part, part.title()))
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
    if isinstance(record, list):
        return [refactor_record_according_to_schema(r) for r in record]
    converted_data = {camel_to_snake_case(k): fix_if_datetime(v) if not isinstance(v, (dict, list)) else refactor_record_according_to_schema(v)
                      for k, v in record.items()}
    return converted_data
