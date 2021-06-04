import re

import pandas as pd


def get_mentions(entities):
    user_names = set()
    if pd.isna(entities):
        return list(user_names)
    if entities.get("mentions"):
        for mention in entities.get("mentions"):
            user_names.add(mention["username"])
    return list(user_names)


def substitute_mentions(row, user_name_mapping):
    text = row.text
    mentions = get_mentions(row.entities)
    for mention in mentions:
        full_name = user_name_mapping.get(mention)
        if full_name:
            text = re.sub(f"@{mention}", full_name, text)
    return text
