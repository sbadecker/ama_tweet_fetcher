import re
from itertools import chain

import pandas as pd

from utils import save_json


def normalize_tokens(text, token_mapping):
    for original_token, token_replacement in token_mapping.items():
        text = re.sub(original_token, token_replacement, text)
    return text


def normalize_mentions(twitter_df, twitter_api_instance, user_name_mapping_path):
    twitter_df["mentions"] = twitter_df.entities.map(get_mentions)
    user_names = set(chain(*twitter_df["mentions"].values))
    user_data = twitter_api_instance.batch_query_users_data_by_name(
        list(user_names))
    user_name_mapping = {user["username"]: user["name"] for user in user_data}
    save_json(user_name_mapping, user_name_mapping_path)
    twitter_df["cleaned_text"] = twitter_df.apply(
        lambda x: substitute_mentions(x, user_name_mapping), axis=1
    )


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
    for mention in row.mentions:
        full_name = user_name_mapping.get(mention)
        if full_name:
            text = re.sub(f"@{mention}", full_name, text)
    return text
