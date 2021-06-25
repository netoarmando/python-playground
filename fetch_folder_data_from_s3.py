#!/bin/env python
# pip install boto3 pytz

from datetime import datetime

import boto3
import pytz

from database import Database

client = boto3.client("s3")
bucket_name = "bucket"
folder_prefix = "items/"

db = Database(path="all_jobs.sqlite3")


def iterate_bucket_items(prefix, bucket=bucket_name, content_key="Contents"):
    paginator = client.get_paginator("list_objects_v2")
    response_iterator = paginator.paginate(
        Bucket=bucket,
        Delimiter="/",
        EncodingType="url",
        Prefix=prefix,
        PaginationConfig={
            "MaxItems": 100,
            "PageSize": 100,
        },
    )
    for page in response_iterator:
        if page["KeyCount"] > 0:
            for item in page[content_key]:
                yield item


def get_folder_data(prefix):
    data = {"items": 0, "size": 0, "last_modified": datetime.now(tz=pytz.utc)}
    for item in iterate_bucket_items(prefix=prefix, content_key="Contents"):
        data["items"] += 1
        data["size"] += item["Size"]
        if data["last_modified"] > item["LastModified"]:
            data["last_modified"] = item["LastModified"]

    return data


if __name__ == "__main__":
    for folder in iterate_bucket_items(prefix=folder_prefix, content_key="CommonPrefixes"):
        prefix = folder["Prefix"]
        folder_data = get_folder_data(prefix)
        folder_data["last_modified"] = folder_data["last_modified"].isoformat()
        print(prefix)
        print(folder_data)
        print("===============")
        db[prefix] = folder_data
