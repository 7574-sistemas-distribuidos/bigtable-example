#!/usr/bin/env python

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Demonstrates how to connect to Cloud Bigtable and run some basic operations.

Prerequisites:

- Create a Cloud Bigtable cluster.
  https://cloud.google.com/bigtable/docs/creating-cluster
- Set your Google Application Default Credentials.
  https://developers.google.com/identity/protocols/application-default-credentials
"""

import argparse

# [START bigtable_hw_imports]
import datetime

from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters

# [END bigtable_hw_imports]

MUTATION_BATCHSIZE = 500

def _write_words(table, words_filepath, attrs_col_family):
    print("Writing words....")
    rows = []
    prev_word = ''
    with open(words_filepath) as f:
        for line in f:
            word = line.rstrip()
            att_letters = str(len(word)).encode()
            shared_with_prev = [i for i,j in zip(list(word), list(prev_word)) if i == j]
            att_shared_root_with_prev = ''.join(shared_with_prev)
            tstamp = datetime.datetime.utcnow()

            prev_word = word

            row_key = word.encode()
            row = table.direct_row(row_key)
            row.set_cell(attrs_col_family, 'att_letters'.encode(), att_letters, timestamp=tstamp)
            row.set_cell(attrs_col_family, 'att_shared_root_with_prev'.encode(), att_shared_root_with_prev, timestamp=tstamp)
            rows.append(row)

            if len(rows) >= MUTATION_BATCHSIZE:
                print("Writing mutations batch...")
                table.mutate_rows(rows)
                rows = []

    if len(rows) > 0:
        print("Writing mutations batch...")
        table.mutate_rows(rows)

def _print_row(row, column_family_id):
    columns = row.cells[column_family_id].keys()
    values = []
    for column in columns:
        cell = row.cells[column_family_id][column][0]
        if isinstance(cell.value, int):
            value = cell.value
        else:
            value = cell.value.decode('utf-8')
        values.append(f'{column}:{cell.value.decode()}')
    values_str = ' - '.join(values)
    print(f'Row: {row.row_key} - {values_str}')

def main(project_id, instance_id, table_id, words_filepath):
    # The client must be created with admin=True because it will create a
    # table.
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)

    print("Creating the {} table.".format(table_id))
    table = instance.table(table_id)

    print("Creating column family with Max Version GC rule...")
    # Create a column family with GC policy : most recent N versions
    # Define the GC policy to retain only the most recent 2 versions
    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_family_id = "word_attributes"
    column_families = {column_family_id: max_versions_rule}
    if not table.exists():
        table.create(column_families=column_families)
    else:
        print("Table {} already exists.".format(table_id))

    _write_words(table, words_filepath, column_family_id)

    # Create a filter to only retrieve the most recent version of the cell
    # for each column accross entire row.
    row_filter = row_filters.CellsColumnLimitFilter(1)

    print("Getting a single greeting by row key.")
    key = "internet".encode()

    row = table.read_row(key, row_filter)
    if row:
        _print_row(row, column_family_id)

    print("Scanning for all greetings:")
    partial_rows = table.read_rows(start_key='inte'.encode(), end_key='internet'.encode(), filter_=row_filter)

    for row in partial_rows:
        _print_row(row, column_family_id)

    print("Deleting the {} table.".format(table_id))
    table.delete()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("project_id", help="Your Cloud Platform project ID.")
    parser.add_argument("instance_id", help="ID of the Cloud Bigtable instance to connect to.")
    parser.add_argument("words_file", help="Words filepath to upload.")
    parser.add_argument(
        "--table", help="Table to create and destroy.", default="words"
    )

    args = parser.parse_args()
    main(args.project_id, args.instance_id, args.table, args.words_file)
