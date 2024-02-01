from google.cloud import bigtable
import google.cloud.bigtable.row_filters as row_filters
from google.cloud.bigtable.row_set import RowSet
import datetime

project_id = "centered-center-179415"
instance_id = "bt-test-01"
table_id  = "bt-test-tb"

client = bigtable.Client(project=project_id)
instance = client.instance(instance_id)
table = instance.table(table_id)

timestamp = datetime.datetime.utcnow()
column_family_id = "convo"
row_key = "c1#s1#app#c1234"

def print_row(row):
    #print("Reading data for {}:".format(row.row_key.decode("utf-8")))
    for cf, cols in sorted(row.cells.items()):
        print("Column Family {}".format(cf))
        for col, cells in sorted(cols.items()):
            for cell in cells:
                labels = (
                    " [{}]".format(",".join(cell.labels)) if len(cell.labels) else ""
                )
                print(
                    "\t{}: {} @{}{}".format(
                        col.decode("utf-8"),
                        cell.value.decode("utf-8"),
                        cell.timestamp,
                        labels,
                    )
                )
    print("")

def read_row_filter(row_key):
    #col_filter = row_filters.ColumnQualifierRegexFilter(b"question")
    row = table.read_row(row_key)

    print_row(row)

def delete_from_row(row_key):
    row = table.row(row_key)
    row.delete()
    row.commit()
    print("Successfully deleted row {}.".format(row_key))

def write_row(row_key, sequence, question, answer): 
    #row = table.direct_row(row_key)
    row = table.row(row_key)
    row.set_cell(column_family_id, "sequence", sequence, timestamp)
    row.set_cell(column_family_id, "question", question, timestamp)
    row.set_cell(column_family_id, "answer", answer, timestamp)
    row.commit()
    print("Successfully wrote row {}.".format(row_key))

"""
for x in range(2, 10):
    write_row(row_key, x, "question" + str(x), "answer" + str(x))
"""

write_row(row_key, "1", "question1?", "answer1")

question = ""
answer = ""

for x in range(2, 10):
    row = table.read_row(row_key)
    for cf, cols in sorted(row.cells.items()):
        #print("Column Family {}".format(cf))
        for col, cells in sorted(cols.items()):
            for cell in cells:
                #print(col.decode("utf-8"))
                #print(cell.value.decode("utf-8"))
                if col.decode("utf-8") == "question":
                    question = cell.value.decode("utf-8") + "question" + str(x) + "?"
                if col.decode("utf-8") == "answer":
                    answer = cell.value.decode("utf-8") + "answer" + str(x)
    write_row(row_key, x, question, answer)

#delete_from_row(row_key)
read_row_filter(row_key)