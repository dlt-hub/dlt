import dlt

from dlt import annotations as a
from dlt.common import json
from typing_extensions import Annotated, Never, Optional


class Items:

    # primary keys
    id: Annotated[str, a.PrimaryKey, a.Unique]

    # additional columns
    name: Annotated[Optional[str], a.Classifiers(["pii.name"])]
    email: Annotated[Optional[str], a.Unique, a.Classifiers(["pii.email"])]
    likes_herring: Annotated[bool, a.Classifiers(["pii.food_preference"])]


AnnotatedItems = Annotated[Items, a.TableName("my_items"), a.WriteDisposition("merge")]

if __name__ == "__main__":

    # print result of class_to_table
    print(json.dumps(a.class_to_table(AnnotatedItems), pretty=True))

    p = dlt.pipeline("my_pipe", destination="duckdb", full_refresh=True)

    data = [{
        "id": "my_id"
    }]

    # run simple pipeline and see wether schema was used
    load_info = p.run(data, columns=AnnotatedItems, table_name="blah")
    print(load_info)
    print(p.default_schema.to_pretty_yaml())

