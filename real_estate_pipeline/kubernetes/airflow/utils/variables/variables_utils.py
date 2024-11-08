# from typing import Any, Dict
# from airflow.models import Variable
#
# FACTS_TABLE = 'facts_table'
# DIMENSIONS_TABLE = 'dimensions_table'
#
#
# def get_variables(name, deserialize_json=True, default_var={}, key=None) -> Dict[str, Any]:
#     variables = Variable.get(key=name, deserialize_json=deserialize_json, default_var=default_var)
#     if key is None:
#         return variables
#     else:
#         return variables.get(key, None)
from typing import Any, Dict

from airflow.models import Variable

FACTS_TABLE = "facts_table"
DIMENSIONS_TABLE = "dimensions_table"


def get_variables(
    name,
    deserialize_json=True,
    default_var={},
    key=None,
) -> Dict[str, Any]:
    variables = Variable.get(
        key=name, deserialize_json=deserialize_json, default_var=default_var
    )
    if key is None:
        return variables
    else:
        return variables.get(key, None)
