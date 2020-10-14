# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Pivot registry class."""
import difflib
from functools import partial
from typing import Iterable, Callable, Dict, Union, Any

import attr
import pandas as pd

from ..nbtools import entityschema as entities
from .._version import VERSION

__version__ = VERSION
__author__ = "Ian Hellen"


@attr.s(auto_attribs=True)
class PivotRegistration:
    # The name of the query function
    qry_name: str
    # The query function to call
    qry_func: Callable[..., pd.DataFrame]
    # If the provider is a class that needs instantiating
    prov_class: type = None
    # The output column -> schema map
    schema: Dict[str, str] = attr.Factory(dict)
    # dict of entities and either kwarg name to use for
    # query value or dict of kwargs to supply to qry_func
    qry_entities: Dict[str, Union[str, Dict[str, Any]]] = attr.Factory(dict)

    multi_query_supported: bool = True


def register_provider(prov: PivotRegistration):
    for entity in prov.qry_entities:
        qry_func = prov.qry_func
        if not prov.multi_query_supported:
            qry_func = partial(_iterate_prov_query, qry_func=qry_func)
    tgt_entity = _entity_class(entity)
    set_attr()


def _iterate_prov_query(qry_func, qry_values: Iterable[Any], **kwargs):
    return pd.concat([qry_func(qry_val, **kwargs) for qry_val in qry_values])


def _value_to_df(qry_val):
    if isinstance(qry_val, (str, int, float)):
        qry_val = [qry_val]
    return pd.DataFrame(qry_val, columns="qry_value")


def _entity_class(entity_name: str):
    """Get matching entity class."""
    ent_cls = getattr(entities, entity_name)
    if ent_cls:
        return ent_cls

    legal_entities = dir(entities)
    closest = difflib.get_close_matches(entity_name, legal_entities)
    mssg = f"{entity_name} is not a recognized entity name. "
    if len(closest) == 1:
        mssg += f"Closest match is '{closest[0]}'"
    elif closest:
        match_list = [f"'{mtch}'" for mtch in closest]
        mssg += f"Closest matches are {', '.join(match_list)}"
    else:
        mssg += f"Valid arguments are {', '.join(legal_entities)}"
    raise NameError(entity_name, mssg)


# Notes
# -----
# 1. Create funcs in sources to match standard pattern
# 2. Prob need special interface to queries
# 3. Get TimeRange from last-created QueryTimes
#        def last_instance_of_type(var_type):
#            found = next(
#               iter(var for _, var in globals().items() if isinstance(var, var_type))
#            )
#            if found:
#                return found.last_instance
#
#        last_instance_of_type(TestEntity)
#
#        Or explicity register a time boundary
#
# 4. Create magic to convert list into DF (specify type as param)

# Function Sigs
# =============
# def extract_df(
#     self, data: pd.DataFrame, columns: List[str], **kwargs
# ) -> pd.DataFrame:
#
# def unpack_df(
#     data: pd.DataFrame, column: str, trace: bool = False, utf16: bool = False
# ) -> pd.DataFrame:
#
# def lookup_iocs(
#         self,
#         data: Union[pd.DataFrame, Mapping[str, str], Iterable[str]],
#         obs_col: str = None,
#         ioc_type_col: str = None,
#         ioc_query_type: str = None,
#         providers: List[str] = None,
#         prov_scope: str = "primary",
#         **kwargs,
#     ) -> pd.DataFrame:
# Notes
# -----
# 1. Various interfaces:
# - Single provider, single type
# - All providers, single type
# - All providers, type in input data (or guess type)

# Query Providers
# ---------------
# def query(param1=p1, param2=p2)

# WhoIs
# def get_whois_df(
#     data: pd.DataFrame,
#     ip_column: str,
#     asn_col="AsnDescription",
#     whois_col=None,
#     show_progress: bool = False,
# ) -> pd.DataFrame:

# GeoIP
# def df_lookup_ip(self, data: pd.DataFrame, column: str) -> pd.DataFrame:
#
# Specify default GeoIP provider

# ProcessTree
# def build_process_tree(
#     procs: pd.DataFrame,
#     schema: ProcSchema = None,
#     show_progress: bool = False,
#     debug: bool = False,
# ) -> pd.DataFrame:
#

# AzureData

# AzureSentinel APIs
