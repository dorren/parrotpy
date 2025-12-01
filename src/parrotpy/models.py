from abc import ABC, abstractmethod
from pyspark.sql import Column, DataFrame
from typing import Any

from .utils import Snapshot

class ColumnValue:
    """ value in a dataframe column"""
    pass

class ColumnSpec:
    def __init__(self, name: str, data_type: str, col_val: ColumnValue):
        self.name = name
        self.data_type = data_type
        self.value = col_val

    def __str__(self):
        cls_name = self.__class__.__name__
        return f"{cls_name}({self.__dict__})"
    
    def to_dict(self):
        val_attrs = self.value.to_dict() if hasattr(self.value, "to_dict") else str(self.value)

        result = {
            "name": self.name,
            "data_type": self.data_type,
            "value": val_attrs
        }
        return result
    
    def to_context(self) -> Any:
        """ convert to function context """
        return {
            "column_name":  self.name,
            "column_type":  self.data_type,
            "column_value": self.value
        }

class DfSpec:
    def __init__(self):
        self.columns = []
        self._options = {}
    
    def options(self, **kwargs):
        allowed = ["name"]
        filtered_dict = {key: kwargs[key] for key in kwargs if key in allowed}

        self._options = {**self._options, **filtered_dict}

    def add_column(self, col_spec: ColumnSpec):
        self.columns.append(col_spec)
        return self
    
    def __str__(self):
        result = f"{self.__class__}({self._options})"
        for c in self.columns:
            result += f"\n  {c}"

        return result

    def to_dict(self):
        result = {}
        result["columns"] = [col.to_dict() for col in self.columns]

        return result