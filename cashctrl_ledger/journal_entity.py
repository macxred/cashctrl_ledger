"""Provides a class with Journal accessors and mutators for CashCtrl."""

import pandas as pd
from typing import Callable
from pyledger import JournalEntity
from .cashctrl_accounting_entity import CashCtrlAccountingEntity


class Journal(JournalEntity, CashCtrlAccountingEntity):
    """
    Provides journal accessors and mutators for CashCtrl.

    These methods are passed as parameters during initialization and defined as callables.
    They are implemented in this class by calling the provided callables, thus satisfying
    the abstract method requirements of the base class.
    """

    def __init__(
        self,
        list: Callable[[], pd.DataFrame],
        add: Callable[[pd.DataFrame], None],
        modify: Callable[[pd.DataFrame], None],
        delete: Callable[[pd.DataFrame, bool], None],
        standardize: Callable[[pd.DataFrame], pd.DataFrame],
        *args, **kwargs
    ):
        """
        Initializes the journal class.

        Args:
            list (Callable[[], pd.DataFrame]):
                A callable that lists journal entries and returns a DataFrame.
            add (Callable[[pd.DataFrame], None]):
                A callable that adds journal entries from a DataFrame.
            modify (Callable[[pd.DataFrame], None]):
                A callable that modifies journal entries based on a DataFrame.
            delete (Callable[[pd.DataFrame, bool], None]):
                A callable that deletes journal entries specified in a DataFrame.
             *args, **kwargs: Additional arguments passed to the superclass.
        """
        super().__init__(*args, **kwargs)
        self._list = list
        self._add = add
        self._modify = modify
        self._delete = delete
        self._standardize = standardize

    def list(self) -> pd.DataFrame:
        return self._list()

    def add(self, data: pd.DataFrame) -> None:
        return self._add(data)

    def modify(self, data: pd.DataFrame) -> None:
        self._modify(data)

    def delete(self, id: pd.DataFrame, allow_missing: bool = False) -> None:
        self._delete(id, allow_missing)

    def standardize(self, data, keep_extra_columns=False):
        data = super().standardize(data, keep_extra_columns)
        return self._standardize(data)
