"""
This script provides the template `Enricher `class interface for adding your
custom data enrichment functions and models. Some example enricher classes are
provided below for your reference.
"""
################################################################################


from abc import ABCMeta, abstractmethod
from typing import Generic, Iterable, List, TypeVar
from watchful import attributes


class Enricher(metaclass=ABCMeta):
    """
    This is the interface that customized enricher classes should inherit, and
    then implement the abstract methods.
    """

    @abstractmethod
    def __init__(
        self,
    ) -> None:
        """
        In this function, we create variables that we will later use in
        `enrich_row` to enrich our data row-wise.
        """
        pass

    @abstractmethod
    def enrich_row(
        self,
        row: Iterable[str],
    ) -> List[attributes.EnrichedCell]:
        """
        In this function, we use our variables from `self.enrichment_args` to
        enrich every row of your data. The return value is our enriched row.
        """
        pass

    @classmethod
    def is_enricher(
        cls,
        possibly_an_enricher: Generic[TypeVar('T')]
    ) -> bool:
        """
        Convenience method for checking if `possibly_an_enricher` is indeed of
        type `Enricher`.
        """
        return issubclass(possibly_an_enricher, cls)
