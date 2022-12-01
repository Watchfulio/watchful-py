"""
This script provides the abstract :class:`Enricher` class interface to be
inherited in your custom enricher class, where you can then implement your
custom data enrichment functions and models within :meth:`enrich_row`. Refer to
https://github.com/Watchfulio/watchful-py/blob/main/examples/enrichment_intro.ipynb
for a tutorial on how to implement your custom enricher class.
"""
################################################################################


from abc import ABCMeta, abstractmethod
from typing import Generic, Dict, List, Optional, TypeVar
from watchful import attributes


class Enricher(metaclass=ABCMeta):
    """
    This is the abstract class that customized enricher classes should inherit,
    and then implement the abstract methods :meth:`__init__` and
    :meth:`enrich_row`.
    """

    @abstractmethod
    def __init__(self) -> None:
        """
        In this method, we create variables that we will store in
        :attr:`self.enrichment_args`. We then later use them in
        :meth:`enrich_row` to enrich our data row by row. This :meth:`__init__`
        method needs to be implemented in your enricher class.
        """

        pass

    @abstractmethod
    def enrich_row(
        self,
        row: Dict[Optional[str], Optional[str]],
    ) -> List[attributes.EnrichedCell]:
        """
        In this method, we use our variables from :attr:`self.enrichment_args`
        initialized in :meth:`__init__` to enrich our data, row by row. The
        return value is our enriched row. This :meth:`enrich_row` method needs
        to be implemented in your enricher class.

        :param row: A dictionary containing string keys as the column names and
            string values as the cell values, one for each cell of the row; the
            rows are read using ``csv.reader`` on a csv file representing the
            dataset.
        :type row: Dict[Optional[str], Optional[str]]
        :return: A list of ``attributes.EnrichedCell`` containing the attributes
            for each cell, for the entire row.
        :rtype: List[attributes.EnrichedCell]
        """

        pass

    @classmethod
    def is_enricher(cls, possibly_an_enricher: Generic[TypeVar("T")]) -> bool:
        """
        This is a convenience method used for checking if
        :class:`possibly_an_enricher` is indeed of the :class:`Enricher` class.

        :param possibly_an_enricher: A class that is possibly of the
            :class:`Enricher` class.
        :type possibly_an_enricher: Class
        :return: A boolean indicating if :class:`possibly_an_enricher` is
            indeed of the :class:`Enricher` class.
        :rtype: bool
        """

        return issubclass(possibly_an_enricher, cls)
