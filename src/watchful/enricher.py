"""
This script provides the abstract :class:`Enricher` class interface to be
inherited in your custom enricher class, where you can then implement your
custom data enrichment functions and models within :meth:`enrich_fn`. Refer to
https://github.com/Watchfulio/watchful-py/blob/main/examples/enrichment_intro.ipynb
for a tutorial on how to implement your custom enricher class.
"""
################################################################################


from abc import ABCMeta, abstractmethod
from typing import (
    Callable,
    Generic,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
)


EnrichedCell = List[
    Tuple[
        Union[
            List[Tuple[int]],
            Dict[str, List[str]],
            Optional[str],
        ]
    ]
]
ENRICHMENT_ORDERS: List[str] = ["row", "col"]


def set_enrich_fn_order(
    enrich_fn: Callable = None,
    order: Literal["row", "col"] = "row",
) -> Callable:
    """
    This function annotates a data enrichment function with an attribute that
    indicates the order of the data enrichment. Currently, the allowed orders
    are "row" and "col".

    :param enrich_fn: The data enrichment function, defaults to None.
    :type enrich_fn: Callable
    :param order: The data enrichment order; currently the allowed orders are
        "row" and "col", defaults to "row".
    :type order: str
    :return: The list of enriched cell values in the row.
    :rtype: Callable
    """

    assert order in ENRICHMENT_ORDERS, (
        f'The enrichment order "{order}" is unrecognized; use either "row" or'
        f' "col".'
    )

    def __set_enrich_fn_order(enrich_fn):
        enrich_fn.order = order
        return enrich_fn

    if enrich_fn is None:
        return __set_enrich_fn_order
    return __set_enrich_fn_order(enrich_fn)


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
        :meth:`enrich_fn` to enrich our data row by row or column by column.
        This :meth:`__init__` method needs to be implemented in your enricher
        class.
        """

        pass

    @abstractmethod
    @set_enrich_fn_order(
        order="row",
    )  # `order` accepts either "row" or "col"; defaults to "row".
    def enrich_fn(
        self,
        row_or_col: Union[
            Dict[Optional[str], Optional[str]],  # For row-by-row enrichment.
            Tuple[
                Optional[str], Optional[List[str]]
            ],  # For column-by-column enrichment.
        ],
    ) -> List[EnrichedCell]:
        """
        In this method, we use our variables from :attr:`self.enrichment_args`
        initialized in :meth:`__init__` to enrich our data, row by row or column
        by column. The return value is our enriched row or column. This
        :meth:`enrich_fn` method needs to be implemented in your enricher class,
        and annotated with the :func:`attributes.set_enrich_fn_order` to
        indicate enrichment by your choice of either "row" or "col".

        :param row_or_col: If this function enriches rows, a dictionary
            containing string keys as the column names and string values as the
            cell values, one for each cell of the row; the rows are read using
            ``csv.reader`` on a csv file representing the dataset. If this
            function enriches columns, a dictionary containing a string key as
            the column name and a list of string values as the column values.
        :type row_or_col: Union[
                Dict[Optional[str], Optional[str]],
                Tuple[Optional[str], Optional[List[str]]],
            ]
        :return: A list of ``EnrichedCell`` containing the attributes for each
            cell, for the entire row or column.
        :rtype: List[EnrichedCell]
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

    @classmethod
    def is_fully_formed_enricher(
        cls, possibly_an_enricher: Generic[TypeVar("T")]
    ) -> bool:
        """
        This is a convenience method used for checking if
        :class:`possibly_an_enricher` has defined its enrichment order that
        can be one of :attr:`ENRICHMENT_ORDERS`.

        :param possibly_an_enricher: A class that is possibly of the
            :class:`Enricher` class.
        :type possibly_an_enricher: Class
        :return: A boolean indicating if :class:`possibly_an_enricher` has
            defined its enrichment order that can be one of
            :attr:`ENRICHMENT_ORDERS`.
        :rtype: bool
        """

        assert cls.is_enricher(
            possibly_an_enricher
        ), "Your enricher has not implemented the `Enricher` class!"

        try:
            assert getattr(
                possibly_an_enricher, "enrich_fn"
            ), "Your enricher has not defined its `enrich_fn`!"
        except AssertionError:
            return False

        enrich_fn = getattr(possibly_an_enricher, "enrich_fn")
        try:
            assert getattr(
                enrich_fn, "order"
            ), "Your enricher's `enrich_fn` has not defined its `order`!"
        except AssertionError:
            return False
        try:
            assert getattr(enrich_fn, "order") in ENRICHMENT_ORDERS, (
                "Your enricher's `enrich_fn` has defined an invalid `order` "
                f"not in {ENRICHMENT_ORDERS}!"
            )
        except AssertionError:
            return False

        return True
