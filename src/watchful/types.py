import enum


# XXX: rockstar (22 May 2023) - This enum could
# be changed to a StrEnum and can use enum.auto()
# once we only support python 3.11+
class ClassificationType(enum.Enum):
    """The supported text classifaction types in Watchful."""

    FTC = "ftc"
    NER = "ner"
