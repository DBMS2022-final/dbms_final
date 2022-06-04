import re
from typing import List


def preprocessing(stmt_origin: str) -> str:
    """preprocess input stmt

    1. replace linebreak with space
    2. remove redundant spaces
    3. to lower case
    """
    stmt_working = stmt_origin.replace('\n', ' ')
    stmt_working = re.sub('\s+', ' ', stmt_working).strip()
    stmt_working = stmt_working.lower()
    return stmt_working


def timestamp_pattern() -> str:
    """MySQL timestamp format

    example: 2022-06-05 21:07:11
    """
    return r"\d+-\d+-\d+\s\d+:\d+:\d+"


def value_pattern() -> str:
    """int or floating point

    floating point is optional
    example:
      15
      3.14
      0.98701
    """
    return "\d+(.\d+)?"


def find_time_condition(stmt: str) -> List[str]:
    condition_pattern = (r"timestamp\s?[<>]=?\s?\'"
                         + timestamp_pattern() + r"\'")
    result = re.findall(condition_pattern, stmt)
    return result


def get_table_name_from_select(stmt: str) -> str:
    return re.search(r"from\s(\w+)", stmt).group(1)


def get_first_time_from_string(stmt: str) -> str:
    return re.search(timestamp_pattern(), stmt).group(1)
