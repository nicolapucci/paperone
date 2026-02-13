from sqlalchemy.org import DeclarativeBase
from sqlalchemy import(
    TIMESTAMP,
    BIGINT,
    String
)

import datetime



class Base(DeclarativeBase):
    type_annotation_map = {
        datetime.datetime: TIMESTAMP(timezone=True),
        int: BIGINT,
        str: String
    }