from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
)
from sqlalchemy import (
    BIGINT,
    String,
    TIMESTAMP,
    ForeignKey,
    JSON
)

from typing import Optional
import datetime

class Base(DeclarativeBase):
    type_annotation_map = {
        datetime.datetime: TIMESTAMP(timezone=True),
        int: BIGINT,
        str: String
    }

class Issue(Base):
    __tablename__ = 'issue'

    id:Mapped[int] = mapped_column(primary_key=True)

    youtrack_id: Mapped[int] = mapped_column(
        BIGINT,
        unique=True,
        nullable=False
    )

    id_readable:Mapped[str] = mapped_column(unique=True)#id readable of the issue on yt

    origin:Mapped[Optional[str]]# optional custom field to define where the issue originates
    type:Mapped[str]#custom field to define the type of the issue (bug,feature...)

    created: Mapped[datetime.datetime]


class ActivityItem(Base):#this class represent a change occurred to a issue field
    __tablename__= 'activityItem'

    id:Mapped[int] = mapped_column(primary_key=True)
    removed:Mapped[Optional[list]]= mapped_column(JSON)#value/s removed
    added:Mapped[Optional[list]]= mapped_column(JSON)#value/s added
    timestamp:Mapped[datetime.datetime]
    
    issue_youtrack_id: Mapped[int] = mapped_column(
        ForeignKey("issue.youtrack_id"),
        nullable=False
    )
    targetMember:Mapped[str]#name of the field that got the cange
