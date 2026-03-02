from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    relationship
)
from sqlalchemy import (
    UniqueConstraint
)
import datetime

from models.base import Base
from models.value import FieldValue


class Issue(Base):
    __tablename__ = 'issue'

    id:Mapped[int] = mapped_column(primary_key=True)

    youtrack_id: Mapped[str] = mapped_column(unique=True,nullable=False)
    id_readable:Mapped[str] = mapped_column(unique=True,nullable=False)

    summary:Mapped[str]

    custom_fields:Mapped[list["IssueCustomField"]] = relationship(back_populates="issue",cascade="all, delete-orphan")

    parent: Mapped["Issue"] = relationship(back_populates="childs")

    childs:Mapped[list["Issue"]] = relationship(back_populates="parent")

    #author: Mapped["User"] = relationship(back_populates="issues")

    created: Mapped[datetime.datetime]
    updated: Mapped[datetime.datetime]


class IssueCustomField(Base):
    __tablename__ = 'issueCustomField'

    id:Mapped[int] = mapped_column(primary_key=True)

    name:Mapped[str]

    issue:Mapped["Issue"] = relationship(back_populates="custom_fields")

    value:Mapped["FieldValue"] = relationship(back_populates="value",cascade="all, delete-orphan")

    changes: Mapped[list["IssueCustomFieldChange"]] = relationship(back_populates="field",cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("issue_id", "name"),
    )


class IssueCustomFieldChange(Base):
    __tablename__= 'issueCustomFieldChange'

    id:Mapped[int] = mapped_column(primary_key=True)

    field: Mapped["IssueCustomField"] = relationship(back_populates="changes")

    old_value:Mapped["FieldValue"] = relationship(back_populates="value",nullable=True)
    new_value:Mapped["FieldValue"] = relationship(back_populates="value",nullable=True)

    timestamp:Mapped[datetime.datetime]

    #author: Mapped["User"] = relationship(back_populates="actions")
    
    __table_args__ = (
        UniqueConstraint("field","timestamp"),
    )


