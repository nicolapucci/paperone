from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    relationship
)
from sqlalchemy import (
    ForeignKey,
    UniqueConstraint
)
import datetime

from models.base import Base



class Issue(Base):
    __tablename__ = 'issue'

    id:Mapped[int] = mapped_column(primary_key=True)

    youtrack_id: Mapped[str] = mapped_column(unique=True,nullable=False)
    id_readable:Mapped[str] = mapped_column(unique=True,nullable=False)

    custom_fields:Mapped[list["IssueCustomField"]] = relationship(back_populates="issue",cascade="all, delete-orphan")

    created: Mapped[datetime.datetime]
    updated: Mapped[datetime.datetime]


class IssueCustomField(Base):
    __tablename__ = 'issueCustomField'

    id:Mapped[int] = mapped_column(primary_key=True)
    issue_id: Mapped[int] = mapped_column(ForeignKey("issue.id"),nullable=False)


    name:Mapped[str]

    issue:Mapped["Issue"] = relationship(back_populates="custom_fields")

    value:Mapped["IssueCustomFieldValue"] = relationship(back_populates="field",cascade="all, delete-orphan")

    changes: Mapped[list["IssueCustomFieldChange"]] = relationship(back_populates="field",cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("issue_id", "name")
    )


class IssueCustomFieldValue(Base):
    __tablename__ = 'issueCustomFieldValue'

    id:Mapped[int] = mapped_column(primary_key=True)
    custom_field_id: Mapped[int] = mapped_column(ForeignKey("issueCustomField.id"),nullable=False)

    type: Mapped[str] = mapped_column()
    
    field: Mapped["IssueCustomField"] = relationship(back_populates="value")
    
    value_string: Mapped[str | None]
    value_number: Mapped[int| None]
    value_date: Mapped[datetime.datetime | None]
    
    __table_args__ = (
        UniqueConstraint("custom_field_id"),
    )
    
    __mapper_args__ = {
        "polymorphic_on":type,
        "polymorphic_identity":'base',
    }

class StringFieldValue(IssueCustomFieldValue):
    __mapper_args__ = {"polymorphic_identity":'string',}

    @property
    def value(self) -> str | None:
        return self.value_string

    @value.setter
    def value(self,v:str):
        self.value_string = v

class NumberFieldValue(IssueCustomFieldValue):
    __mapper_args__ = {"polymorphic_identity":'number',}

    @property
    def value(self) -> int | None:
        return self.value_number

    @value.setter
    def value(self,v:int):
        self.value_number = v

class  DateFieldValue(IssueCustomFieldValue):
    __mapper_args__ = {"polymorphic_identity":'date',}
    
    @property
    def value(self) -> datetime.datetime | None:
        return self.value_date
    
    @value.setter
    def value(self,v:datetime.datetime):
        self.value_date = v


class IssueCustomFieldChange(Base):
    __tablename__= 'issueCustomFieldChange'

    id:Mapped[int] = mapped_column(primary_key=True)

    field_id: Mapped[int] = mapped_column(ForeignKey("issueCustomField.id"),nullable=False)
    field: Mapped["IssueCustomField"] = relationship(back_populates="changes")


    old_value_string:Mapped[str] = mapped_column(nullable=True)
    new_value_string:Mapped[str] = mapped_column(nullable=True)

    old_value_number:Mapped[int]= mapped_column(nullable=True)
    new_value_number:Mapped[int] = mapped_column( nullable=True)

    old_value_date:Mapped[datetime.datetime] = mapped_column(nullable=True)
    new_value_date:Mapped[datetime.datetime] = mapped_column( nullable=True)

    timestamp:Mapped[datetime.datetime]



