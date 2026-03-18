from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    relationship
)
from sqlalchemy import (
    UniqueConstraint,
    ForeignKey
)
import datetime

from models.base import Base
from models.value import FieldValue

"""
    Issue rappresenta la Issue di YouTrack,

    IssueCustomField rappresenta un customm Field di una Issue 
        -name è il nome del campo
        -value è ottenuto dalla relazione con FieldValue
        -relazione con Issue su issue_id = Issue.id
        -relazione con FieldValue su value_id = FieldValue.id
        
    IssueCustomFieldChange rappresenta un cambiamento di un IssueCustomField 
        -old_value è il valore rimosso dall'IssueCustomField
        -new_value è il valore aggiunto all'IssueCustomField (NON SOSTITUISCE QUELLO PRESENTE)
        -relazione com IssueCustomField su field_id = IssueCustomField.id
        -relazione con FieldValue su old_value_id = FieldValue.id
        -relazione con FieldValue su new_value_id = FieldValue.id
"""





#rappresenta una issue di YouTrack
class Issue(Base):
    __tablename__ = 'issue'

    id:Mapped[int] = mapped_column(primary_key=True)

    youtrack_id: Mapped[str] = mapped_column(unique=True,nullable=False)
    id_readable:Mapped[str] = mapped_column(unique=True,nullable=False)

    summary:Mapped[str]

    custom_fields:Mapped[list["IssueCustomField"]] = relationship(back_populates="issue",cascade="all, delete-orphan")

    parent_id: Mapped[str] = mapped_column(nullable=True)

    #author: Mapped["User"] = relationship(back_populates="issues")

    created: Mapped[datetime.datetime]
    updated: Mapped[datetime.datetime]

#rappresenta un singolo CustomField di una issue
class IssueCustomField(Base):
    __tablename__ = 'issueCustomField'

    id:Mapped[int] = mapped_column(primary_key=True)

    name:Mapped[str]

    issue_id: Mapped[int] = mapped_column(ForeignKey('issue.id'))
    issue:Mapped["Issue"] = relationship(back_populates="custom_fields")

    value_id:Mapped[int] = mapped_column(ForeignKey('field_values.id'))
    value:Mapped["FieldValue"] = relationship(
        single_parent=True,
        cascade="all, delete-orphan"
        )

    changes: Mapped[list["IssueCustomFieldChange"]] = relationship(
        back_populates="field",
        cascade="all, delete-orphan",
        single_parent=True
        )

    __table_args__ = (
        UniqueConstraint("issue_id", "name"),
    )
 
#rappresenta un cambiamento di un IssueCustomField di una Issue
#può essere inteso per esempio come un cambio di assegnatario o di stage
class IssueCustomFieldChange(Base):
    __tablename__= 'issueCustomFieldChange'

    id:Mapped[int] = mapped_column(primary_key=True)

    field_id: Mapped[int] = mapped_column(ForeignKey('issueCustomField.id'))
    field: Mapped["IssueCustomField"] = relationship(back_populates="changes")
    
    old_value_id:Mapped[int] = mapped_column(ForeignKey('field_values.id'))
    old_value:Mapped["FieldValue"] = relationship(
        single_parent=True,
        foreign_keys=[old_value_id],
        cascade="all, delete-orphan"
    )

    new_value_id:Mapped[int] = mapped_column(ForeignKey('field_values.id'))
    new_value:Mapped["FieldValue"] = relationship(
        single_parent=True,
        foreign_keys=[new_value_id],
        cascade="all, delete-orphan"
    )

    timestamp:Mapped[datetime.datetime]

    #author: Mapped["User"] = relationship(back_populates="actions")

    __table_args__ = (
        UniqueConstraint("field_id","timestamp"),
    )


