from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    relationship
)
from sqlalchemy import (
    UniqueConstraint,
    ForeignKey
)

from sqlalchemy.dialects.postgresql import UUID
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

    parent_id: Mapped[str] = mapped_column(nullable=True)

    tags: Mapped[list[str]]

    #author: Mapped["User"] = relationship(back_populates="issues")

    created: Mapped[datetime.datetime]
    updated: Mapped[datetime.datetime]

#rappresenta un singolo CustomField di una issue
class IssueCustomField(Base):
    __tablename__ = 'issueCustomField'

    id:Mapped[int] = mapped_column(primary_key=True)

    name:Mapped[str]

    issue_id: Mapped[str] = mapped_column(ForeignKey('issue.id_readable'))

    value_id:Mapped[UUID] = mapped_column(ForeignKey('field_value.id'))

    __table_args__ = (
        UniqueConstraint("issue_id", "name"),
    )
 
#rappresenta un cambiamento di un IssueCustomField di una Issue
#può essere inteso per esempio come un cambio di assegnatario o di stage
class IssueCustomFieldChange(Base):
    __tablename__= 'issueCustomFieldChange'

    id:Mapped[int] = mapped_column(primary_key=True)

    field_id: Mapped[int] = mapped_column(ForeignKey('issueCustomField.id'))
    
    old_value_id:Mapped[UUID] = mapped_column(ForeignKey('field_value.id'),nullable=True)

    new_value_id:Mapped[UUID] = mapped_column(ForeignKey('field_value.id'),nullable=True)

    timestamp:Mapped[datetime.datetime]

    #author: Mapped["User"] = relationship(back_populates="actions")

    __table_args__ = (
        UniqueConstraint("field_id","timestamp"),
    )


