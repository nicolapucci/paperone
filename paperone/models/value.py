from sqlalchemy import (
    Column,
    Integer, 
    String, 
    DateTime, 
    Interval,
    ForeignKey,
    Column
    )
from sqlalchemy.orm import (
    Mapped, 
    mapped_column, 
    relationship
    )

from sqlalchemy.dialects.postgresql import UUID
from models.base import Base

import uuid
import datetime

"""
    Value rappresenta il valore che deve assumere un CustomField di una issue
        classi che estendono Value:
            StringValue esiste perchè il valore di CustomField sia di tipo stringa
            DataValue esiste perchè  il valore di CustomField sia di tipo data
            NumberValue esiste perchè il valore di CustomField sia di tipo numerico
"""

class Value(Base):
    __tablename__ = 'value'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    type: Mapped[str] = mapped_column(String, nullable=False)

    field_id:Mapped[UUID] = mapped_column(ForeignKey('field_value.id'))
    __mapper_args__ = {
        'polymorphic_identity': 'value',
        'polymorphic_on': type
    }

class StringValue(Value):
    __tablename__ = 'string_value'
    
    id: Mapped[int] = mapped_column(ForeignKey('value.id'),primary_key=True)
    value: Mapped[str] = mapped_column(String)

    field_id:Mapped[UUID] = mapped_column(ForeignKey('field_value.id'))
    
    __mapper_args__ = {
        'polymorphic_identity': 'string'
    }

class DateValue(Value):
    __tablename__ = 'date_values'
    
    id: Mapped[int] = mapped_column(ForeignKey('value.id'),primary_key=True)
    value: Mapped[datetime.datetime] = mapped_column(DateTime)

    field_id:Mapped[UUID] = mapped_column(ForeignKey('field_value.id'))
    
    __mapper_args__ = {
        'polymorphic_identity': 'date'
    }

class TimeValue(Value):
    __tablename__ = 'time_value'

    id: Mapped[int] = mapped_column(ForeignKey('value.id'),primary_key=True)
    value: Mapped[datetime.timedelta] = mapped_column(Interval)

    field_id:Mapped[UUID] = mapped_column(ForeignKey('field_value.id'))
    
    __mapper_args__ = {
        'polymorphic_identity': 'time'
    }

class NumberValue(Value):
    __tablename__ = 'number_value'
    
    id: Mapped[int] = mapped_column(ForeignKey('value.id'),primary_key=True)
    value: Mapped[int] = mapped_column(Integer)

    field_id:Mapped[UUID] = mapped_column(ForeignKey('field_value.id'))
    
    __mapper_args__ = {
        'polymorphic_identity': 'number'
    }


class FieldValue(Base):

    __tablename__ = 'field_value'

    id:Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
