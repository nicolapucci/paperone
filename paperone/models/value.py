from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.base import Base
import datetime

class Value(Base):
    __tablename__ = 'values'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    
    __mapper_args__ = {
        'polymorphic_identity': 'value',
        'polymorphic_on': type
    }


class StringValue(Value):
    __tablename__ = 'string_values'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[str] = mapped_column(String)
    
    __mapper_args__ = {
        'polymorphic_identity': 'string'
    }


class DateValue(Value):
    __tablename__ = 'date_values'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[datetime.datetime] = mapped_column(DateTime)
    
    __mapper_args__ = {
        'polymorphic_identity': 'date'
    }


class NumberValue(Value):
    __tablename__ = 'number_values'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[int] = mapped_column(Integer)
    
    __mapper_args__ = {
        'polymorphic_identity': 'number'
    }




class FieldValue(Base):
    __tablename__ = 'field_values'

    id: Mapped[int] = mapped_column(primary_key=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
        
    __mapper_args__ = {
        'polymorphic_identity': 'value',
        'polymorphic_on': type
    }


class PrimitiveValue(FieldValue):
    __tablename__ = 'primitive_values'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped["Value"] = relationship("Value", back_populates="field", cascade="all, delete-orphan")
    
    __mapper_args__ = {
        'polymorphic_identity': 'primitive'
    }


class ArrayValue(FieldValue):
    __tablename__ = 'array_values'

    id: Mapped[int] = mapped_column(primary_key=True)

    value: Mapped[list["Value"]] = relationship("Value", back_populates="field", cascade="all, delete-orphan")
    
    __mapper_args__ = {
        'polymorphic_identity': 'list'
    }


Value.field = relationship("FieldValue", back_populates="value", uselist=False)