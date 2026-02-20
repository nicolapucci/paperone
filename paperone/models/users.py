from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    relationship
)
from sqlalchemy import (
    ForeignKey,
    UniqueConstraint
)

from models.base import Base

class User(Base):

    __tablename__ = 'user'

    name:Mapped[str] = mapped_column(unique=True)

    id:Mapped[int] = mapped_column(primary_key=True)
