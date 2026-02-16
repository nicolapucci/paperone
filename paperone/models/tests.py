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

class Product(Base):
    __tablename__ = 'product'

    id:Mapped[int] = mapped_column(primary_key=True)
    
    test_runs: Mapped[list["TestRuns"]] = relationship(back_populates='release')

    name:Mapped['str']
    version:Mapped['str']

class Test(Base):
    __tablename__= 'test'

    id: Mapped[int] = mapped_column(primary_key=True)

    id_readable: Mapped[str] = mapped_column(unique=True,nullable=False)#mantenuto in tutte le versioni

    automated: Boolean

    runs: Mapped[list["TestRun"]] = relationship(back_populates='test')

class TestRun(Base):
    __tablename__ = 'testRun'

    id: Mapped[int] = mapped_column(primary_key=True)

    test_id: Mapped[int] = mapped_column(ForeignKey("test.id"),nullable=False)
    test: Mapped["Test"] = relationship(back_populates="runs")

    release_id: Mapped[int] = mapped_column(ForeignKey("product.id"),nullable=True)
    release: Mapped["Product"] = relationship(back_populates="test_runs")

    rc: Mapped[int]

    outcome: Mapped[str]