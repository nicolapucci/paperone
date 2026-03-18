from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    relationship
)
from sqlalchemy import (
    ForeignKey,
    UniqueConstraint,
    BOOLEAN
)
import datetime

from models.base import Base

"""
    Prodotto rappresenta il prodotto di riferimento a un test su bugia

    Test rappresenta un test da eseguire
        -id_readable è l'id di riferimento
        -automated indica se il test è automatico quando impostato su si(di default no)

    TestRun rappresenta la singola esecuzione del test associato
        -relazione con test su test_id = Test.id
        -relazione con product su product_id = Product.id
"""

#EVALUATE IF IT'S BETTER TO SPLIT PRODUCT IN PRODUCT & VERSION
class Product(Base):
    __tablename__ = 'product'

    id:Mapped[int] = mapped_column(primary_key=True)
    
    test_runs: Mapped[list["TestRun"]] = relationship(back_populates='release')

    name:Mapped['str']
    version:Mapped['str']

    __table_args__ = (
        UniqueConstraint("name","version"),
    )
    

class Test(Base):
    __tablename__= 'test'

    id: Mapped[int] = mapped_column(primary_key=True)

    #bugia talking_id
    id_readable: Mapped[str] = mapped_column(unique=True,nullable=False)
    
    #EVALUATE POSSIBLE RELATIONSHIP WITH PRODUCT
    
    automated = mapped_column(BOOLEAN,default=False)

    runs: Mapped[list["TestRun"]] = relationship(back_populates='test')

class TestRun(Base):
    __tablename__ = 'testRun'

    id: Mapped[int] = mapped_column(primary_key=True)

    test_id: Mapped[int] = mapped_column(ForeignKey("test.id"),nullable=False)
    test: Mapped["Test"] = relationship(back_populates="runs")

    release_id: Mapped[int] = mapped_column(ForeignKey("product.id"),nullable=True)#RENAME TO PRODUCT IF KEPT
    release: Mapped["Product"] = relationship(back_populates="test_runs")

    rc: Mapped[int]#EVAULATE IF RC IS BETTER SUITED AS A CLASS RELATED TO PRODUCT

    status: Mapped[str]
    outcome: Mapped[str]
    __table_args__ = (
        UniqueConstraint("test_id","release_id","rc"),#TO BE REVIEWED
    )