# DeclarativeBase è una classe sql alchemy che serve per dichiarare i Modelli
from sqlalchemy.orm import DeclarativeBase 
from sqlalchemy import(                    
    TIMESTAMP,                             
    BIGINT,                                
    String,
)

import datetime

# I Modelli trovati in /models estenderanno Base
# qui posso definire eventuali attributi/comportamenti comuni a tutti i Modelli
class Base(DeclarativeBase):
    __allow_unmapped__ = True 
    type_annotation_map = {
        datetime.datetime: TIMESTAMP(timezone=True),
        int: BIGINT,
        str: String
    }
