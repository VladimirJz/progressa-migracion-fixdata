from decimal import Decimal
from datetime import date
class Repository():
    class GenericParameter(**kwargs):
        
        @property
        def order(self):
            return self._order
        @order.setter
        def order(self,value):
            self._order=value
        
        @property
        def default(self):
            return self._default
        @default.setter
        def default(self,value):
            self._default=value

        @property
        def required(self):
            return self._required
        @required.setter
        def required(self,value):
            self._required=value
        
    class TextParameter(GenericParameter):
        def __init__(self):
            super().__init__()




class Integracion(Repository):


class saldos-diarios(Integracion):
    Tipo=Repository.TextParameter(order=1,default='G',required=True)
    Instrumento=Repository.IntParameter(order=2,default=0)
    
        




