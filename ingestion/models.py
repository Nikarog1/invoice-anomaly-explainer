from pydantic import BaseModel 



class RawInvoice(BaseModel):
    model_config = {"extra": "allow"}