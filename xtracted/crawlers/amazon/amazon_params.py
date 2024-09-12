from pydantic import BaseModel


class AmazonParams(BaseModel):
    with_variants: bool = False
    with_reviews: bool = False
