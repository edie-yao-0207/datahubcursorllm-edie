# licenses and license returns share the same class
from datetime import datetime


class License:
    def __init__(
        self,
        sam_number="",
        netsuite_transaction_id=0,
        netsuite_transaction_line_id=0,
        transaction_type="",
        status="status",
        created_from_id=0,
        order_number="",
        return_number="",
        sku="",
        quantity=0,
        start_date: datetime = None,
        end_date: datetime = None,
    ):
        self.sam_number = sam_number
        self.netsuite_transaction_id = netsuite_transaction_id
        self.netsuite_transaction_line_id = netsuite_transaction_line_id
        self.transaction_type = transaction_type
        self.status = status
        self.created_from_id = created_from_id
        self.order_number = order_number
        self.return_number = return_number
        self.sku = sku
        self.quantity = quantity
        self.start_date = start_date
        self.end_date = end_date

    def to_dict(self) -> dict:
        license_dict = {
            "sam_number": self.sam_number,
            "netsuite_transaction_id": self.netsuite_transaction_id,
            "netsuite_transaction_line_id": self.netsuite_transaction_line_id,
            "transaction_type": self.transaction_type,
            "status": self.status,
            "created_from_id": self.created_from_id,
            "order_number": self.order_number,
            "return_number": self.return_number,
            "sku": self.sku,
            "quantity": self.quantity,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        return license_dict
