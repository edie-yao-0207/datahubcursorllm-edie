from datetime import datetime


class DeletedRecord:
    def __init__(self, transaction_id: int, sam_number: str, date_deleted: datetime):
        self.transaction_id: int = int(transaction_id)
        self.date_deleted: datetime = date_deleted
        self.sam_number: str = sam_number

    def to_dict(self) -> dict:
        deleted_record = {
            "transaction_id": self.transaction_id,
            "date_deleted": self.date_deleted,
            "sam_number": self.sam_number,
        }
        return deleted_record
