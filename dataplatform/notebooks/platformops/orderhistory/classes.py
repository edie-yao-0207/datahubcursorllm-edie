from datetime import datetime
from typing import List

import pandas as pd


def datetimeToMs(dt: datetime):
    if dt is None:
        return None
    return int(dt.timestamp() * 1000)


class Address:
    def __init__(
        self,
        address_line_1="",
        address_line_2="",
        address_line_3="",
        city="",
        country="",
        state="",
        zip="",
    ):
        self.address_line_1 = address_line_1
        self.address_line_2 = address_line_2
        self.address_line_3 = address_line_3
        self.city = city
        self.country = country
        self.state = state
        self.zip = zip

    def to_dict(self) -> dict:
        return {
            "address_line_1": self.address_line_1,
            "address_line_2": self.address_line_2,
            "address_line_3": self.address_line_3,
            "locality": self.city,
            "country": self.country,
            "region": self.state,
            "zip_code": self.zip,
        }


class ShippingTrackingLink:
    def __init__(
        self,
        delivery_tracking_number="",
        delivery_carrier="",
    ):
        self.delivery_tracking_number = delivery_tracking_number
        self.delivery_carrier = delivery_carrier

    def to_dict(self) -> dict:
        return {
            "delivery_tracking_number": self.delivery_tracking_number,
            "delivery_carrier": self.delivery_carrier,
        }


class Serial:
    def __init__(self, serial: str):
        self.serial = serial

    def to_dict(self) -> dict:
        serial_dict = {
            "serial": self.serial,
        }

        return serial_dict


class ProductDetail:
    def __init__(
        self, sku="", product_name="", quantity=0, serials: List[Serial] = None
    ):
        self.sku = sku
        self.product_name = product_name
        self.quantity = quantity
        self.serials = serials

    def to_dict(self) -> dict:
        product_detail_dict = {
            "sku": self.sku,
            "product_name": self.product_name,
            "product_serial_details": self.serials,
        }

        if self.quantity is not None and not pd.isna(self.quantity):
            product_detail_dict["quantity"] = int(self.quantity)
        else:
            product_detail_dict["quantity"] = 0

        if self.serials is not None:
            product_detail_dict["product_serial_details"] = [
                serial.to_dict() for serial in self.serials
            ]
        else:
            product_detail_dict["product_serial_details"] = []

        return product_detail_dict


# Represented as an Item Fulfillment
class ShippingGroup:
    def __init__(
        self,
        delivery_status="",
        shipping_address: Address = None,
        shipping_email="",
        shipping_contact="",
        shipping_tracking_links: List[ShippingTrackingLink] = None,
        product_details: List[ProductDetail] = None,
        shipped_at: datetime = None,
    ):
        self.delivery_status = delivery_status
        self.shipping_address = shipping_address
        self.shipping_email = shipping_email
        self.shipping_contact = shipping_contact
        self.shipping_tracking_links = shipping_tracking_links
        self.product_details = product_details
        self.shipped_at = shipped_at

    def to_dict(self) -> dict:
        shipping_group_dict = {
            "status": self.delivery_status,
            "shipping_email": self.shipping_email,
            "shipping_contact": self.shipping_contact,
        }

        if self.shipping_address is not None:
            shipping_group_dict["shipping_address"] = self.shipping_address.to_dict()
        else:
            shipping_group_dict["shipping_address"] = None

        if self.shipping_tracking_links is not None:
            shipping_group_dict["shipping_tracking_links"] = [
                tracking.to_dict() for tracking in self.shipping_tracking_links
            ]
        else:
            shipping_group_dict["shipping_tracking_links"] = []

        if self.product_details is not None:
            shipping_group_dict["product_details"] = [
                product.to_dict() for product in self.product_details
            ]
        else:
            shipping_group_dict["product_details"] = []

        if self.shipped_at is not None:
            shipping_group_dict["shipped_at_ms"] = datetimeToMs(self.shipped_at)

        return shipping_group_dict


# Represented as a Sales Order
class Order:
    def __init__(
        self,
        netsuite_transaction_id=0,
        order_number="",
        sam_number="",
        purchased_by_email="",
        order_status="",
        bill_address: Address = None,
        shipping_groups: List[ShippingGroup] = None,
        created_at: datetime = None,
    ):
        self.netsuite_transaction_id = netsuite_transaction_id
        self.order_number = order_number
        self.sam_number = sam_number
        self.purchased_by_email = purchased_by_email
        self.order_status = order_status
        self.bill_address = bill_address
        self.shipping_groups = shipping_groups
        self.created_at = created_at

    def to_dict(self) -> dict:
        order_dict = {
            "netsuite_transaction_id": int(self.netsuite_transaction_id),
            "order_number": self.order_number,
            "sam_number": self.sam_number,
            "requested_by_email": self.purchased_by_email,
            "order_status": self.order_status,
        }

        if self.bill_address is not None:
            order_dict["bill_address"] = self.bill_address.to_dict()
        else:
            order_dict["bill_address"] = None

        if self.shipping_groups is not None:
            order_dict["shipping_groups"] = [
                group.to_dict() for group in self.shipping_groups
            ]
        else:
            order_dict["shipping_groups"] = []

        if self.created_at is not None:
            order_dict["created_at_ms"] = datetimeToMs(self.created_at)

        return order_dict


class Exchange:
    def __init__(
        self,
        netsuite_transaction_id=0,
        exchange_number="",
        sam_number="",
        requested_by_email="",
        exchange_status="",
        bill_address: Address = None,
        shipping_groups: List[ShippingGroup] = None,
        created_at: datetime = None,
    ):
        self.netsuite_transaction_id = netsuite_transaction_id
        self.exchange_number = exchange_number
        self.sam_number = sam_number
        self.requested_by_email = requested_by_email
        self.exchange_status = exchange_status
        self.bill_address = bill_address
        self.shipping_groups = shipping_groups
        self.created_at = created_at

    def to_dict(self) -> dict:
        exchange_dict = {
            "netsuite_transaction_id": int(self.netsuite_transaction_id),
            "exchange_number": self.exchange_number,
            "sam_number": self.sam_number,
            "requested_by_email": self.requested_by_email,
            "exchange_status": self.exchange_status,
        }

        if self.bill_address is not None:
            exchange_dict["bill_address"] = self.bill_address.to_dict()
        else:
            exchange_dict["bill_address"] = None

        if self.shipping_groups is not None:
            exchange_dict["shipping_groups"] = [
                group.to_dict() for group in self.shipping_groups
            ]
        else:
            exchange_dict["shipping_groups"] = []

        if self.created_at is not None:
            exchange_dict["created_at_ms"] = datetimeToMs(self.created_at)

        return exchange_dict
