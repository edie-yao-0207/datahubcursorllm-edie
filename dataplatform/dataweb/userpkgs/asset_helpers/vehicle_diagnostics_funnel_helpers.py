from dataclasses import dataclass

from typing import List


@dataclass
class VehicleClassMapping:
    class_name: str
    cable_names: List[str]

vehicle_class_mappings = [
    VehicleClassMapping(
        class_name="heavy_duty",
        cable_names=[
            "CBL-VG-B1226",
            "CBL-VG-BFMS",
            "CBL-VG-BFMS or ACC-BJ1939-Y1",
            "CBL-VG-BHGV",
            "CBL-VG-BJ1708 with USB (9-pin or 6-pin)",
            "CBL-VG-BJ1939",
            "CBL-VG-BJ1939 or CBL-VG-BIZU",
            "CBL-VG-BJ1939-VM",
            "CBL-VG-CFMS",
            "CBL-VG-CHGV",
            "CBL-VG-CIZU",
            "CBL-VG-CJ1708",
            "CBL-VG-CJ1939",
            "CBL-VG-CJ1939-VM",
            "CBL-VG-DFMS",
            "CBL-VG-DHGV",
            "CBL-VG-DTDC-Y0",
            "CBL-VG-HJ1939-PAC",
            "CBL-VG-CRP1226",
        ],
    ),
    VehicleClassMapping(
        class_name="heavy_equipment",
        cable_names=["CBL-VG-CCT9", "CBL-VG-CCT14", "CBL-VG-CKM12"],
    ),
    VehicleClassMapping(
        class_name="power_only",
        cable_names=[
            "CBL-VG-CCIG Power Only",
            "CBL-VG-CPC Power Only",
            "CBL-VG-CPWR",
            "CBL-VG-CPWR Power Only",
        ],
    ),
    VehicleClassMapping(
        class_name="light_medium_duty",
        cable_names=[
            "CBL-VG-BOBDII",
            "CBL-VG-COBDII-Y0",
            "CBL-VG-COBDII-Y0 MOD + TACHO",
            "CBL-VG-COBDII-Y0S",
            "CBL-VG-COBDII-Y1",
            "CBL-VG-COBDII-Y1S",
            "CBL-VG-COBDII-Y2",
            "CBL-VG-COBDII-Y3",
            "CBL-VG-CTSLA3Y-19",
            "CBL-VG-CTSLAXS-18",
        ],
    ),
    VehicleClassMapping(
        class_name="unknown",
        cable_names=["Cable ID Undetected", "Unknown or Power Only"],
    ),
]
