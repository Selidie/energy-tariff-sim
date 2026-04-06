from dataclasses import dataclass
from datetime import datetime

@dataclass
class EnergyRecord:
    timestamp: datetime
    import_kwh: float
    export_kwh: float