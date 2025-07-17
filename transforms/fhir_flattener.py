import json
from typing import Dict, Any, List

class FhirFlattener:
    def __call__(self, row: Dict[str, Any]) -> Dict[str, Any]:
        raw = row.get("fhir_json", "{}")
        bundle = json.loads(raw)
        flat = {}
        for entry in bundle.get("entry", []):
            res = entry.get("resource", {})
            if res.get("resourceType") == "Patient":
                flat["patient_id"] = res.get("id")
                flat["birth_date"] = res.get("birthDate")
                flat["gender"] = res.get("gender")
        return {**row, **flat}