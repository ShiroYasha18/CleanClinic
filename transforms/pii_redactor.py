from typing import Dict, Any
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine

class PiiRedactor:
    def __init__(self):
        self.analyzer = AnalyzerEngine()
        self.anonymizer = AnonymizerEngine()

    def __call__(self, row: Dict[str, Any]) -> Dict[str, Any]:
        text = row.get("text", "")
        results = self.analyzer.analyze(text=text, language="en")
        row["text"] = self.anonymizer.anonymize(
            text=text, analyzer_results=results
        ).text
        return row