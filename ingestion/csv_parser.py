import csv
from pathlib import Path

from core.exceptions import InvalidCSVError
from ingestion.models import RawInvoice


class CSVParser:
    """
    Deterministic csv parser with path and file checking.
    Use parse() method to return list of RawInvoice.
    
    Args:
        path: path to csv
    """
    
    def __init__(self, path: Path|str) -> None:
        self._path = Path(path)
        
        if not self._path.is_file():
            raise FileNotFoundError(f"There is no file in provided path: {path}") 
        
        if not self._path.suffix.lower() == ".csv" or not self._is_csv(self._path):
            raise InvalidCSVError(path)
        
    def parse(self) -> list[RawInvoice]:
        results = []
        
        with open(self._path, newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                results.append(RawInvoice(**row))
                
        return results

    @staticmethod    
    def _is_csv(path: Path) -> bool:
        """
        Internal method for checking either file in provided path is csv.
        This method checks file's internal content.
        
        Args:
            path: path to csv file
        """
        try:
            with open(path, newline='', encoding="utf-8") as csvfile:
                sample = csvfile.read(256)
                csvfile.seek(0)
                
                try:
                    dialect = csv.Sniffer().sniff(sample)
                except:
                    dialect = csv.excel # default csv dialect 
                    
                reader = csv.reader(csvfile, dialect)

                for _ in range(5):
                    try:
                        next(reader)
                    except StopIteration:
                        break
            return True
        except Exception:
            return False