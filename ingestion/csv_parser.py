import csv
import io
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
    @staticmethod    
    def is_csv(source: Path | bytes) -> bool:
        """
        Static method for checking whether provided source is valid CSV.
        This method checks file's internal content.

        Args:
            source: csv content as filesystem path or raw bytes
        """
        try:
            if isinstance(source, bytes):
                stream = io.TextIOWrapper(io.BytesIO(source), encoding="utf-8", newline="")
        
            else:
                stream = open(source, newline='', encoding="utf-8")
                
            with stream:
                sample = stream.read(256)
                
                try:
                    dialect = csv.Sniffer().sniff(sample)
                    
                except csv.Error:
                    dialect = csv.excel # default csv dialect 

                stream.seek(0)
                reader = csv.reader(stream, dialect)
                
                for _ in range(5):
                    try:
                        next(reader)
                    except StopIteration:
                        break
            return True
        
        except (UnicodeDecodeError, csv.Error, OSError):
            return False

        
    def __init__(self, path: Path|str) -> None:
        self._path = Path(path)
        
        if not self._path.is_file():
            raise FileNotFoundError(f"There is no file in provided path: {path}") 
        
        if not self._path.suffix.lower() == ".csv" or not self.is_csv(self._path):
            raise InvalidCSVError(path)
        
    def parse(self) -> list[RawInvoice]:
        results = []
        
        with open(self._path, newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                results.append(RawInvoice(**row))
                
        return results