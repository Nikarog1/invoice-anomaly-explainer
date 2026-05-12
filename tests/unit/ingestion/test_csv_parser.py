import csv
import json
from pathlib import Path

import pytest 

from ingestion.csv_parser import CSVParser
from core.exceptions import InvalidCSVError



def test_csv_parser_returns_expected_output(tmp_path: Path) -> None:
    data = [
        ["invoice_number", "supplier_name", "buyer_name"],
        ["012345", "Company1", "SuperCompany"],
        ["012346", "Company2", "SuperCompany"],
        ["012347", "Company3", "SuperCompany"],
        ["012348", "Company4", "SuperCompany"],
        ["012349", "Company5", "SuperCompany"],
    ]
    
    path = tmp_path / "data.csv"
    with open(path, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)
        
    parser = CSVParser(path)
    loaded = parser.parse()
    
    loaded_first_row = loaded[0]
    loaded_first_row_dict = loaded_first_row.model_dump()
    
    assert loaded_first_row_dict["invoice_number"] == "012345"
    assert loaded_first_row_dict["supplier_name"] == "Company1"
    assert loaded_first_row_dict["buyer_name"] == "SuperCompany"
    

def test_csv_parser_returns_filenotfound_error(tmp_path: Path) -> None:
    path = tmp_path / "data.csv"
    
    with pytest.raises(FileNotFoundError):
        CSVParser(path)
        

def test_csv_parser_returns_invalidcsv_error(tmp_path: Path) -> None:
    data = {
        "invoice_number": ["123", "234", "345"],
        "supplier_name": ["A", "B", "C"]
    }
    path = tmp_path / "data.json"
    
    with open(path, "w") as f:
        json.dump(data, f)
    
    with pytest.raises(InvalidCSVError):
        CSVParser(path)
    
        