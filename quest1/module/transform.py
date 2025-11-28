from typing import List
import re

def extract_date(file:str) -> str:
    date = re.search(r'\d{4}-\d{2}-\d{2}', file)
    print("Date extracted successfully!")
    return date.group() if date else None
    

def get_rows(file:str) -> List[List[str]]:
    
    rows = []
    for block in file.split("\n\n\nAGENTE: "):
        agente = block.split("\n")[0]

        for line in block.split("\n")[1:]:
            try:
                if line.split(",")[1] == " D":
                    complete_line = [agente + " ," + line]
                    rows.append(complete_line[0].split(","))
            except IndexError:
                print(f"WARNING! line being excepted:'{line}'")
    print("Rows obtained successfully!")
    return rows

