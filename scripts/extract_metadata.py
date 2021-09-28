import sys
import re
import csv

if __name__ == "__main__":
    PATH = "C:\Users\DELL\Documents\dataeng\airflow\data\extracted_text.txt"
    pattern = "(\d:\d\d)-(\d:\d\d) (.*\n)"

    with open(PATH+'metadata.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["start_hour","end_hour","activity"])
        
        txt = open(PATH+"extracted_text.txt", encoding="UTF-8").read()
        extracted_data = re.findall(pattern,txt)
        for data in extracted_data:
            moment = [data[0].strip(),data[1].strip(),data[2].strip()]
            writer.writerow(moment)
