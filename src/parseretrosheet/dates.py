from datetime import datetime

dates = set()

with open("out.txt", "r") as f:
    for line in f:
        if "on" in line:
            try:
                date_str = line.strip().split("on")[-1].strip()
                date = datetime.strptime(date_str, "%Y/%m/%d").date()
                dates.add(date)
            except Exception as e:
                print(f"Failed to parse line: {line.strip()} → {e}")

# Sort the dates
sorted_dates = sorted(dates)

# Print as list
print([str(d) for d in sorted_dates])
