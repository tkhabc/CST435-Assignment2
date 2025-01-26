import numpy as np
import datetime

# Set the data size
data_size = 10000

# Start date
start_date = datetime.date(2024, 1, 1)

# Generate data
data = []
for i in range(data_size):
    # Calculate the current date
    current_date = start_date + datetime.timedelta(days=i)
    # Generate temperature with a mean of 20 and standard deviation of 5
    temp = 20 + np.random.normal(0, 5)
    # Append date and temperature to the list
    data.append((current_date, temp))

# Save data to sample_data.txt
with open("sample_data.txt", "w") as f:
    for date, temp in data:
        f.write(f"{date},{temp:.2f}\n")

print(f"Generated {data_size} entries and saved to sample_data.txt.")
