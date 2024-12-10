import pandas as pd
from concurrent.futures import ProcessPoolExecutor

# Load the CSV files
learner_data = pd.read_csv('F:\\Users\\Ghzan\'s-data\\Download\\pdc assignment\\students.csv')
transaction_data = pd.read_csv('E:\\Users\\Ghzan\'s-data\\Download\\pdc assignment\\fees.csv')

# Ensure learner_id columns are integers and trim any whitespace
learner_data["learner_id"] = learner_data["learner_id"].astype(str).str.strip().astype(int)
transaction_data["learner_id"] = transaction_data["learner_id"].astype(str).str.strip().astype(int)

# Debugging: Print unique learner IDs for verification
print("Unique Learner IDs in learner_data:", learner_data["learner_id"].unique())
print("Unique Learner IDs in transaction_data:", transaction_data["learner_id"].unique())

# Preprocess transaction data to find the most relevant transaction date for each learner
def find_primary_date(group):
    date_occurrences = group["transaction_date"].value_counts()
    if all(date_occurrences == 1):  # If all dates are unique
        return group["transaction_date"].max()  # Pick the latest date
    else:
        return date_occurrences.idxmax()  # Pick the most frequent date

# Create a mapping of learner_id to the most relevant transaction date
primary_dates = transaction_data.groupby("learner_id").apply(find_primary_date).reset_index()
primary_dates.columns = ["learner_id", "key_date"]

# Parallelized function to process each learner's record
def process_learner(record):
    learner_id = record["learner_id"]

    if pd.notna(learner_id):  # Ensure the learner ID is valid
        # Check if the learner ID exists in the precomputed primary dates
        key_date_row = primary_dates[primary_dates["learner_id"] == learner_id]

        if not key_date_row.empty:
            key_date = key_date_row["key_date"].iloc[0]
            return f"Learner ID {learner_id}: Key transaction date: {key_date}"
        else:
            return f"Learner ID {learner_id}: No transaction records found."
    else:
        return f"Invalid Learner ID: {learner_id}"

# Execute processing in parallel
if __name__ == "__main__":
    # Convert learner data to list of dictionaries (rows) for parallel processing
    learner_records = learner_data.to_dict("records")

    # Use ProcessPoolExecutor for multiprocessing
    with ProcessPoolExecutor() as executor:
        result_output = list(executor.map(process_learner, learner_records))

    # Print results
    for result in result_output:
        print(result)
