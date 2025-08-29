You have access to a large-scale sales transactions dataset containing 5 million rows, available on Kaggle as the "Sample Sales Data (5 million transactions)". The dataset contains detailed sales records including transaction IDs, product details, dates, prices, and quantities.

Processing this dataset on a single machine is slow and inefficient. Your task is to build a distributed system where multiple worker nodes process chunks of the dataset in parallel, communicating via TCP sockets. Analysis results will be stored and managed in an SQL database for aggregation and reporting.

Design and implement a distributed system in Python that performs parallel analysis of the 5 million-row sales dataset using multiple worker nodes communicating with a server via sockets, and leveraging an SQL database for storing partial and aggregated results.

Detailed Requirements:
Dataset:

Use the Sample Sales Data (5 million transactions) Kaggle dataset (or a similar CSV file with millions of rows).

SQL Database:

Set up an SQL database (e.g., SQLite, PostgreSQL, MySQL, MS SQL, etc) to store partial and final aggregated results.

Database schema should include:

Worker identifier

Number of rows processed

Total sales amount (e.g., sum of Price * Quantity)

Min, max, average of Price

Timestamp of result insertion

The server inserts or updates these records as partial results come in.

The server queries and aggregates these results to produce final statistics.

Server (Coordinator):

Loads the entire sales dataset using Pandas (pd.read_csv).

Splits the dataset into equally sized DataFrame chunks.

Sends these chunks to multiple worker nodes over TCP sockets.

Receives computed partial statistics from workers.

Inserts partial results into the SQL database.

Performs final aggregation using SQL queries to produce overall statistics:

Total rows processed.

Total sales amount.

Min, max, average price.

Displays the final aggregated output.

Worker Nodes:

Connect to the server via sockets.

Receive serialized DataFrame chunks.

Deserialize into Pandas DataFrames.

Compute partial metrics on the chunk such as:

Number of transactions (rows)

Total sales amount (Price * Quantity sum)

Min, max, mean of Price

Send these partial results back to the server.

Parallelism and Communication:

Support at least 2 worker nodes running concurrently.

The server handles multiple socket connections concurrently.

Use efficient serialization like pickle or JSON for DataFrame transfer.

Implement error handling and ensure synchronized processing.

