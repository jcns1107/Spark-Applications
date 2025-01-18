# Spark Applications

## Project Objective
The objective of this project is to test and apply the Apache Spark library for various data processing tasks, leveraging its distributed computing power to efficiently handle large datasets. The recommendation system is a side product of this implementation, demonstrating Spark's capabilities in machine learning with the ALS (Alternating Least Squares) algorithm to generate item recommendations based on customer preferences.

## Project Application
This project showcases how Spark can be applied to process and analyze large datasets, particularly for customer shopping preferences. The project integrates SQL queries for data analysis and uses Spark's ALS algorithm to build a recommendation system. Although the main goal is to test Spark's functionality, the recommendation system highlights the practical applications of Spark MLlib in real-world scenarios like personalized marketing and customer relationship management.

## Data
The dataset used in this project is the **Customer Shopping Trends Dataset**, which is available on [Kaggle](https://www.kaggle.com/). This dataset, authored by Sourav Banerjee, contains customer shopping behaviors, including items purchased, categories, review ratings, and purchase amounts. The data allows us to explore shopping trends, create user profiles, and generate item recommendations based on past purchase behavior.

The dataset includes the following features:
- **Customer ID**: Unique identifier for each customer.
- **Item Purchased**: The name of the item bought by the customer.
- **Category**: The category to which the item belongs.
- **Purchase Amount (USD)**: The total amount spent on the item.
- **Location**: The location of the customer.
- **Size**: The size of the item purchased (if applicable).
- **Color**: The color of the item purchased.
- **Season**: The season in which the purchase was made.
- **Review Rating**: The rating given by the customer for the item.

The dataset enables a variety of analyses, including customer segmentation, sales trends, and personalized recommendations using Spark MLlib.

## Installing and Running the Project (Spark Setup)
To get the project running, follow the steps below to set up Apache Spark with PostgreSQL:

### Requirements:
- Apache Spark 3.x (Make sure Spark is installed and set up correctly on your machine)
- PostgreSQL 13.x or higher
- Java 8 or higher
- Python 3.x (preferably 3.7+)
- PySpark (Python API for Apache Spark)
- PostgreSQL JDBC Driver (`postgresql-42.x.x.jar`)

### Steps:
1. **Install Apache Spark**:
   - Download Spark from the [official website](https://spark.apache.org/downloads.html).
   - Follow the installation instructions based on your operating system.

2. **Set Up PostgreSQL**:
   - Install PostgreSQL from the [official website](https://www.postgresql.org/download/).
   - Make sure you have a database and a table to store your data.
   - Download the PostgreSQL JDBC Driver (`postgresql-42.x.x.jar`) and place it in the appropriate directory.

3. **Install Python Dependencies**:
   - Use `pip` to install PySpark:
     ```bash
     pip install pyspark
     ```

4. **Configure Spark to Use PostgreSQL**:
   - Ensure that the PostgreSQL JDBC driver is accessible by Spark.
   - Set the `spark.jars` configuration in the Spark session to point to the JDBC driver:
     ```python
     spark = SparkSession.builder \
         .appName("SparkApplication") \
         .config("spark.jars", "path/to/postgresql-42.x.x.jar") \
         .getOrCreate()
     ```

5. **Configure PostgreSQL Database Connection**:
   - Replace the database URL, user, and password with your own details:
     ```python
     jdbc_url = "jdbc:postgresql://<host>:<port>/<database>"
     properties = {
         "user": "<username>",
         "password": "<password>",
         "driver": "org.postgresql.Driver"
     }
     ```

6. **Start the Spark Session**:
   - After configuring Spark and PostgreSQL, initialize the Spark session:
     ```python
     spark = SparkSession.builder \
         .appName("SparkApplication") \
         .config("spark.jars", "path/to/postgresql-42.x.x.jar") \
         .getOrCreate()
     ```

### Running the Project:
Once you've set up Spark, PostgreSQL, and installed the necessary dependencies, you can run the project by executing the Python script. Ensure that the dataset is loaded into the PostgreSQL database, and the appropriate tables and columns are created for storing customer information, purchase history, and item details.

The main flow of the project includes:
- Loading the data from PostgreSQL into Spark.
- Applying SQL queries for analysis (e.g., total spending by age group).
- Applying the ALS algorithm to generate personalized item recommendations for each user.
- Displaying the recommendations with item names instead of IDs.

### Usage:
To use the project, follow these steps:

1. **Run the Python Script**:
   - The Python script will load the customer shopping preferences data from PostgreSQL.
   - It will apply SQL queries to extract relevant insights, such as total spending by age group.
   - The ALS algorithm will generate personalized item recommendations for each user based on their shopping history.

2. **View the Results**:
   - SQL queries will display insights such as average purchase amounts by age group.
   - The ALS model will generate top 10 item recommendations for each user and display them with the corresponding item names.

3. **Adjust Parameters**:
   - You can modify the parameters of the ALS algorithm, such as the number of latent factors (`rank`), the regularization parameter (`regParam`), and the maximum number of iterations (`maxIter`), to improve the quality of recommendations.

4. **PostgreSQL Integration**:
   - The results of the ALS recommendations can be saved back into the PostgreSQL database or used for further analysis.

## How to Use the Project:
1. Clone the repository to your local machine.
2. Install dependencies as described above.
3. Configure PostgreSQL database connection and ensure the JDBC driver is in place.
4. Execute the Python script to start the recommendation system and explore the data using SQL.

## Contributors:
- **jcns1107** - Project creator and developer

