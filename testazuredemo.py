from pyspark.sql import Row

# Create a list of Rows, simulating a CSV file's content
data = [
    Row(EmployeeName="John Doe1", Salary=50000, Age=30),
    Row(EmployeeName="Jane Smith1", Salary=55000, Age=28),
    Row(EmployeeName="Mike Johnson", Salary=60000, Age=35),
    Row(EmployeeName="Emily Davis", Salary=65000, Age=40),
    Row(EmployeeName="Daniel Brown", Salary=70000, Age=45),
    Row(EmployeeName="Jessica Wilson", Salary=75000, Age=50),
    Row(EmployeeName="Larry Moore", Salary=80000, Age=55),
    Row(EmployeeName="Lisa Taylor", Salary=85000, Age=60),
    Row(EmployeeName="Tom Anderson", Salary=90000, Age=65),
    Row(EmployeeName="Megan Clark", Salary=95000, Age=70)
]

# Parallelize the data and create a DataFrame
df = spark.createDataFrame(data)

# Write the DataFrame to a CSV file
df.write.csv("/tmp/employee_data1.csv", header=True)

# Read the CSV file into a DataFrame
df_read = spark.read.csv("/tmp/employee_data1.csv", header=True)

# Display the first 10 records
display(df_read)
