# PySpark Aggregation and Complex Data Types

This tutorial covers the fundamental and advanced aggregation techniques in PySpark, focusing on how to transform raw data into meaningful summaries using both standard functions and complex collection types.

## Core Concepts

### 1. Grouping Data (`groupBy`)
The `groupBy` operation is the foundation of data aggregation. It collects rows that have the same values in specified columns into "buckets," allowing you to perform calculations on each bucket independently.

### 2. Standard Aggregation Functions
These functions return a single value for each group:
- **`sum()`**: Calculates the total numeric sum of a column within a group.
- **`count()`**: Returns the number of items (rows) in each group.

### 3. Complex Collection Types (Arrays)
PySpark allows you to gather multiple values from a group into a single row using collection functions. This results in an **ArrayType** column.
- **`collect_list()`**: Gathers all values from a group into a list. It **preserves duplicates** and maintains the order in which data was processed.
- **`collect_set()`**: Gathers values into a set. It **removes duplicates**, ensuring only unique values are stored for that group.

### 4. Conditional Logic with `expr()`
The `expr()` function allows you to use SQL-like syntax within your DataFrame operations. 
- **`CASE WHEN`**: Used for categorical binning (e.g., turning ages into age groups like '19-35'). This is essential for feature engineering and segmenting reports.

### 5. Data Deduplication (`dropDuplicates`)
Before aggregating, it is often necessary to clean the data. `dropDuplicates()` ensures that the input to your aggregation functions contains only unique rows based on either the entire row or specific columns, preventing skewed results in functions like `count()`.

## Practical Scenarios Covered
1. **Transaction Summary**: Calculating total spend and transaction counts per user.
2. **Demographic Segmentation**: Binning users into age groups and counting the distribution.
3. **Daily Inventory/Sales**: Collecting a unique list of products sold each day using `collect_set`.

## How to Run
Ensure you have PySpark installed and your `JAVA_HOME` correctly configured as shown in the environment setup section of the script.
