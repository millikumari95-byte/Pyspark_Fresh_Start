# PySpark Window Functions & StructType Deep Dive

This tutorial covers advanced analytical functions in PySpark, specifically focusing on Window functions for row-level comparisons and `StructType` for programmatic schema definition.

## Core Concepts

### 1. Window Functions (`Window`)
Unlike `groupBy` aggregations, Window functions allow you to perform calculations across a set of rows while **retaining each row's original identity**.

- **`partitionBy()`**: Defines how to group rows (e.g., by Department).
- **`orderBy()`**: Defines the sorting within those groups (e.g., by Salary descending).
- **`dense_rank()`**: A ranking function that assigns the same rank to tied values without skipping any subsequent ranks (1, 2, 2, 3...).

### 2. Finding the Nth Highest Value
By combining `partitionBy`, `orderBy`, and a ranking function, you can solve common analytical problems like:
- Finding the 2nd highest salary per department.
- Identifying the most recent transaction for each user.
- Calculating running totals within a group.

### 3. Programmatic Schema Definition (`StructType`)
In production environments, it is best practice to define your DataFrame's schema explicitly rather than relying on Spark's type inference.
- **`StructType`**: Represents the entire schema (a list of fields).
- **`StructField`**: Represents a single column, defining its name, type (e.g., `StringType`, `IntegerType`), and whether it can contain null values.

### 4. Nested Data Structures (`struct`)
PySpark allows you to wrap multiple related columns into a single "struct" (object) column. This is useful for:
- Grouping related metadata (e.g., grouping `age` and `gender` into a `demographics` column).
- Preparing data for JSON exports or complex API responses.

## Practical Scenarios Covered
1. **Salary Ranking**: Using `dense_rank` to identify the 2nd highest salary within different departments.
2. **Manual Schema Creation**: Defining a robust schema using `StructType` and `StructField`.
3. **Column Nesting**: Using the `struct()` function to create hierarchical data structures.

## How to Run
Run the Python script directly. Ensure your environment variables for Spark (`PYSPARK_PYTHON`, `JAVA_HOME`) are correctly set.
