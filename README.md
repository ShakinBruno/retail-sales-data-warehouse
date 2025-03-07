# Data Warehouse for Retail Sales

## Overview
This project implements a Data Warehouse solution for an online and offline retail business. The goal is to centralize and structure retail sales data for analytics, business intelligence, and decision-making. The DWH integrates e-commerce and physical transactions to provide a comprehensive view of business operations.

## Table of Contents
1. [Datasets Description](#datasets-description)
2. [Logical Scheme](#logical-scheme)
   - [Diagram](#logical-scheme-diagram)
3. [Grain, Dimensions & Facts](#grain-dimensions--facts)
4. [Business Layer - 3NF Model](#business-layer---3nf-model)
   - [Diagram](#3nf-model-diagram)
5. [Business Layer - Dimensional Model](#business-layer---dimensional-model)
   - [Diagram](#dimensional-model-diagram)
6. [Data Flow](#data-flow)
   - [Diagram](#data-flow-diagram)
7. [How to Run the Data Warehouse](#how-to-run-the-data-warehouse)
8. [Conclusion](#conclusion)

## Datasets Description
The project is based on an extended version of the [Online Retail Sales Dataset](https://www.kaggle.com/datasets/arnavsmayan/online-retail-sales-dataset) originally sourced from Kaggle. The dataset has been preprocessed into two separate datasets:

### **1. Online Retail Sales Dataset (`online_retail_sales.csv`)**
Captures e-commerce transactions, including:
- **Customer Information**: name, email, age, gender.
- **Product Information**: name, manufacturer, category, subcategory.
- **Payment Information**: method, card type, verification method.
- **Device Information**: device type, browser used.
- **Sales Information**: timestamp, quantity, price, discount, total amount.

### **2. Offline Retail Sales Dataset (`offline_retail_sales.csv`)**
Captures transactions from physical retail stores, including:
- **Product Information**: name, manufacturer, category, subcategory.
- **Location Information**: store region, country, city, address.
- **Employee Information**: name, email of sales representative involved.
- **Payment Information**: method, card type, verification method.
- **Sales Information**: timestamp, quantity, price, discount, total amount.

### **Preprocessing Steps**
The raw dataset from Kaggle was processed using Python with `pandas` and `faker` libraries to generate these two datasets. Key preprocessing steps include:
- **Data Cleaning**: Handling missing values, correcting inconsistencies.
- **Splitting Data**: Separating online and offline transactions based on relevant attributes.
- **Data Transformation**: Standardizing column names, ensuring proper data types.
- **Feature Engineering**: Generating additional attributes for improved analysis.

The preprocessing script used for this transformation is available in `data_preprocessing.ipynb`.

## Logical Scheme

The architecture in the Data Warehouse Logical Model is divided into multiple layers, each serving a specific role in data transformation and management. This structured approach ensures efficient data processing and accessibility for analytical purposes.

### Layers of the Logical Scheme

- **Data Source Layer**: This layer represents external systems such as operational databases, APIs, flat files, and other data sources. The data from these sources is initially loaded into External Tables for preliminary storage.

- **Data Staging Layer**: Acts as a temporary landing zone where raw data undergoes minimal transformations before integration. It consists of:
  - **Source Tables**: Initial storage after extraction.
  - **Mapping Tables**: Helps standardize and transform data for consistency.
  - **ETL Process**: Extracts, cleanses, and moves data to the next layer.

- **3NF Relational Layer**: This is the core of the Data Warehouse, where data is stored in a normalized format (3rd Normal Form, 3NF) to eliminate redundancy and maintain consistency. It includes:
  - **Core Entity Tables**: Stores integrated, cleansed, and structured data.
  - **ETL Process**: Further transformations ensure referential integrity and historical tracking.

- **Dimensional Layer**: Data is denormalized into a star schema or snowflake schema for analytical processing. This layer includes:
  - **Fact Tables**: Stores business transactions and metrics.
  - **Dimension Tables**: Stores descriptive attributes for analysis.
  - **ETL Process**: Aggregates, indexes, and optimizes data for fast querying.

- **End-User Tools**: This is the front-end interface where users access analytical insights through:
  - **OLAP Tools**: Enables multidimensional analysis and data exploration.
  - **Reporting Tools**: Provides dashboards, KPIs, and structured reports.

### Logical Scheme Diagram

![DWH Logical Model](https://github.com/user-attachments/assets/bd61b2d1-57f0-4c1c-9392-941ffa0432e1)

This logical scheme provides a comprehensive framework for managing data from extraction to end-user analysis, ensuring that data is accurate, consistent, and readily available for decision-making.

## Grain, Dimensions & Facts

### **Grain Definition**
The fact table stores retail sales transactions at the atomic level:
- **Grain:** One row per individual sales transaction identified by `TRANSACTION_ID`.

### **Identified Dimensions**
- **Customer** (Who bought?)
- **Product** (What was bought?)
- **Employee** (Who sold?)
- **Time** (When was it sold?)
- **Location** (Where was it sold?)
- **Payment** (How was it paid?)
- **Device** (For online transactions)

### **Identified Facts**
- **Quantity Sold** (Number of items purchased in the transaction)
- **Unit Price** (Monetary value per item)
- **Discount Amount** (Percentage value of price reduction)
- **Total Amount** (Final transaction value)

### Slowly Changing Dimension Implementation
To effectively manage historical data patterns, a Type 2 Slowly Changing Dimension (SCD) was implemented on the `CE_EMPLOYEES` table. This approach allows for comprehensive temporal tracking and analysis, particularly concerning employee location assignments. The key features of this implementation include:

- **Track Historical Employee Location Assignments**: Enables the analysis of how employee assignments to different locations have changed over time.
- **Enable Temporal Analysis of Employee Performance by Location**: Facilitates the evaluation of employee performance metrics based on their location assignments at different times.
- **Maintain Historical Accuracy in Reporting and Analytics**: Ensures that reports and analytics reflect the true historical context of employee data.
- **Support Time-Based Performance Metrics**: Allows for the calculation of performance metrics that are sensitive to changes over time.

The implementation includes:
- **Effective Dating**: Utilizes `START_DT` and `END_DT` columns to define the validity period of each record.
- **Active Record Flagging**: Uses an `IS_ACTIVE` flag to indicate the current active record for each employee.
- **Historical Record Preservation**: Ensures that all historical records are preserved for accurate temporal analysis.

This approach to temporal data management enhances the data warehouse's ability to provide insightful and accurate historical analyses, particularly in relation to employee performance and location-based metrics.

## Business Layer - 3NF Model

The **3rd Normal Form (3NF) model** ensures data integrity, eliminates redundancy, and improves referential integrity. It structures data into multiple related tables to minimize duplication and dependency inconsistencies. The normalization process follows these steps:

- **Customers Table**: Stores unique customer details, ensuring each customer has a single record with attributes like name, email, age, and gender.
- **Products Table**: Separated into hierarchical structures, distinguishing product categories and subcategories, improving retrieval efficiency.
- **Geographical Hierarchy**: Locations are normalized into separate tables for countries, regions, cities, and store addresses to ensure referential integrity and allow geographical analysis.
- **Employees Table**: Tracks sales representatives and their associated store locations. The employee-location relationship is managed using foreign keys to maintain consistency.
- **Payment Methods Table**: Normalized to prevent redundant payment details. It maintains structured relationships between payment methods, card types, and verification mechanisms.
- **Sales Transactions Table**: Links customers, employees, locations, products, and payments. Each sale transaction references relevant dimension tables to ensure accurate reporting.

### **3NF Model Diagram**

![3NF Model](https://github.com/user-attachments/assets/aa84950a-a9f8-40d3-91e1-6c8e3894b5ca)

## Business Layer - Dimensional Model

The **dimensional model** transforms the 3NF structure into a star schema optimized for analytical queries.

### **Fact Table (`FCT_SALES`)**
Contains:
- Foreign keys to dimensions.
- Measures `QUANTITY`, `PRICE`, `COST` (`QUANTITY` * `PRICE`), `DISCOUNT`, and `TOTAL_AMOUNT`.

### **Dimension Tables**
- `DIM_CUSTOMERS`: Customer details.
- `DIM_PRODUCTS`: Product hierarchy.
- `DIM_EMPLOYEES_SCD`: Sales representatives.
- `DIM_LOCATIONS`: Store location hierarchy.
- `DIM_PAYMENT_DETAILS`: Payment methods and card types.
- `DIM_DATES`: Temporal attributes (day, week, month, year).
- `DIM_TIMES`: Detailed time attributes.
- `DIM_DEVICES`: Device and browser details for online sales.

### **Dimensional Model Diagram**

![DM Model](https://github.com/user-attachments/assets/4d9407e1-2de3-4a47-9d7b-aacb7efa8e02)

## Data Flow

The **Data Flow** represents the end-to-end ETL process, from ingestion to structured storage in the Data Warehouse. The Data Flow Diagram (DFD) illustrates the end-to-end process of data movement within the data warehouse, from source extraction to final storage in fact and dimension tables.

### Layers of Data Flow

1. **Data Source Layer**

   Raw data is ingested from multiple sources, including:

   - **Online Sales Data** from `online_retail_sales.csv` (e-commerce transactions).
   - **Offline Sales Data** from `offline_retail_sales.csv` (physical store transactions).

   These files are loaded into staging tables:

   - `EXT_ONLINE_RETAIL_SALES` and `EXT_OFFLINE_RETAIL_SALES` (foreign tables).
   - `SRC_ONLINE_RETAIL_SALES` and `SRC_OFFLINE_RETAIL_SALES` (source tables).

2. **Cleansing Layer**

   Raw source data is transformed and standardized while ensuring data quality, consistency, and referential integrity. Key processes in this layer include:

   - **Data Standardization**: Cleansing and normalizing values (e.g., converting to uppercase, trimming spaces, handling missing values).
   - **Deduplication & Ranking**: Identifying and retaining the latest records using ranking mechanisms.
   - **Entity Mapping**: Establishing mapping tables (`T_MAP_PRODUCTS`, `T_MAP_PAYMENT_DETAILS`, etc.) to unify and consolidate data from multiple sources.
   - **Slowly Changing Dimensions (SCD)**: Implementing logic to track historical changes in key attributes.
   - **ETL Processing**: Staging transformed data in structured tables for further integration into the 3NF Layer.

3. **Normalized (3NF) Layer**

   Data is structured into core entities such as:

   - `CE_SALES`
   - `CE_CUSTOMERS`
   - `CE_EMPLOYEES_SCD`
   - `CE_PRODUCTS`
   - `CE_PAYMENT_DETAILS`
   - Related lookup tables

   These tables maintain historical accuracy and support referential integrity.

4. **Dimensional Layer (Star Schema)**

   The 3NF data is transformed into denormalized dimensions for optimized analytical querying. Key tables include:

   - **Dimension Tables**: `DIM_CUSTOMERS`, `DIM_PRODUCTS`, `DIM_LOCATIONS`, `DIM_EMPLOYEES_SCD`, `DIM_PAYMENT_DETAILS`, `DIM_DEVICES`, `DIM_DATES`, `DIM_TIMES`
   - **Fact Table**: `FCT_SALES`

This structured approach ensures that data is clean, consistent, and optimized for business intelligence and analytics, providing a comprehensive view of retail operations across both online and offline channels.

### **Data Flow Diagram**

![Data Flow Diagram](https://github.com/user-attachments/assets/3aacbe51-9ee9-42f0-96a8-41ee4ade1d4f)

## How to Run the Data Warehouse

To set up and populate the data warehouse, follow these steps. Ensure you have PostgreSQL installed and properly configured on your system.

### Prerequisites

- PostgreSQL installed with access to the `psql` command-line tool.
- Ensure you have the necessary permissions to create schemas and tables in your PostgreSQL database.

### Steps

1. **Prepare the Datasets**

   Unpack the datasets and move the `datasets` folder into the PostgreSQL `bin` directory. This is necessary because the foreign tables use this directory as a relative path for accessing the CSV files.

2. **Run the Data Warehouse Definition Script**

   Execute the `data_warehouse_definition.sql` script to create the necessary schemas and tables. Use the following command, replacing `<username>` and `<database>` with your PostgreSQL username and database name:

   ```bash
   psql -U <username> -d <database> -f data_warehouse_definition.sql
   ```

3. **Run the Data Warehouse Population Script**

   After defining the data warehouse structure, populate it with data by executing the `data_warehouse_population.sql` script:

   ```bash
   psql -U <username> -d <database> -f data_warehouse_population.sql
   ```

These steps will set up the data warehouse, create the necessary schemas and tables, and populate them with data from the specified datasets. Ensure that all paths and permissions are correctly configured to avoid any issues during the setup process.

## Conclusion
This Data Warehouse provides a robust, scalable solution for analyzing retail sales across online and offline channels. By leveraging structured data storage, dimensional modeling, and optimized query performance, businesses can gain deeper insights into customer behavior, product performance, and overall sales trends.
