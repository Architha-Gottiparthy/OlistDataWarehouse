# Olist Data Warehouse

## Project Overview
This project builds a **data warehouse pipeline** using the Olist e-commerce dataset.  
The pipeline extracts raw data, performs data cleaning and transformations, and creates **fact and dimension tables** suitable for analytics and reporting.

The goal of this project is to simulate a **real-world data engineering workflow**, including ETL processes and dimensional modeling.

---

## Tech Stack

- Python
- Pandas
- NumPy
- Boto3 (AWS interaction)
- JSON configuration
- Git & GitHub

---

## Project Structure

---

## Data Warehouse Design

The project follows a **star schema** design.

### Fact Tables
- FactOrderItemTable
- FactPaymentTable

### Dimension Tables
- Customer Dimension
- Product Dimension
- Seller Dimension
- Order Dimension

These tables allow analytical queries such as:

- Total revenue
- Order trends
- Customer analysis
- Seller performance

---

## How to Run the Project

### 1️⃣ Clone the repository
git clone https://github.com/Architha-Gottiparthy/OlistDataWarehouse.git


### 2️⃣ Create a virtual environment

Activate it:

Mac/Linux

### 3️⃣ Install dependencies

### 4️⃣ Run the pipeline

---

## Learning Goals

This project demonstrates:

- Building ETL pipelines
- Data cleaning and preprocessing
- Dimensional modeling
- Fact and dimension tables
- Python-based data engineering workflow

---

## Future Improvements

- Automate pipeline with **Apache Airflow**
- Integrate with **AWS S3**
- Load data into **Redshift / Snowflake**
- Add data quality checks
- Build dashboards using **Power BI / Tableau**

---

## Author

Architha Gottiparthy