# Data Cleaning

## Executive Summary

Before performing any analysis, the raw e-commerce dataset was cleaned and validated to ensure data quality and reliability.

The cleaning process included removing duplicates, handling missing values, validating business logic rules, and ensuring consistency across related tables.

Clean data is essential to ensure that business insights and analytical results are accurate and trustworthy.

---

## Business Problem

Raw datasets often contain issues such as:

- duplicate records
- inconsistent formats
- missing values
- incorrect relationships between tables
- invalid values 

If these issues are not addressed, analytical results may be misleading and lead to poor business decisions.

The goal of this step was to prepare the dataset for reliable analysis.

---

## Methodology

Data cleaning was performed in **MySQL** using staging tables to safely transform the original dataset.

Key steps included:

### 1️⃣ Duplicate Detection

Duplicate records were identified using:

- primary key checks
- business key checks
- window functions


---

### 2️⃣ Data Type Standardization

Columns were converted to appropriate formats such as:

- DATETIME for timestamps
- DECIMAL for monetary values
- VARCHAR for categorical variables

---

### 3️⃣ Missing Value Validation

All tables were checked for null values in critical columns such as:

- user_id
- product_id
- order_date
- price
- quantity

---

### 4️⃣ Cross-Table Validation

Foreign key relationships were validated to detect **orphan records**.

Examples:

- events without a valid user
- orders without a valid product

Invalid records were isolated and removed from the main dataset.

---

### 5️⃣ Business Logic Validation

Logical checks were performed to detect unrealistic values, including:

- negative prices
- negative quantities
- orders placed before user signup date
- future timestamps

---

## Skills

This stage demonstrates:

- MySQL data validation
- window functions
- data quality checks
- cross-table validation
- data type transformations

---

## Results & Business Recommendations

After cleaning, the dataset became consistent and analysis-ready.

Key improvements:

- orphan records removed
- incorrect timestamps corrected
- invalid order statuses standardized
- consistent numeric formats applied

This ensured that all subsequent analyses were based on reliable data.

---

## Next Steps

With the dataset cleaned, the next steps include:

- Funnel Analysis
- Revenue & Profit Analysis
- Customer Analytics






