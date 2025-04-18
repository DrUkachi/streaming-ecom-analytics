# Project Title: E-commerce Stream Analytics

## Overview

In this project, I developed a real-time analytics pipeline to demonstrate how user conversion funnels can be monitored for a multi-category e-commerce platform. By leveraging a large-scale Kaggle dataset ([eCommerce Behavior Data](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store/metadata?utm_source=chatgpt.com)) containing 285 million users' events from November 2019 to April 2020, I engineered a system to track user journeys from product view → add to cart → purchase with millisecond latency. The goal demonstrate how was retailers can be empowered with actionable insights to detect drop-offs, identify trending products and understand customer behaviour.

## Problem Statement

E-commerce businesses need immediate insight into user clickstream behavior to understand conversion funnels, identify friction points (drop-offs), recognize popular items, and personalize the user experience in real-time. Currently, delays in processing this data prevent timely actions like triggering relevant offers or addressing site issues, potentially leading to user abandonment. This project aims to build a system to ingest, process, and visualize this data live.


## Data Pipeline Architecture

[![E-commerce Analytics Pipeline](https://doimages.nyc3.cdn.digitaloceanspaces.com/002Blog/0-BLOG-BANNERS/app_platform.png)](https://www.digitalocean.com/products/app-platform)

**Pipeline Type:** Hybrid

This project utilizes a hybrid data pipeline architecture, combining elements of both batch and streaming processing to handle historical data simulation and real-time analytics effectively.

*   **Data Ingestion:** 

    *   **Source**: Historical e-commerce event data (Oct 2019 - Apr 2020) downloaded as compressed CSV files (.csv.gz) from [Open CDP](https://data.rees46.com/datasets/marketplace/) using Python.

    *   **Initial Processing:** Due to the large dataset size (~8GB uncompressed), initial processing and cleaning are performed using PySpark on the downloaded files.

    *   **Streaming Simulation:** A Python script leveraging a Kafka client acts as a Kafka Producer. It reads the processed data and streams it to a Confluent Cloud Kafka Cluster topic. The schema for this raw event stream is defined using JSON Schema.

    *   **Ingestion Frequency & Orchestration:** To simulate a continuous stream without incurring excessive costs and to manage data volume for the project scope, Apache Airflow is used to orchestrate the Python Kafka producer script. Airflow triggers the script at 3-hour intervals. In each run, the script streams 10,000 rows for each day within a specific month (e.g., all of Oct 2019's daily 10k rows streamed in one go), iterating through the months (Oct 2019 to Apr 2020) in subsequent runs. This provides a controlled, time-distributed flow of data into Kafka.

    

*   **Data Lake/Warehouse:**

    - **Technology:** **Snowflake** serves as the central Data Warehouse. 

    *   **Setup:**: Data lands in Snowflake via two separate **Confluent Cloud Snowflake Sink Connectors**:
        - **Raw Data Sink:** Consumes the original event stream (JSON format) directly from the initial Kafka topic. This data populates tables intended for batch processing and historical analysis.

        - **Real-time Aggregates Sink:** Consumes processed, aggregated data (AVRO format) from separate Kafka topic generated by Flink. This populates tables optimized for the real-time dashboard.
    *   **Organization**: Data within Snowflake is explicitly organised into three logical layers: a raw layer -  schema that stores the unmodified event data ingested directly from Kafka; a processed layer - schema that holds near real-time analytics and aggregates derived from streaming pipelines; and a `REPORTING` schema used for historical batch processing and advanced analytics. This layered architecture ensures data traceability, efficient querying, and clear separation of concerns across different processing stages. To enhance performance and reduce query latency tables in were partitioned based on relevant attributes such as event timestamps and entitiy identifiers.



*   **Data Transformation:** 
    *   **Initial Batch Transformation (Pre-Kafka):** PySpark is used initially to handle the large raw CSV files, likely performing cleaning, basic structuring, and potentially filtering before the data is ready for the Kafka producer.

    *   **Streaming Transformation:** Flink SQL on Confluent Cloud is used for real-time stream processing. It consumes data from the raw Kafka topic, performs calculations like rolling aggregations, windowing functions, or filtering based on event types (view, cart, purchase), and outputs the results to a separate Kafka topic using AVRO schema for efficiency and schema evolution capabilities.

    *   **Batch Transformation (Post-Kafka, Airflow Orchestrated):** Once a streaming segment (e.g., one month's simulated data) is complete via the Python producer, Apache Airflow triggers batch transformation jobs directly within Snowflake using its SQL engine. These jobs run against the raw event data that has landed in Snowflake (via the first sink connector). They perform tasks like table refreshes or full recreations, calculating aggregates (e.g., daily/monthly summaries) needed for historical analysis and the batch dashboard in Metabase.

    *   **Near Real-time Transformation (Post-Flink):** After the Flink-processed data lands in Snowflake (via the second sink connector), minor additional transformations using Snowflake SQL might occur to prepare the data specifically for the real-time dashboard, ensuring it's in the precise format needed for visualization and adheres to the 5-minute refresh requirement.


*   **Workflow Orchestration:** 
    *   **Apache Airflow** plays a crucial role in orchestrating the batch-oriented components of this hybrid pipeline. It manages a workflow (DAG) that includes at least two key tasks run at 3-hour intervals:

        1.   Triggering the **Python Kafka producer script** to simulate the streaming of a segment of historical data into Confluent Cloud Kafka.


        2.   **Upon successful completion** of the data streaming task, triggering the **Snowflake SQL batch transformation jobs** (e.g., stored procedures or SQL scripts) to process the newly arrived raw data in Snowflake, refreshing or recreating the tables used for batch analysis and the Metabase dashboard.

   *    Airflow ensures that batch processing in Snowflake only  happens after the corresponding data segment has been successfully streamed, maintaining data consistency for the batch layer. (Note: Airflow does not directly orchestrate the continuous Flink stream processing or the Snowflake Sink connectors, which operate based on data availability in Kafka).


Okay, here's the template filled out based on the details you provided about your project:

### Technologies Used

* **Cloud Provider:** **Confluent Cloud** Note: Confluent Cloud is a managed cloud service that runs on underlying providers. I opted to use AWS for this.
* **Infrastructure as Code (IaC):** **None**

* **Workflow Orchestration:** **Airflow** (Apache Airflow used to schedule the Kafka producer script and the Snowflake batch transformation jobs).

* **Data Warehouse:** **Snowflake** (Used as the central repository for both raw event data and processed/aggregated data).
* **Batch Processing:** **PySpark** (Used for initial processing of large raw CSV files before streaming), **Snowflake SQL** (Used for batch transformations triggered by Airflow within the data warehouse).
* **Stream Processing:** **Kafka** (Specifically Confluent Cloud Kafka for messaging/event streaming), **Flink** (Specifically Flink SQL on Confluent Cloud for real-time analytics on the Kafka stream).
* **Dashboarding Tool:** **Metabase** (For visualizing the results of the batch and near-real time processing).
* **Other Tools:**
    * **Python:** Used for initial data download script and the Kafka producer script simulating the event stream.
    * **Confluent Cloud:** Managed platform service used to host and operate Kafka, Flink, and Schema Registry (implied for AVRO/JSON), simplifying infrastructure management for streaming components.
    * **Confluent Snowflake Sink Connector:** A specific Kafka Connect plugin used to efficiently and reliably transfer data from Kafka topics (both raw JSON and processed AVRO) into Snowflake tables.
    * **JSON Schema:** Used to define the structure of the raw event data being produced to the initial Kafka topic and sunk to Snowflake.
    * **AVRO Schema:** Used to define the structure of the data processed by Flink, providing schema evolution capabilities and efficient serialization for the processed Kafka topic and subsequent sink to Snowflake.

**Explanation of less common tools (if needed for course context):**

* **Confluent Cloud:** A fully managed, cloud-native service for Apache Kafka and its ecosystem components (like Flink, Kafka Connect, Schema Registry). It allows developers to build streaming applications without managing the underlying infrastructure.

* **AVRO (Apache Avro):** A data serialization system that uses schemas (typically defined in JSON) to structure data. It's efficient for storage and transmission, supports schema evolution (handling changes in data structure over time), and integrates well with systems like Kafka and Hadoop. It is required by Confluent Cloud's Schema Registry to create 'Data Contracts'.

* **Confluent Snowflake Sink Connector:** Part of the Kafka Connect framework, this specific connector is optimized to stream data from Kafka topics directly into Snowflake tables, handling schema mapping and data loading automatically.

## Dashboard

[Describe your dashboard. What insights does it provide? What are the key metrics visualized? Include screenshots of your dashboard]

The dashboard consists of 5 tiles:

*   **Tile 1: Top 15 Revenue by Category:** This tile displays a horizontal bar chart showing the total revenue generated by each product category that can be filtered within a specific period (default overall). The chart specifically highlights the 15 categories that brought in the most revenue, usually sorted from highest to lowest. It provides the visualization that quickly identifies the most profitable product categories. It helps businesses understand which areas are driving the most significant portion of income, informing decisions about inventory investment, marketing focus, and identifying potential growth areas or underperforming categories relative to the top performers.

*   **Tile 2: Top Selling Brand:** This tile uses a bar chart to display performance of different brands sold through the e-commerce platform. It offers valuable insights to the business by highlighting brands that are most popular with customer base. It can also be used to optimse inventory to ensure stock levels do not drop for these and sales maximised.

*   **Tile 3: Daily Revenue:** This tile uses a horizontal line chart where the horizontal axis represents time (days) and vertical axis represents total revenue amount ($). Each point shows how much revenue as achieved on a specific day.

## Reproducibility

[Provide clear and concise instructions on how to run the code and reproduce the results.  This is critical for the evaluation. Be sure to include:]

1.  **Prerequisites:** [List all necessary software and dependencies.]
2.  **Setup:** [Describe how to set up the environment, including configuring access to cloud services and installing dependencies.]
3.  **Execution:** [Provide step-by-step instructions on how to run the code, including any necessary commands and configurations.]

[The instructions should be complete and easy to follow so that someone else can easily run your project.]

## Challenges and Lessons Learned

* **Challenge:** Extracting and processing large datasets using Pandas proved challenging due to the memory limitations of the processing environment. I resolved this by first testing the environment's memory capacity and then switching to PySpark for datasets that exceeded the available memory threshold, as PySpark efficiently handled the larger volumes. I learned the importance of proactively assessing environmental memory constraints against data size and recognized PySpark's superior capability for processing large datasets, especially when leveraging cluster resources for speed.

* **Challenge:** Configuring the Kafka producer to work seamlessly with the Snowflake Sink connector was difficult, particularly achieving compatibility across different data sources (Python producer vs. Flink) and schema formats. I resolved this by establishing clear data contracts per topic and standardizing on one schema format per topic. After finding JSON schemas worked for Python-produced data but not Flink-processed data with the Snowflake connector, I successfully switched the relevant topic and connector configuration to use the AVRO schema format, which proved compatible with both Flink and the Snowflake Sink connector. I learned the necessity of strict data contracts and consistent schema formats (like AVRO) per Kafka topic, and that connector compatibility can differ based on the upstream processing tool, requiring testing to find the right combination (in this case, AVRO for Flink-to-Snowflake).

* **Challenge:** Initially, understanding and correctly implementing Snowflake's access control model, including the differences and interactions between roles, accounts, and privileges, was confusing. I resolved this by investing time in studying Snowflake's documentation and security concepts until I understood the hierarchical structure and could configure permissions appropriately. I learned the specific details of Snowflake's role-based access control (RBAC) system and the critical importance of mastering its structure for effective security management.

* **Challenge:** Setting up the Apache Airflow environment correctly using Docker was challenging as it had been some time since I last worked extensively with this specific technology stack. I resolved this by reviewing current Docker and Airflow documentation, configuration best practices, and examples, which allowed me to successfully configure the necessary environment. I learned (or refreshed my knowledge on) the specifics of setting up Airflow within Docker and reinforced the need to consult up-to-date documentation when returning to technologies after a period of non-use.

## Acknowledgements

[Acknowledge any individuals or resources that helped you with this project.]

## Repository Structure
