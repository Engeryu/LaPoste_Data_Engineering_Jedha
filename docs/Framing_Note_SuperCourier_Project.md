# **Framing Note: SuperCourier Project**

## **1\. Context and Problem Statement**

### **1.1. Initial Context**

The SuperCourier project is initially a certification exercise for Data Engineers in training. Its objective is to build a simple ETL pipeline to prepare a dataset that will be used by a Data Science team to model delivery delays.

### **1.2. Identified Problem**

The current exercise, while functional, has significant pedagogical and technical shortcomings. It boils down to translating a provided algorithm into a monolithic Python script, using entirely synthetic data and a limited technological scope.  
This approach does not prepare future engineers for the real-world challenges of a production environment, such as:

* **Scalability** to handle large data volumes.  
* **Flexibility** to process various source data formats.  
* **Maintainability** and **evolvability** of a modular codebase.  
* **Integration of software engineering best practices** (testing, CI/CD, configuration management).  
* **Deployment** of the solution to different targets (CLI, web service, container).

## **2\. Strategic Vision**

This redesign elevates SuperCourier from a simple exercise to a **production-grade data processing platform**. This project will serve as the ultimate reference ("ultimate correction") for apprentices, demonstrating the skills and standards expected of a modern Data Engineer.  
The platform will be designed as a **modular, high-performance, and versatile tool**, capable not only of meeting the initial need of Data Scientists but also of serving other use cases within the company. The focus will be on creating a robust, testable, and easily deployable software asset.

## **3\. Project Objectives and Scope**

### **3.1. Functional Objectives**

* **Versatile Data Ingestion:** The platform will ingest data from multiple sources, including flat files (.csv, .json, .parquet, .xlsx, .db).  
* **Dual Processing Capability:**  
  1. Process **real-world data** provided by a user.  
  2. Generate **large-scale synthetic data** (millions of rows) for load testing and demonstrations.  
* **Enrichment via External API:** Replace the generation of fictitious weather data with calls to an external API to obtain real conditions based on date and location.  
* **Flexible Output:** Allow the user to choose the output format(s) for the processed data, regardless of the original input format.  
* **Externalized Configuration:** Enable pipeline configuration (column mapping, calculation parameters) via an external file (e.g., config.yaml) without modifying the source code.

### **3.2. Technical Objectives**

* **High Performance:** The processing core must be optimized for speed and low memory consumption to handle large datasets on standard machines.  
* **Quality and Testing:** Implement a comprehensive suite of unit and integration tests to ensure code reliability and prevent regressions.  
* **Continuous Integration & Deployment (CI/CD):** Automate testing, linting, and building of the project via a CI/CD pipeline (e.g., GitHub Actions).  
* **Multiple Entrypoints:**  
  1. A **Command-Line Interface (CLI)** for technical users and automation.  
  2. A **Web API** for programmatic integration with other services.  
* **Portability:** The project must be fully **containerizable** (Docker) to ensure reproducible deployment in any environment.  
* **Dependency Management:** Use Conda to manage environments and dependencies, ensuring cross-platform compatibility.

## **4\. Stakeholders**

* **Apprentice Data Engineers:** Primary users of the project as a pedagogical tool and portfolio piece.  
* **Data Scientists:** End consumers of the cleaned and enriched data.  
* **Other (Fictional) Stakeholders:** Potential users of the API or a future GUI for ad-hoc analysis.

## **5\. Acceptance Criteria**

To be considered complete and successful, the project must meet the following criteria:

* **Code Quality:** The codebase must pass automated linting and static analysis checks (e.g., via ruff and mypy) in the CI pipeline.  
* **Performance Benchmark:** The core processing pipeline must process a synthetic dataset of 1 million records in under 60 seconds on a standard reference machine (e.g., GitHub Actions runner).  
* **Test Coverage:** The overall test coverage, measured by pytest-cov, must be maintained at or above 80%.  
* **API Functionality:** The Web API must expose at least one endpoint to trigger a data processing job and another to retrieve the status or result.  
* **Documentation:** The README.md must provide complete, validated setup and usage instructions. All public functions and classes must have PEP 257-compliant docstrings.