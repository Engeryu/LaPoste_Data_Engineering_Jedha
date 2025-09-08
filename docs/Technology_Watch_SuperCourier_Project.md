# **Technology Watch: SuperCourier Project**

## **1\. Introduction**

This document presents a comparative analysis of the technologies considered for the SuperCourier project. Each choice is justified by evaluating the advantages and disadvantages of popular alternatives within the project's context of performance, maintainability, and pedagogical relevance. The selected language version is **Python 3.12+**.

## **2\. Data Processing Engine**

**Requirement:** Process millions of rows efficiently on a single, standard machine.

| Technology | Pros | Cons | Rationale & Decision |
| :---- | :---- | :---- | :---- |
| **Pandas** | \- The de facto standard in the Python data ecosystem.\<br\>- Vast community and extensive documentation. | \- Single-threaded by design.\<br\>- High memory overhead, struggles with out-of-core data.\<br\>- Poor performance on large datasets. | Unsuitable for the project's scalability goals. It serves as a good baseline but fails to meet performance requirements. |
| **PySpark** | \- Designed for distributed computing on clusters.\<br\>- Scales to petabytes of data.\<br\>- Industry standard for Big Data. | \- High overhead and complexity for local setup (requires a JVM).\<br\>- Overkill for datasets that fit in a single machine's memory.\<br\>- Slower than Polars on a single node. | Over-engineered for this project. The complexity of a distributed system is not justified by the requirements and would detract from the core learning objectives. |
| **Polars** | \- **Multi-threaded by default, utilizing all CPU cores.**\<br\>- **Lazy evaluation** optimizes query plans.\<br\>- Written in Rust for exceptional speed and memory efficiency. | \- Smaller ecosystem compared to Pandas.\<br\>- Steeper initial learning curve for developers accustomed to Pandas. | **Decision: Polars.** It is the ideal choice, offering superior performance on a single machine without the complexity of a distributed framework. It represents a modern, high-performance standard. |

## **3\. Dependency and Environment Management**

**Requirement:** Provide a reliable and reproducible environment, familiar to Data Engineers.

| Technology | Pros | Cons | Rationale & Decision |
| :---- | :---- | :---- | :---- |
| **Poetry** | \- Excellent dependency resolver with a lock file for deterministic builds.\<br\>- Unified project management and packaging. | \- Less common in the data science community.\<br\>- Does not manage non-Python dependencies (e.g., system libraries). | While superior for pure application development, it is less aligned with the target audience's typical workflow and toolset. |
| **Conda** | \- **The standard in the Data Science/Engineering community.**\<br\>- Manages both Python and non-Python dependencies.\<br\>- Robust environment isolation. | \- Slower dependency resolution compared to Poetry.\<br\>- Less focused on application packaging. | **Decision: Conda.** This choice aligns the project with industry standards for data professionals, ensuring that apprentices work with tools they will encounter in their careers. |

## **4\. Web API Framework**

**Requirement:** A high-performance, modern framework for exposing the processing engine via a web service.

| Technology | Pros | Cons | Rationale & Decision |
| :---- | :---- | :---- | :---- |
| **Flask** | \- Mature, lightweight, and highly extensible.\<br\>- Simple to get started with. | \- Synchronous by default, requiring extra components (e.g., Gunicorn) for concurrent performance.\<br\>- No built-in data validation. | A solid choice, but lacks the modern features and out-of-the-box performance of asynchronous frameworks. |
| **Starlette** | \- A lightweight ASGI framework/toolkit.\<br\>- Provides the foundation for FastAPI, offering extreme performance. | \- More of a toolkit than a full-featured framework.\<br\>- Requires more boilerplate for features like data validation and documentation. | An excellent core, but for this project, a higher-level framework that builds upon it is more practical. |
| **FastAPI** | \- **Extremely high performance (built on Starlette).**\<br\>- **Automatic data validation** with Pydantic.\<br\>- **Automatic, interactive API documentation** (Swagger UI). | \- Newer ecosystem compared to Flask. | **Decision: FastAPI.** It is the modern standard for building high-performance Python APIs. Its built-in validation and documentation features accelerate development and improve quality. |

## **5\. CI/CD Platform**

**Requirement:** An accessible, powerful platform to automate testing and builds.

| Technology | Pros | Cons | Rationale & Decision |
| :---- | :---- | :---- | :---- |
| **Jenkins** | \- Extremely powerful and extensible via a vast plugin ecosystem.\<br\>- Self-hosted, offering full control over the environment. | \- High maintenance overhead.\<br\>- Steep learning curve and complex configuration ("Jenkinsfile").\<br\>- Can be considered a legacy tool. | Too complex and maintenance-intensive for the scope of this project. The focus should be on the application, not on managing the CI/CD infrastructure. |
| **GitHub Actions** | \- **Natively integrated with GitHub.**\<br\>- Simple, YAML-based configuration.\<br\>- Large marketplace of reusable actions.\<br\>- Managed runners available for free. | \- Tightly coupled to the GitHub platform.\<br\>- Less flexibility than a self-hosted Jenkins instance. | **Decision: GitHub Actions.** Its seamless integration with the source code repository and ease of use make it the perfect choice for automating workflows in a modern project. |

## **6\. External Weather API**

**Requirement:** A reliable, free, and easy-to-use API for fetching historical weather data.

| Technology | Pros | Cons | Rationale & Decision |
| :---- | :---- | :---- | :---- |
| **OpenWeatherMap** | \- Very popular and well-documented.\<br\>- Provides a wide range of weather data types. | \- Requires an API key.\<br\>- The free tier has significant limitations on historical data access and call frequency. | The restrictions of the free tier, especially on historical data, make it unsuitable for enriching a large dataset spanning different dates. |
| **Open-Meteo** | \- **No API key required.**\<br\>- **Completely free** with a generous fair-use policy.\<br\>- High-performance API designed for historical and forecast data. | \- Provides slightly fewer data parameters than some paid alternatives. | **Decision: Open-Meteo.** Its keyless access, focus on performance, and permissive free tier make it the ideal choice for this project, removing friction for developers. |

