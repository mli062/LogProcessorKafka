# LogProcessorKafka

## Overview
Log Processor Kafka is a scalable log processing application designed to handle connection logs from Kafka sources. 
The project is implemented using Scala for the main processing logic and Python for generating synthetic logs.

## Key Features
- **Synthetic Log Generation:** Python scripts for generating synthetic connection logs and GeoIP data for testing and development.
- **Log Processing:** Extracts timestamp, username, IP address, port, and connection status from connection logs.
- **Analytics:** Provides insights into total connections, successful connections, and failed connections over time.

## Project Structure
The project is organized into the following directories:
- `src/main/scala`: Scala source code for the main application and LogProcessor.
- `src/main/python`: Python source code for log generation scripts.
- `tests/scala`: Scala test code for LogProcessor.
- `tests/python`: Python test code for log generation scripts.
- `resources`: Project resources, including GeoIP data.

## Prerequisites
Before you begin, ensure you have the following software installed:
- Java
- Maven
- Python 3

## Getting Started
1. **Clone Repository:**
   ```bash
   git clone <repository_url>
   ```
2. **Set Up Python Virtual Environment:**
   ```bash
   python3 -m venv lpk
   # activate lpk env
   source lpk/bin/activate
   pip install -r requirements.txt
   ```
   ```bash
   # Deactivate lpk env
   deactivate
   ```
3. **Install Kafka and run on local machine**
   ```bash
   Download kafka version 2.13-3.6.1 and extract it:
   https://www.apache.org/dyn/closer.cgi?path=/kafka/3.6.1/kafka_2.13-3.6.1.tgz
   
   tar -xzf kafka_2.13-3.6.1.tgz
   cd kafka_2.13-3.6.1
   ```
   Start Kafka Environment:
   ```bash
   cd bin
   bash zookeeper-server-start.sh ../config/zookeeper.properties
   bash kafka-server-start.sh ../config/server.properties
   ```
   You can find the official documentation [here](https://kafka.apache.org/quickstart).
4. **Run Python Log Generation Script**:
   ```bash
   python src/main/python/log_generator/main_log_generator.py
   ```
5. **Build and Run Scala/Spark Application**:<br/>
   Version use for Scala 2.13.12.<br/>
   Version use for Spark 3.5.0.
   ```bash
   mvn clean install
   ```

## GitHub Actions Pipeline

We use GitHub Actions to automate the testing process for both the Python and Scala components of the Log Processor Kafka project. The pipeline consists of two jobs:

1. **Python Tests:**
   - Executes Python tests on the Ubuntu latest environment.
   - Installs required dependencies using the `requirements.txt` file.
   - Runs Python unit tests using the `unittest` framework.
2. **Maven Tests**:
   - Executes Maven tests for the Scala code on the Ubuntu latest environment.
   - Sets up Java using the `actions/setup-java` action.
   - Builds and tests the Scala code using `mvn clean install`.

## GeoLite2 Database

The GeoIP data used in this project is sourced from the [ip-location-db](https://github.com/sapics/ip-location-db) repository. 
The GeoLite2 database provides information about the geographical location (country) of IP addresses.

## Testing
The project includes unit tests for both Scala and Python components. To run the tests, use the following commands:

Scala Tests: `mvn test`<br/>
Python Tests: `python -m unittest discover tests/python/ -p "test_*.py"`