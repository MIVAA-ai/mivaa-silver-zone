# MIVAA Silver Zone Tool

This repository contains the **MIVAA Silver Zone Tool**, a critical software utility for transforming and enriching field boundary data that has been validated in the **Bronze Zone**. The tool applies advanced validation rules, coordinate transformations, and OSDU metadata integration, ensuring the data is optimized for further processing in the **Gold Zone**.

The Silver Zone enhances **data quality** by:
- Filtering and refining Bronze Zone results.
- Performing **coordinate transformations** to WGS84 using OSDU's CRS services.
- Validating and enriching metadata with **OSDU ID resolution**.
- Logging results and **storing structured outputs** for downstream consumption.

---

## Prerequisites

1. **Download the Repository**:
   - [Clone](https://github.com/MIVAA-ai/mivaa-silver-zone.git) or download the repository as a [ZIP](https://github.com/MIVAA-ai/mivaa-silver-zone/archive/refs/heads/main.zip) file.

2. **Unzip the Repository**:
   - Extract the downloaded ZIP file to a folder on your system.

3. **Install Python**:
   - Ensure Python 3.9+ is installed on your machine. You can download Python [here](https://www.python.org/downloads/).

4. **Install Docker**:
   - Ensure Docker and Docker Compose are installed and running on your machine. You can download Docker [here](https://www.docker.com/).

---

## Steps to Run the Application Using the Startup Script

### 1. Setup the Environment

#### For Windows:
1. Open Command Prompt.
2. Navigate to the repository directory and run:
   ```cmd
   startup-windows.bat D:/MIVAA-ai/mivaa-silver-directory
   ```
   Replace `D:/MIVAA-ai/mivaa-silver-directory` with your desired base directory.

#### For Linux:
1. Open a terminal.
2. Navigate to the repository directory.
3. Make the script executable (only needed the first time):
   ```bash
   chmod +x startup-linux.sh
   ```
4. Run the command:
   ```bash
   ./startup-linux.sh /path/to/mivaa-silver-directory
   ```
   Replace `/path/to/mivaa-silver-directory` with your desired base directory.


   ### 2. Initialize the Database

Run the following command in your terminal:
```bash
python startup.py
```
This will initialize the database and prepare the application for use. Example log output:
```plaintext
INFO - Initialize the database from the JSON Schema file.
INFO - Database initialization completed successfully.
INFO - Starting application...
```

### 3. Start the Application Using Docker Compose

Run the following command to start the application:
```bash
docker-compose --env-file .env up --build
```

### Error Logging
Errors are logged in the database with severity levels (`WARNING`, `ERROR`). Detailed logs are generated to help users identify and resolve issues efficiently.

---

## Accessing Logs and Outputs

- **Uploads Directory**:
  Place your CSV files in the directory specified in the `UPLOADS_DIR` path in your `.env` file.
- **Processed Directory**:
  The validated and processed files will be saved in the directory specified in the `OUTPUT_DIR` path.
- **Error Logs**:
  Detailed error logs are saved in the database and corresponding output folders for review.


---

## Project Structure

```
mivaa-silver-zone/
├── silver_zone_processor.py  # Main processing script
├── config/                   # Configuration settings
│   ├── logger_config.py      # Logging configuration
│   ├── project_config.py     # Project settings and environment variables
├── models/                   # Database models
│   ├── field_bronze_data.py  # Fetches data from Bronze Zone
│   ├── field_silver_data.py  # Stores and fetches Silver Zone results
│   ├── validation_errors.py  # Logs validation errors
├── osdu/                     # OSDU integration
│   ├── osdu_client.py        # OSDU API client for metadata resolution
├── utils/                     # Utility functions
│   ├── db_utils.py           # Database handling
│   ├── validation_utils.py   # Validation logic
├── migrations/                # Database migrations
├── tests/                     # Unit and integration tests
│   ├── test_validations.py   # Validation unit tests
│   ├── test_crs_conversion.py # CRS transformation tests
├── docker/                    # Docker configuration
│   ├── Dockerfile             # Docker image setup
│   ├── docker-compose.yml     # Docker Compose settings
├── .env                        # Environment variables
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation
```

---

## Troubleshooting

- **Check Environment Variables**:
  Ensure the directories specified in the `.env` file exist and are accessible.
- **Inspect Docker Logs**:
  Run the following command to view Docker logs:
  ```bash
  docker-compose logs
  ```
- **Rebuild Containers**:
  If you encounter issues, rebuild the containers using:
  ```bash
  docker-compose --env-file .env up --build
  ```

---

## Additional Resources
- **Blog**: Read the detailed blog post about this application: 
- **Medallion Architecture**: Learn more about the principles of Medallion Architecture [here](https://example.com).

---

## Notes

- This application requires Docker Compose.
- This application is currently tested in the windows environment, incase you face any issues running it in Linux, feel free to reach out.

Feel free to raise any issues or suggestions for improvement! Reach out at [info@deepdatawithmivaa.com](mailto:info@deepdatawithmivaa.com) for more help, comments, or feedback.

