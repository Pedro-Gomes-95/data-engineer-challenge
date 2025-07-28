# Weather data collection

## Project description
**Note**: you can find a TL;DR below.

The code in this repository fetches weather data for one or more predefined cities using the Open Weather API. You can find out more about this website in [this](https://openweathermap.org/) link. Details on API calls can be found [here](https://openweathermap.org/current#name). A description of the project is provided below.

⚠️ **IMPORTANT**: in order to access the API, you need an API key. To obtain this key, start by registering in the link provided. You can find the API key in your account page, under the "API key" tab. This key **must** be added to the .env file. This is explained in more detail below.

The following information is available after pipeline execution:

* Weather data information:  

    | Field Name              | Description                                                       |
    |-------------------------|-------------------------------------------------------------------|
    | `time_value`            | UTC time of data calculation                                      |
    | `timezone`              | Shift in seconds from UTC                                         |
    | `city_id`               | Unique identifier of the city (used to join with city metadata)   |
    | `weather_id`            | Weather condition code (used to join with weather codes)          |
    | `visibility`            | Visibility distance (meters)                                      |
    | `temperature`           | Current temperature (°C)                                          |
    | `perceived_temperature` | Human perception of temperature  (°C)                             |
    | `min_temperature`       | Minimum temperature at the moment (°C)                            |
    | `max_temperature`       | Maximum temperature at the moment (°C)                            |
    | `atm_pressure_sea_level`| Atmospheric pressure at sea level (hPa)                           |
    | `atm_pressure_ground_level`| Atmospheric pressure at ground level (hPa)                     |
    | `humidity`              | Humidity (%)                                                      |
    | `wind_speed`            | Wind speed (m/s)                                                  |
    | `wind_direction`        | Wind direction (in degrees)                                       |
    | `wind_gust`             | Wind gust speed (if available)                                    |
    | `cloudiness`            | Cloudiness (percentage)                                           |
    | `rain`                  | Precipitation (mm/h)                                              |
    | `snow`                  | Precipitation (mm/h)                                              |
    | `time_sunrise`          | UTC sunrise time                                                  |
    | `time_sunset`           | UTC sunset time                                                   |
    | `file_name`             | Name of the source file the data was extracted from               |
    | `ingestion_date`        | Date on which the file was created                                |

* Weather code information:

    | Field              | Description                                      |
    |--------------------|--------------------------------------------------|
    | `id`               | Unique identifier for the weather condition      |
    | `short_description`| General category of the weather (e.g., Rain)     |
    | `long_description` | More detailed description of the weather condition    |
    | `ingestion_date`             | Date on which the file was created               |


* City metadata:  

    | Field        | Description                                      |
    |--------------|--------------------------------------------------|
    | `id`         | Unique identifier for the city                   |
    | `name`       | Name of the city                                 |
    | `state`      | Name of the state                                |
    | `country`    | ISO 3166 country code (e.g., "PT")               |
    | `longitude`  | Geographic longitude coordinate of the city      |
    | `latitude`   | Geographic latitude coordinate of the city       |
    | `ingestion_date`             | Date on which the file was created               |


## TL;DR
If you are short on time, follow the instructions below:
1. Set up the project in your local machine, using the procedure described in the section "Getting started".
2. Add your own API key to the .env file (**the code will not execute without the API key**.)
3. Add the cities for which you want to fetch weather data to the  `config_file.json` file.
4. Execute the file `pipeline.py`.
5. Weather data can be found in the `data/processed/weather_data_processed.parquet` file.

### Getting started
Follow these instructions to copy the project to your local machine and run it:

#### Clone the repository
Make sure you have Git installed in your computer. Clone the repository using Git Bash and the following command:  
```
git clone https://github.com/Pedro-Gomes-95/data-engineer-challenge.git
```

#### Install requirements
Make sure you have Python installed in your computer. Install the dependencies in `requirements.txt` in the environment you are using:  
```
pip install -r requirements.txt
```

### Running with Docker
You can run this project in a Docker container. To do so, build the Docker image:  
```
docker build -t weather-data .
```

Run the container:  
```
docker run -it --rm weather-data
```

If you wish to interact with the container via bash shell, run:  
```
docker run -it --rm weather-data /bin/bash
```

You can replace `weather-data` with any other name if you wish.

### Directory structure
Code separation is the key to staying sane. The directory structure is as follows:
```
├── .env                                    # API key and path definition
├── config                                  
│   └── config_file.json                    # API, city and file definitions
├── data    
│   ├── loaded                              # Directory for the loaded data
│   ├── processed                           # Directory for the processed data
│   └── raw                                 # Directory for the raw data
│       ├── city_codes                      
│       ├── weather_codes                   
│       └── weather_data                    
├── src                                     # Source code folder
|   ├── ingestion                           # Code for ingesting the data
|   │   └── ingestion_weather_data.py       # Code for making API calls and getting weather data
|   ├── loading                             # Code for loading the data into Parquet files
|   │   ├── loading_city_codes.py           
|   │   ├── loading_weather_codes.py
|   │   └── loading_weather_data.py
|   ├── processing                          # Code for processing and cleaning the Parquet files
|   │   ├── processing_city_codes.py
|   │   ├── processing_weather_codes.py
|   │   └── processing_weather_data.py
|   ├── setup
|   │   └── setup.py                        # Sets up the folders where the data will be stored
|   └── utils
|   │   ├── auxiliary_functions.py          # Aux functions used in the code
|   │   ├── input_configuration.py          # Input validation to the WeatherAPI class
|   │   └── weather_api_client.py           # Weather API class
|   pipeline.py                             # Runs all the code
├── .dockerignore                           # Docker ignore file
├── Docker                                  # Dockerfile
├── requirements.txt                        # Requirements file
└── README.md                               # This file
``` 

### Project description
The source code for this project follows a modular approach, based on the ELT (Extract-Load-Transform) pattern. It is structured in 3 layers:
* **Ingestion**  
This layer is where weather data is fetched from the API. Cities to be queried are defined in the `config_file.json`. For each city, weather data is stored under `data/raw/weather_data/<city-name>`, where `<city-name>` is the target location.

The `data/raw` directory also includes two other subfolders: `weather_codes` and `city_codes`. The former contains descriptive weather condition codes, which were manually compiled into a CSV file based on the information provided in [this](https://openweathermap.org/weather-conditions) page. The latter contains a list of cities and their metadata, as a JSON file, downloaded from the [OpenWeather bulk data page](https://bulk.openweathermap.org/sample/) (`city.list.json.gz`)

* **Loading**  
The raw data is parsed and transformed into structured Parquet files. 

* **Processing**  
The data is cleaned and filtered. The output from this step is ready for further analysis.

Each layer is implemented as a separate module under the `src/` directory, making the data pipeline easy to maintain, test, and extend. The full pipeline can be triggered by the `pipeline.py` script.

#### `env`
The `.env` file is extremely important in the execution of the data pipeline, as it stores the API key and defines custom paths used during ingestion, loading, and processing. 

⚠️ **IMPORTANT**: as mentioned above, you must add your API key to this file before running the pipeline. After this, make sure that you add the file to the `.gitignore` file, so that your key is not accidentaly uploaded to Git.

#### `config`
This folder contains a JSON file that centralizes the configuration for the pipeline. It includes:

* `cities`  
A list of cities for which weather data should be collected.

* `api`  
Contains settings related to the API:
    * `base_url`: the root URL used for the API requests.
    * `units`: the measurement system (can be"standard", "metric" or "imperial").
    * `language`: the language of the output.

* `ingestion_layer`  
Contains settings related to the raw data ingestion:
    * `weather_data`
        * `fields`: a mapping of fields and their data types from the API response.
    * `weather_codes`: 
        * `file_name`: the name of the raw file that stores weather condition codes.
    * `city_codes`: 
        * `file_name`: the name of the raw file containing metadata about cities.

* `loading layer`  
Configurations for transforming the raw data into Parquet files:
    * `weather_data`:  
        * `table_name`: the name of the output Parquet file.
        * `logging_file`: the name of the text file used to log which raw files have already been processed.
    * `weather_codes`: 
        * `file_name`: the name of the output Parquet file.
    * `city_codes`: 
        * `file_name`: the name of the output Parquet file.

* `processing layer`  
Settings related to data processing:
    * `weather_data`:
        * `table_name`: name of the final processed Parquet file.
        * `columns_rename`: dictionary for renaming the columns.
    * `weather_codes`: 
        * `file_name`: name of the processed weather codes file.
        * `columns_rename`: dictionary for renaming the columns.
        * `fields`: dictionary containing the data types for each column (used in type casting).
    * `city_codes`: 
        * `file_name`: the name of the Parquet file that stores processed city data.
        * `columns_rename`: dictionary for column renaming.
        * `fields`: dictionary containing the data type of each column for casting purposes.

#### `data`
The `data` directory is organized into 3 folders, each folder reflecting a stage of the pipeline:
* `data/raw`  
Contains raw, unprocessed files obtained either from data from the API or the Open Weather website. Has the following subfolders:

    * `weather_data`: stores raw weather data fetched from the Open Weather API. Each file corresponds to a single city and timestamp, in a JSON format. The files follow the convention `YYYYMMDD_HHMMSS_<city_name>.json`, where `YYYY`, `MM`, and `DD` represent the year, month, and day of the weather measurement; `HH`, `MM`, and `SS` represent the hour, minute, and second; and `<city_name>` corresponds to the name of the city queried.

    * `weather_codes`: contains descriptive metadata about weather condition codes. Created manually based on the Open Weather documentation, and stored as a CSV.

    * `city_codes`: a JSON file downloaded from Open Weather's bulk dataset, containing city metadata such as name, ID, country, and coordinates.

* `data/loaded`  
Stores the Parquet versions of the raw data. Each file consists of transforming the raw inputs in Parque tables. In the case of weather data, all the JSON files are processed into a single Parquet file.

* `data/processed`  
Contains the schema-validated, standardized and reformatted datasets ready for analysis. Transformations include (but are not limited to) renaming columns and doing schema enforcement.

#### `src`
The `src` folder contains the source code for the pipeline, organized by layers, mimicking an ELT logic. Each script is properly documented and contains the relevant information about the steps taken. It contains the following folders:

* `setup`  
Includes the `setup.py` script, which creates the data structure described above, before any processing begins.

* `utils`  
Stores utility scripts, including helper functions (under `auxiliary_functions.py`) and the API client logic (under `weather_api_client.py`).

* `ingestion`  
The only script in this folder is `ingestion_weather_data`, which fetches raw weather data from the API and stores them under `raw/data/weather_data`. This script uses the API client logic stored in `utils/weather_api_client.py`.

* `loading`
Handles the transformation of raw files into the Parquet format, with individual scripts for each dataset: `loading_city_codes.py`, `loading_weather_codes.py`, and `loading_weather_data.py`. 




