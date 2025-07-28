# Weather data collection

### Project description

The code in this repository fetches weather data for one or more predefined cities using the Open Weather API. You can find out more about this website in [this](https://openweathermap.org/) link. Details on API calls can be found [here](https://openweathermap.org/current#name). A description of the project is provided below.

⚠️ **IMPORTANT**: in order to access the API, you need an API key. To obtain this key, start by registering in the link provided. You can find the API key in your account page, under the "API key" tab. This key **must** be added to the .env file. This is explained in more detail below.

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
The configuration file in this folder holds the following definitions:
* `cities`: the cities for which to fetch data.
* `api`
    * `base_url`: the base URL of the API to which to make requests to.
    * `units`: the units of measurement. According to the documentation, can be "standard", "metric" or "imperial".
    * `language`: the language of the output.
* `ingestion_layer`: configurations related to the files in the ingestion layer.
    * `weather_data`: configurations related to the weather data.
        * `fields`: the fields in the data retrieved by the API, and the respective data types.
    * `weather_codes`: 
        * `file_name`: the raw file from which to read weather information. 
    * `city_codes`: 
        * `file_name`: the raw file from which to read city information.
* `loading layer`: configurations related to the files in the loading layer.
    * `weather_data`: configurations related to the loaded weather data.
        * `table_name`: the name of the Parquet file that stores loaded weather data.
        * `logging_file`: the name of the txt file that stores the names of files already processed.
    * `weather_codes`: 
        * `file_name`: the name of the Parquet file that stores loaded weather code data.
    * `city_codes`: 
        * `file_name`: the name of the Parquet file that stores loaded city data.

#### `src`
# Weather data collection

### Project description

The code in this repository fetches weather data for one or more predefined cities using the Open Weather API. You can find out more about this website in [this](https://openweathermap.org/) link. Details on API calls can be found [here](https://openweathermap.org/current#name). A description of the project is provided below.

⚠️ **IMPORTANT**: in order to access the API, you need an API key. To obtain this key, start by registering in the link provided. You can find the API key in your account page, under the "API key" tab. This key **must** be added to the .env file. This is explained in more detail below.

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
The configuration file in this folder holds the following definitions:
* `cities`: the cities for which to fetch data.
* `api`
    * `base_url`: the base URL of the API to which to make requests to.
    * `units`: the units of measurement. According to the documentation, can be "standard", "metric" or "imperial".
    * `language`: the language of the output.
* `ingestion_layer`: configurations related to the files in the ingestion layer.
    * `weather_data`: configurations related to the weather data.
        * `fields`: the fields in the data retrieved by the API, and the respective data types.
    * `weather_codes`: 
        * `file_name`: the raw file from which to read weather information. 
    * `city_codes`: 
        * `file_name`: the raw file from which to read city information.
* `loading layer`: configurations related to the files in the loading layer.
    * `weather_data`: configurations related to the loaded weather data.
        * `table_name`: the name of the Parquet file that stores loaded weather data.
        * `logging_file`: the name of the txt file that stores the names of files already processed.
    * `weather_codes`: 
        * `file_name`: the name of the Parquet file that stores loaded weather code data.
    * `city_codes`: 
        * `file_name`: the name of the Parquet file that stores loaded city data.

#### `src`
