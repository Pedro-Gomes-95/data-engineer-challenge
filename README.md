# Weather data collection

### Project description

The code in this repository fetches weather data for one or more predefined cities, from the Open Weather API. You can find out more about this website in [this](https://openweathermap.org/) link. Details on API calls can be found [here](https://openweathermap.org/current#name).

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
