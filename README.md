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
├── .env                                    # File containing the API key and path definition
├── config                                  # Contains 
│   └── config_file.json
├── data
│   ├── loaded
│   ├── processed
│   └── raw
│       ├── city_codes
│       ├── weather_codes
│       └── weather_data
└── src
    ├── ingestion
    │   └── ingestion_weather_data.py
    ├── loading
    │   ├── loading_city_codes.py
    │   ├── loading_weather_codes.py
    │   └── loading_weather_data.py
    ├── processing
    │   ├── processing_city_codes.py
    │   ├── processing_weather_codes.py
    │   └── processing_weather_data.py
    ├── setup
    │   └── setup.py
    └── utils
        ├── auxiliary_functions.py
        ├── input_configuration.py
        └── weather_api_client.py
``` 
