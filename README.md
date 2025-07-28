# Weather data collection

### Project description

The code in this repository fetches weather data for one or more predefined cities, from the Open Weather API. You can find out more about this website in [this](https://openweathermap.org/) link. Details on API calls can be found [here](https://openweathermap.org/current#name).

⚠️ **IMPORTANT**: in order to access the API, you need an API key. To obtain this key, start by registering in the link provided. You can find the API key in your account page, under the "API key" tab.

### Getting started
Follow these instructions to copy the project to your local machine and run it:

#### 1. Clone the repository
Make sure you have Git installed in your computer. Clone the repository using Git Bash and the following command:  
```git clone https://github.com/Pedro-Gomes-95/data-engineer-challenge.git```

#### 2. Install requirements
Make sure you have Python installed. Install the dependencies in `requirements.txt` in the environment you are using:  
```pip install -r requirements.txt```

### Running with Docker
You can run this project in a Docker container. To do so, build the Docker image:  
```docker build -t <your-image-name> .```

Then, run the container:  
```docker run -it --rm <your-image-name>```

If you wish to interact with the container via bash shell, run:  
```docker run -it --rm <your-image-name> /bin/bash```

### Directory structure
