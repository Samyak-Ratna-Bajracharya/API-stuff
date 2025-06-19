import requests
from datetime import datetime, timezone, timedelta
import mysql.connector


api_key = "API KEY HERE" # Replace with your actual API key


# Define the list of places to retrieve weather data for
placelist = {"country1": ["city1", "city2"],
             "country2": ["city3", "city4"],} 
# Example:
# placelist = {"USA": ["New York", "Los Angeles"], "Canada": ["Toronto", "Vancouver"]}


# Connect to MySQL
db_config = {
    "host": "host IP or name(localhost)", # Replace with your actual host IP or name
    #"port": 3306,  Default MySQL port
    "user": "Username",  # Replace with your actual MySQL username
    "password": "Password",  # Replace with your actual MySQL password
    "database": "weather_db"  # Replace with your actual database name
}

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        country VARCHAR(255),
        country_code VARCHAR(10),
        city VARCHAR(255),
        temperature VARCHAR(255),
        weather_description VARCHAR(255),
        datetime_local VARCHAR(255),
        tempMin VARCHAR(255),
        tempMax VARCHAR(255),
        humidity INT,
        wind_speed FLOAT,
        wind_direction VARCHAR(50),
        visibility VARCHAR(50),
        sea_level VARCHAR(50),
        pressure VARCHAR(50),
        clouds VARCHAR(50),
        latitude FLOAT,
        longitude FLOAT,
        timezone VARCHAR(50),
        UNIQUE(id, city, datetime_local)  -- Ensure no duplicate entries for the same city and datetime
    )
""")
conn.commit()


for country in placelist:
    for city in placelist[country]:
        request= requests.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}")
        data = request.json()
        
        print(data)
        #make items to store in db
        
        if data.get("cod") != 200:
            print(f"Error fetching data for {city}: {data.get('message', 'Unknown error')}")
            continue
        else:
            
            timezone_offset = data["timezone"]
            local_time = datetime.now(timezone.utc) + timedelta(seconds=timezone_offset)
            datetime_local_str = f"{local_time.year}-{local_time.month:02d}-{local_time.day:02d}  {local_time.hour:02d}:{local_time.minute:02d}:{local_time.second:02d}"

            store_data = {
                "country": country,
                "city": data["name"],
                "temperature": round(data["main"]["temp"] - 273.15, 2),
                "weather_description": data["weather"][0]["description"],
                "datetime_local": datetime_local_str,
                "country_code": data["sys"]["country"],
                "tempMin": round(data["main"]["temp_min"] - 273.15, 2),
                "tempMax": round(data["main"]["temp_max"] - 273.15, 2),
                "humidity": data["main"]["humidity"],
                "wind_speed": data["wind"]["speed"],
                "wind_direction": data["wind"].get("deg", "N/A"),
                "visibility": data.get("visibility", "N/A"),
                "sea_level": data["main"].get("sea_level", "N/A"),
                "pressure": data["main"].get("pressure", "N/A"),
                "clouds": data["clouds"].get("all", "N/A"),
                "latitude": data["coord"]["lat"],
                "longitude": data["coord"]["lon"],
                "timezone": data.get("timezone", "N/A")}

            #put into mysql

            cursor.execute("""
                INSERT INTO weather_data (
                    country, 
                    country_code, 
                    city, 
                    temperature, 
                    weather_description, 
                    datetime_local, 
                    tempMin, 
                    tempMax, 
                    humidity, 
                    wind_speed, 
                    wind_direction, 
                    visibility, 
                    sea_level, 
                    pressure, 
                    clouds, 
                    latitude, 
                    longitude, 
                    timezone)
                VALUES (%s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s)
                """, (
                        store_data["country"], 
                        store_data["country_code"], 
                        store_data["city"], 
                        store_data["temperature"], 
                        store_data["weather_description"], 
                        store_data["datetime_local"], 
                        store_data["tempMin"], 
                        store_data["tempMax"], 
                        store_data["humidity"], 
                        store_data["wind_speed"], 
                        store_data["wind_direction"], 
                        store_data["visibility"], 
                        store_data["sea_level"], 
                        store_data["pressure"], 
                        store_data["clouds"], 
                        store_data["latitude"], 
                        store_data["longitude"], 
                        store_data["timezone"] 
                     )
                )
            conn.commit()
            
cursor.close()
conn.close()
