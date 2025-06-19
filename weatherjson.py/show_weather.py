import mysql.connector
from fastapi import FastAPI, HTTPException

database_config = {
    'host': 'host IP or name(localhost)',  # Replace with your actual host IP or name
    # 'port': 3306,  # Default MySQL port
    'user': 'Username',  # Replace with your actual MySQL username
    'password': 'Password',  # Replace with your actual MySQL password
    'database': 'weather_db'  # Replace with your actual database name
}

app = FastAPI()
@app.get("/")
def root():
    return {"message": "Hello. This is the weather data project for whatever city you input as a query parameter in ./extract/city={city}. For example, if you input 'city=London', it will return the weather data for London."}

@app.get("/country/{country}")
def get_weather_by_country(country: str):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE country = %s", (country,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if results:
        return [
            {
                "country": row[1],
                "country_code": row[2],
                "city": row[3],
                "temperature": row[4],
                "weather_description": row[5],
                "datetime_local": row[6],
                "tempMin": row[7],
                "tempMax": row[8],
                "humidity": row[9],
                "wind_speed": row[10],
                "wind_direction": row[11],
                "visibility": row[12],
                "sea_level": row[13],
                "pressure": row[14],
                "clouds": row[15],
                "latitude": row[16],
                "longitude": row[17],
                "timezone": row[18]
            } for row in results
        ]
    else:
        raise HTTPException(status_code=404, detail="Country not found")

@app.get("/city/{city}")
def get_weather(city: str):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE city = %s", (city,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    if result:
        return {
            "country": result[1],
            "country_code": result[2],
            "city": result[3],
            "temperature": result[4],
            "weather_description": result[5],
            "datetime_local": result[6],
            "tempMin": result[7],
            "tempMax": result[8],
            "humidity": result[9],
            "wind_speed": result[10],
            "wind_direction": result[11],
            "visibility": result[12],
            "sea_level": result[13],
            "pressure": result[14],
            "clouds": result[15],
            "latitude": result[16],
            "longitude": result[17],
            "timezone": result[18]
        }
    else:
        raise HTTPException(status_code=404, detail="City not found")
        
@app.get("/weather/{weather}")
def get_weather_by_description(weather: str):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE weather_description LIKE %s", (f"%{weather}%",))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if results:
        return [
            {
                "country": row[1],
                "country_code": row[2],
                "city": row[3],
                "temperature": row[4],
                "weather_description": row[5],
                "datetime_local": row[6],
                "tempMin": row[7],
                "tempMax": row[8],
                "humidity": row[9],
                "wind_speed": row[10],
                "wind_direction": row[11],
                "visibility": row[12],
                "sea_level": row[13],
                "pressure": row[14],
                "clouds": row[15],
                "latitude": row[16],
                "longitude": row[17],
                "timezone": row[18]
            } for row in results
        ]
    else:
        raise HTTPException(status_code=404, detail="Weather description not found")

@app.get("/temperature/{temp}")
def get_weather_by_temperature(temp: float):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE temperature = %s", (temp,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if results:
        return [
            {
                "country": row[1],
                "country_code": row[2],
                "city": row[3],
                "temperature": row[4],
                "weather_description": row[5],
                "datetime_local": row[6],
                "tempMin": row[7],
                "tempMax": row[8],
                "humidity": row[9],
                "wind_speed": row[10],
                "wind_direction": row[11],
                "visibility": row[12],
                "sea_level": row[13],
                "pressure": row[14],
                "clouds": row[15],
                "latitude": row[16],
                "longitude": row[17],
                "timezone": row[18]
            } for row in results
        ]
    else:
        raise HTTPException(status_code=404, detail="Temperature not found")
    
@app.get("/humidity/{humidity}")
def get_weather_by_humidity(humidity: int):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE humidity = %s", (humidity,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if results:
        return [
            {
                "country": row[1],
                "country_code": row[2],
                "city": row[3],
                "temperature": row[4],
                "weather_description": row[5],
                "datetime_local": row[6],
                "tempMin": row[7],
                "tempMax": row[8],
                "humidity": row[9],
                "wind_speed": row[10],
                "wind_direction": row[11],
                "visibility": row[12],
                "sea_level": row[13],
                "pressure": row[14],
                "clouds": row[15],
                "latitude": row[16],
                "longitude": row[17],
                "timezone": row[18]
            } for row in results
        ]
    else:
        raise HTTPException(status_code=404, detail="Humidity not found")
    
@app.get("/wind_speed/{wind_speed}")
def get_weather_by_wind_speed(wind_speed: float):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE wind_speed = %s", (wind_speed,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if results:
        return [
            {
                "country": row[1],
                "country_code": row[2],
                "city": row[3],
                "temperature": row[4],
                "weather_description": row[5],
                "datetime_local": row[6],
                "tempMin": row[7],
                "tempMax": row[8],
                "humidity": row[9],
                "wind_speed": row[10],
                "wind_direction": row[11],
                "visibility": row[12],
                "sea_level": row[13],
                "pressure": row[14],
                "clouds": row[15],
                "latitude": row[16],
                "longitude": row[17],
                "timezone": row[18]
            } for row in results
        ]
    else:
        raise HTTPException(status_code=404, detail="Wind speed not found")
    
@app.get("/wind_direction/{wind_direction}")
def get_weather_by_wind_direction(wind_direction: str):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE wind_direction = %s", (wind_direction,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if results:
        return [
            {
                "country": row[1],
                "country_code": row[2],
                "city": row[3],
                "temperature": row[4],
                "weather_description": row[5],
                "datetime_local": row[6],
                "tempMin": row[7],
                "tempMax": row[8],
                "humidity": row[9],
                "wind_speed": row[10],
                "wind_direction": row[11],
                "visibility": row[12],
                "sea_level": row[13],
                "pressure": row[14],
                "clouds": row[15],
                "latitude": row[16],
                "longitude": row[17],
                "timezone": row[18]
            } for row in results
        ]
    else:
        raise HTTPException(status_code=404, detail="Wind direction not found")
    
@app.get("/visibility/{visibility}")
def get_weather_by_visibility(visibility: int):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE visibility = %s", (visibility,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if results:
        return [
            {
                "country": row[1],
                "country_code": row[2],
                "city": row[3],
                "temperature": row[4],
                "weather_description": row[5],
                "datetime_local": row[6],
                "tempMin": row[7],
                "tempMax": row[8],
                "humidity": row[9],
                "wind_speed": row[10],
                "wind_direction": row[11],
                "visibility": row[12],
                "sea_level": row[13],
                "pressure": row[14],
                "clouds": row[15],
                "latitude": row[16],
                "longitude": row[17],
                "timezone": row[18]
            } for row in results
        ]
    else:
        raise HTTPException(status_code=404, detail="Visibility not found")
    
@app.get("/sea_level/{sea_level}")
def get_weather_by_sea_level(sea_level: int):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE sea_level = %s", (sea_level,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    if results:
        return [
            {
                "country": row[1],
                "country_code": row[2],
                "city": row[3],
                "temperature": row[4],
                "weather_description": row[5],
                "datetime_local": row[6],
                "tempMin": row[7],
                "tempMax": row[8],
                "humidity": row[9],
                "wind_speed": row[10],
                "wind_direction": row[11],
                "visibility": row[12],
                "sea_level": row[13],
                "pressure": row[14],
                "clouds": row[15],
                "latitude": row[16],
                "longitude": row[17],
                "timezone": row[18]
            } for row in results
        ]
    else:
        raise HTTPException(status_code=404, detail="Sea level not found")
    
@app.get("/pressure/{pressure}")
def get_weather_by_pressure(pressure: int):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE pressure = %s", (pressure,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    if results:
        return [
            {
                "country": row[1],
                "country_code": row[2],
                "city": row[3],
                "temperature": row[4],
                "weather_description": row[5],
                "datetime_local": row[6],
                "tempMin": row[7],
                "tempMax": row[8],
                "humidity": row[9],
                "wind_speed": row[10],
                "wind_direction": row[11],
                "visibility": row[12],
                "sea_level": row[13],
                "pressure": row[14],
                "clouds": row[15],
                "latitude": row[16],
                "longitude": row[17],
                "timezone": row[18]
            } for row in results
        ]
    else:
        raise HTTPException(status_code=404, detail="Pressure not found")
    
@app.get("/clouds/{clouds}")
def get_weather_by_clouds(clouds: int):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE clouds = %s", (clouds,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    if results:
        return [
            {
                "country": row[1],
                "country_code": row[2],
                "city": row[3],
                "temperature": row[4],
                "weather_description": row[5],
                "datetime_local": row[6],
                "tempMin": row[7],
                "tempMax": row[8],
                "humidity": row[9],
                "wind_speed": row[10],
                "wind_direction": row[11],
                "visibility": row[12],
                "sea_level": row[13],
                "pressure": row[14],
                "clouds": row[15],
                "latitude": row[16],
                "longitude": row[17],
                "timezone": row[18]
            } for row in results
        ]
    else:
        raise HTTPException(status_code=404, detail="Clouds not found")
    
@app.get("/coordinates/{latitude}/{longitude}")
def get_weather_by_coordinates(latitude: float, longitude: float):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE latitude = %s AND longitude = %s", (latitude, longitude))
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    if results:
        return [
            {
                "country": row[1],
                "country_code": row[2],
                "city": row[3],
                "temperature": row[4],
                "weather_description": row[5],
                "datetime_local": row[6],
                "tempMin": row[7],
                "tempMax": row[8],
                "humidity": row[9],
                "wind_speed": row[10],
                "wind_direction": row[11],
                "visibility": row[12],
                "sea_level": row[13],
                "pressure": row[14],
                "clouds": row[15],
                "latitude": row[16],
                "longitude": row[17],
                "timezone": row[18]
            } for row in results
        ]
    else:
        raise HTTPException(status_code=404, detail="Coordinates not found")
    
@app.get("/timezone/{timezone}")
def get_weather_by_timezone(timezone: str):
    conn = mysql.connector.connect(**database_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data WHERE timezone = %s", (timezone,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    if results:
        return [
            {
                "country": row[1],
                "country_code": row[2],
                "city": row[3],
                "temperature": row[4],
                "weather_description": row[5],
                "datetime_local": row[6],
                "tempMin": row[7],
                "tempMax": row[8],
                "humidity": row[9],
                "wind_speed": row[10],
                "wind_direction": row[11],
                "visibility": row[12],
                "sea_level": row[13],
                "pressure": row[14],
                "clouds": row[15],
                "latitude": row[16],
                "longitude": row[17],
                "timezone": row[18]
            } for row in results
        ]
    else:
        raise HTTPException(status_code=404, detail="Timezone not found")
    
# To run the FastAPI app, use the command: uvicorn show_weather:app --reload
