
import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///C:/Users/Albrecht/Desktop/BC Class Repo/git_hub/sqlalchemy-challenge/SurfsUp/Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
Session = sessionmaker(bind=engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    # Define the available routes
    return (
        "Available Routes:<br/>"
        "/api/v1.0/precipitation<br/>"
        "/api/v1.0/stations<br/>"
        "/api/v1.0/tobs<br/>"
        "/api/v1.0/start_date/YYYY-MM-DD<br/>"
        "/api/v1.0/start_date/YYYY-MM-DD/end_date/YYYY-MM-DD<br/>"
    )

# Define route for precipitation data
@app.route('/api/v1.0/precipitation')
def precipitation():
    # Create a new session instance
    session_instance = Session()
    try:
        # Query precipitation data from measurements table
        results = session_instance.query(Measurement.date, Measurement.prcp).all()
        precipitation_data = {date: prcp for date, prcp in results}
    except Exception as e:
        return jsonify({'error': str(e)})
    finally:
        # Close the session
        session_instance.close()
    
    return jsonify(precipitation_data)

# Define route for station data
@app.route('/api/v1.0/stations')
def stations():
    session_instance = Session()
    try:
        # Query station data from station table
        station_data = session_instance.query(Station.station, Station.name).all()
        result = [{'station': station, 'name': name} for station, name in station_data]
    except Exception as e:
        return jsonify({'error': str(e)})
    finally:
        session_instance.close()
    
    return jsonify(result)

@app.route('/api/v1.0/tobs')
def tobs():
    session_instance = Session()
    try:
        # Query most active station
        most_active_station = session_instance.query(Measurement.station)\
            .group_by(Measurement.station)\
            .order_by(func.count(Measurement.id).desc()).first()[0]
        
        # Calculate the date for the last year of data
        last_date = dt.datetime.strptime('2017-08-18', '%Y-%m-%d')
        one_year_ago = last_date - dt.timedelta(days=365)
        
        # Query temp observation data from measurements table
        results = session_instance.query(Measurement.date, Measurement.tobs)\
            .filter(Measurement.station == most_active_station)\
            .filter(Measurement.date >= one_year_ago).all()
        
        tobs_data = {str(date): tobs for date, tobs in results}
    
    except Exception as e:
        return jsonify({'error': str(e)})
    finally:
        session_instance.close()

    return jsonify(tobs_data)

# Define route to temp stats from a given start date
@app.route('/api/v1.0/start_date/<string:start_date>')
def temp_stats_start(start_date):
    session_instance = Session()
    try:
        # Convert start date string to datetime object
        start_date_obj = dt.datetime.strptime(start_date, '%Y-%m-%d')
 
        # Query temperature stats from Measurements table
        results = session_instance.query(
            func.min(Measurement.tobs),
            func.avg(Measurement.tobs),
            func.max(Measurement.tobs)
        ).filter(Measurement.date >= start_date_obj).all()
 
        # Create a dictionary with temperature stats
        temp_stats = {
            'start_date': start_date,
            'min_temperature': results[0][0],
            'avg_temperature': results[0][1],
            'max_temperature': results[0][2]
        }
 
        return jsonify(temp_stats)
 
    except Exception as e:
        return jsonify({'error': str(e)})
 
    finally:
        session_instance.close()
 
# Define route to get temperature stats between a given start and end date
@app.route('/api/v1.0/start_date/<start_date>/end_date/<end_date>')
def temp_stats_start_end(start_date, end_date):
    session_instance = Session()
    try:
        # Convert start and end dates strings to datetime objects
        start_date_obj = dt.datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = dt.datetime.strptime(end_date, '%Y-%m-%d')
 
        # Query temperature stats from Measurements table
        results = session_instance.query(
            func.min(Measurement.tobs),
            func.avg(Measurement.tobs),
            func.max(Measurement.tobs)
        ).filter(Measurement.date >= start_date_obj, Measurement.date <= end_date_obj).all()
 
        # Create a dictionary with temperature stats
        temp_stats = {
            'start_date': start_date,
            'end_date': end_date,
            'min_temperature': results[0][0],
            'avg_temperature': results[0][1],
            'max_temperature': results[0][2]
        }
 
        return jsonify(temp_stats)
 
    except Exception as e:
        return jsonify({'error': str(e)})
 
    finally:
        session_instance.close()
 
# Run the Flask application if executed as the main script
if __name__ == '__main__':
    app.run(debug=True)

