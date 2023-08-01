# Import the dependencies.

from flask import Flask, jsonify
import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

#################################################
# Database Setup
#################################################
engine = create_engine('sqlite:///Resources/hawaii.sqlite')

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################


app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    return (
        f"Welcome to the Surfs Up API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end<br/>"
        f"Dates must be in format MMDDYYYY"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Precipitation queries
    last_date = dt.date(2017, 8, 23)
    one_year_ago = last_date - dt.timedelta(days= 365)
    last_year_query = session.query(measurement.date, measurement.prcp).filter(measurement.date >= one_year_ago).all()
    df = pd.DataFrame(last_year_query, columns=['Date','Precipitation (inches)'])
    df = df.sort_values('Date')
    df.set_index('Date', inplace= True)
    # Create the dictionary to later jsonify
    precipitation_dict = df.to_dict()
    session.close()
    return jsonify(precipitation_dict)

@app.route("/api/v1.0/stations")
def stations():
    stations = session.query(station.station).group_by(station.station).all()
    station_df = pd.DataFrame(stations)
    station_dict = station_df.to_dict()
    session.close()
    return jsonify(station_dict)

@app.route("/api/v1.0/tobs")
def tobs():
    last_date_519281 = dt.date(2017,8,18)
    past_year_519281 = last_date_519281 - dt.timedelta(days=365)
    past_year_query_519281 = session.query(measurement.date, measurement.tobs).\
        filter(measurement.date >= past_year_519281, measurement.station == 'USC00519281').all()
    df_519281 = pd.DataFrame(past_year_query_519281, columns=['Date','Temperature'])
    df_519281 = df_519281.sort_values('Date')
    df_519281.set_index('Date', inplace= True)
    dict_519281 = df_519281.to_dict()

    session.close()
    return jsonify(dict_519281)

@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def start_end(start=None,end=None):
    sel = [func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)]

    if not end:
        start = dt.datetime.strptime(str(start), "%m%d%Y")
        results = session.query(*sel).\
            filter(measurement.date >= start).all()

        session.close()

        temps = list(np.ravel(results))
        return jsonify(temps=temps)

    start = dt.datetime.strptime(str(start), "%m%d%Y")
    end = dt.datetime.strptime(str(end),"%m%d%Y")
    results = session.query(*sel).\
        filter(measurement.date >= start).\
        filter(measurement.date <= end).all()

    session.close()

    temps = list(np.ravel(results))
    return jsonify(temps=temps)

if __name__ == "__main__":
    app.run(debug=True)