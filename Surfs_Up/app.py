from flask import Flask, jsonify
import numpy as np
import pandas as pd
import datetime as dt
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc

# Please note that we create a session for every page to avoid problems with sqlalchemy threads

app = Flask(__name__)

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Function from Jupyter Notebook
def calc_temps(start_date, end_date):

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """

    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).filter(
            Measurement.date <= end_date).all()


#Begin app
@app.route("/")
def home():
    return f'''
            <h1>Surfs Up API</h1>
            <h2>Version 1.0</h2>
            <hr>
            <h4>Available routes:</h4>
            <ul>
                <li>/api/v1.0/precipitation</li>
                <ul>
                    <li>Returns last 12 months of precipitation data</li>
                </ul>

                <br>

                <li>/api/v1.0/stations</li>
                <ul>
                    <li>Returns all available stations, and how many data points they contributed</li>
                </ul>

                <br>

                <li>/api/v1.0/tobs</li>
                <ul>
                    <li>Returns last 12 months of temperature data</li>
                </ul>

                <br>

                <li>/api/v1.0/start_date</li>
                <ul>
                    <li>Returns minimum, average, and the max temperature for all dates greater than and equal to the start date</li>
                </ul>

                <br>

                <li>/api/v1.0/start_date/end_date</li>
                <ul>
                    <li>Returns minimum, average, and the max temperature for all dates between the start and end date</li>
                </ul>
            </ul>
            <hr>
            <h3>Please note that dates have to be written in the format YYYY-MM-DD for the functions to work.</h3>'''
            


@app.route("/api/v1.0/precipitation")
def precipitation():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Get the last item (furtherst date)
    last_item = session.query(Measurement).order_by(Measurement.date.desc()).first()

    # Get the date that was exactly a year before the date of the last item
    date = dt.datetime.strptime(last_item.date, "%Y-%m-%d") - dt.timedelta(days=365)


    # Perform a query to retrieve the data and precipitation scores

    query = session.query(Measurement).filter(Measurement.date >= date)

    # Save the query results as a Pandas DataFrame and set the index to the date column

    data = pd.DataFrame({"Date": [], "Precipitation": []})

    for result in query:
        data = data.append(
            {"Date": result.date, "Precipitation": result.prcp}, ignore_index=True)

    # Sort the dataframe by date
    data = data.sort_values(by="Date")

    return jsonify(data.to_dict(orient="records"))


@app.route("/api/v1.0/stations")
# Return the station id's and the number of datapoints they took
def stations():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    query = session.query(Measurement.station, func.count(Measurement.station).label('qty')).group_by(Measurement.station).order_by(desc('qty'))

    data = pd.DataFrame({"Station": [], "Count": []})

    for result in query:
        data = data.append(
            {"Station": result.station, "Count": result.qty}, ignore_index=True)

    return jsonify(data.to_dict(orient="records"))


@app.route("/api/v1.0/tobs")
def tobs():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Get the last item (furtherst date)
    last_item = session.query(Measurement).order_by(
        Measurement.date.desc()).first()

    # Get the date that was exactly a year before the date of the last item
    date = dt.datetime.strptime(
        last_item.date, "%Y-%m-%d") - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores

    query = session.query(Measurement).filter(Measurement.date >= date)

    # Save the query results as a Pandas DataFrame and set the index to the date column

    data = pd.DataFrame({"Date": [], "Tobs": []})

    for result in query:
        data = data.append(
            {"Date": result.date, "Tobs": result.tobs}, ignore_index=True)

    # Sort the dataframe by date
    data = data.sort_values(by="Date")

    return jsonify(data.to_dict(orient="records"))


@app.route("/api/v1.0/<start_date>")
def start_only(start_date):

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Get the last item (furtherst date)
    last_item = session.query(Measurement).order_by(Measurement.date.desc()).first()

    data = calc_temps(start_date, last_item.date)[0]

    return jsonify({"Tmin": data[0], "Tavg": data[1], "Tmax": data[2]})


@app.route("/api/v1.0/<start_date>/<end_date>")
def start_end(start_date, end_date):

    # Create our session (link) from Python to the DB
    session = Session(engine)

    data = calc_temps(start_date, end_date)[0]

    return jsonify({"Tmin": data[0], "Tavg": data[1], "Tmax": data[2]})


if __name__ == "__main__":
    app.run(debug=True)
