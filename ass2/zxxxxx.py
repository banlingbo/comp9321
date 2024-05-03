"""
COMP9321 24T1 Assignment 2
Data publication as a RESTful service API

Getting Started
---------------
"""
from flask import Flask, request, Response, make_response, send_file
from flask_restx import Api, Resource, reqparse,fields
import sqlite3
import requests
import tempfile
from datetime import datetime,timezone,timedelta
import logging
import json
from dotenv import load_dotenv
import google.generativeai as genai
import os
from pathlib import Path

load_dotenv()
studentid = Path(__file__).stem
db_file  = f"{studentid}.db"
txt_file = f"{studentid}.txt"
genai.configure(api_key=os.environ["GOOGLE_API_KEY"])
gemini = genai.GenerativeModel('gemini-pro')

app = Flask(__name__)
api = Api(app, version='1.0', title='Tourism in Germany API', description='A smart API based on the Deutsche Bahn API')
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def init_db():
    with sqlite3.connect(db_file) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS stops (
                stop_id INTEGER PRIMARY KEY,
                name TEXT,
                latitude REAL,
                longitude REAL,
                last_updated TEXT
            )
        ''')
parser1 = reqparse.RequestParser()
parser1.add_argument('query', type=str, required=True, help='Query string to search for stops')
parser2 = reqparse.RequestParser()
parser2.add_argument('include', type=str, required=False, help='Comma-separated fields to include in the response')
stop_update = api.model('StopUpdate', {
    'name': fields.String(description='The new name of the stop'),
    'latitude': fields.Float(description='The new latitude of the stop'),
    'longitude': fields.Float(description='The new longitude of the stop'),
})
@api.route('/stops')
class StopsResource(Resource):
    @api.expect(parser1)
    def put(self):
        args = parser1.parse_args()
        query = args['query']
        if not query:
            return make_response('{"message": "Bad Request: Query parameter is required."}', 400)
        response = requests.get(f'https://v6.db.transport.rest/locations?query={query}')
        if response.status_code != 200:
            return make_response('{"message": "Not Found: No stops found with the provided query."}',response.status_code)
        stops_data = response.json()
        if not stops_data:
            return make_response('{"message": "Not Found: No stops found with the provided query."}', 404)
        response_status = 200
        try:
            with sqlite3.connect(db_file) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                new_stops_added = False
                for stop in stops_data[:5]:
                    if stop['type'] == 'stop' and 'location' in stop:
                        stop_id = stop['id']
                        name = stop.get('name', 'Unknown')
                        latitude = stop['location']['latitude']
                        longitude = stop['location']['longitude']
                        last_updated = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
                        cur.execute('SELECT 1 FROM stops WHERE stop_id = ?', (stop_id,))
                        exists = cur.fetchone()
                        cur.execute('''
                            INSERT INTO stops (stop_id, name, latitude, longitude, last_updated)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(stop_id) DO UPDATE SET
                            name=excluded.name,
                            latitude=excluded.latitude,
                            longitude=excluded.longitude,
                            last_updated=excluded.last_updated
                        ''', (stop_id, name, latitude, longitude, last_updated))

                        if not exists:
                            new_stops_added = True
                conn.commit()
                if new_stops_added:
                    response_status = 201
                cur.execute('SELECT * FROM stops ORDER BY stop_id')
                stops_list = cur.fetchall()
                serialized_stops = [
                    {
                        'stop_id': row['stop_id'],
                        'name': row['name'],
                        'latitude': row['latitude'],
                        'longitude': row['longitude'],
                        'last_updated': row['last_updated'],
                        '_links': {
                            'self': {
                                'href': f"http://{request.host}/stops/{row['stop_id']}"
                            }
                        }
                    } for row in stops_list
                ]
            json_data = json.dumps(serialized_stops)
            return Response(json_data, mimetype='application/json', status=response_status)
        except requests.exceptions.HTTPError as e:
            return make_response(
                '{"message": "Bad Request: Failed to fetch data from external API.", "error": ' + str(e) + '}', 400)
        except requests.exceptions.ConnectionError:
            return make_response('{"message": "Service Unavailable: Error in external API."}', 503)

@api.route('/stops/<int:stop_id>')
class Stop(Resource):
    @api.expect(parser2)
    def get(self, stop_id):
        args = parser2.parse_args()
        include_args = args.get('include', '')
        include_fields = include_args.split(',') if include_args else []
        for field in include_fields:
            if field not in ['name', 'latitude', 'longitude', 'last_updated', 'next_departure']:
                return make_response(json.dumps({"message": "Bad Request: Invalid fields in include parameter."}), 400)
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM stops WHERE stop_id = ?', (stop_id,))
        stop = cursor.fetchone()
        if not stop:
            conn.close()
            return make_response(json.dumps({"message": "Stop not found"}), 404)
        response_data = {"stop_id": stop[0]}
        fields_mapping = ['name', 'latitude', 'longitude','last_updated']
        for index, field in enumerate(fields_mapping, start=1):
            if not include_args or field in include_fields:
                response_data[field] = stop[index]
        if 'next_departure' in include_fields or not include_fields:
            try:
                response = requests.get(f'https://v6.db.transport.rest/stops/{stop_id}/departures')
                response.raise_for_status()
            except requests.exceptions.HTTPError:
                conn.close()
                return make_response(json.dumps({"message": "Service Unavailable: Error fetching departures from external API."}), 503)
            departures = response.json().get('departures', [])
            next_departure = None
            now = datetime.now(timezone.utc)
            for departure in departures:
                departure_time = datetime.fromisoformat(departure['plannedWhen'])
                if 'platform' in departure and departure['platform'] and now <= departure_time <= now + timedelta(minutes=120):
                    next_departure = f"Platform {departure['platform']} towards {departure['direction']}"
                    response_data['next_departure'] = next_departure
                    break
        response_data["_links"] = {"self": {"href": f"http://{request.host}/stops/{stop_id}"}}
        cursor.execute('SELECT stop_id FROM stops WHERE stop_id < ? ORDER BY stop_id DESC LIMIT 1', (stop_id,))
        prev_stop = cursor.fetchone()
        cursor.execute('SELECT stop_id FROM stops WHERE stop_id > ? ORDER BY stop_id ASC LIMIT 1', (stop_id,))
        next_stop = cursor.fetchone()
        if next_stop:
            response_data["_links"]["next"] = {"href": f"http://{request.host}/stops/{next_stop[0]}"}
        if prev_stop:
            response_data["_links"]["prev"] = {"href": f"http://{request.host}/stops/{prev_stop[0]}"}

        conn.close()
        return Response(json.dumps(response_data), mimetype='application/json', status=200)

    def delete(self, stop_id):
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM stops WHERE stop_id = ?', (stop_id,))
        stop = cursor.fetchone()
        if stop:
            cursor.execute('DELETE FROM stops WHERE stop_id = ?', (stop_id,))
            conn.commit()
            conn.close()
            return make_response(json.dumps(
                {"message": "The stop_id {} was removed from the database.".format(stop_id), "stop_id": stop_id}), 200)
        else:
            conn.close()
            return make_response(json.dumps(
                {"message": "The stop_id {} was not found in the database.".format(stop_id), "stop_id": stop_id}), 404)

    @api.expect(stop_update)
    def patch(self, stop_id):
        update_fields = request.json
        valid_fields = ['name', 'latitude', 'longitude']
        invalid_fields = [field for field in update_fields if field not in valid_fields]
        if invalid_fields:
            return {"message": f"Bad Request: Invalid or non-updatable fields provided - {invalid_fields}"}, 400
        for field in update_fields:
            if update_fields[field] is None:
                return {"message": f"Bad Request: Field values for {field} must not be empty."}, 400
        update_parts = ', '.join([f"{key} = ?" for key in valid_fields if key in update_fields])
        update_values = [update_fields[key] for key in valid_fields if key in update_fields]
        update_parts += ", last_updated = ?"
        last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_values.append(last_updated)
        update_values.append(stop_id)
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute(f'UPDATE stops SET {update_parts} WHERE stop_id = ?', update_values)
        changes = conn.total_changes
        conn.commit()
        conn.close()
        if changes == 0:
            return {"message": "Stop not found"}, 404
        response_data = {
            "stop_id": stop_id,
            "last_updated": last_updated,
            "_links": {
                "self": {
                    "href": f"http://{request.host}/stops/{stop_id}"
                }
            }
        }
        return response_data, 200


@api.route('/operator-profiles/<int:stop_id>')
class OperatorProfiles(Resource):
    def get(self, stop_id):
        try:
            departures_response = requests.get(f'https://v6.db.transport.rest/stops/{stop_id}/departures', params={'duration': 90})
            departures_response.raise_for_status()
            departures_data = departures_response.json()
            departures = departures_data['departures']
            operator_names = {departure.get('line', {}).get('operator', {}).get('name') for departure in departures if departure.get('line', {}).get('operator', {}).get('name')}
            profiles = []
            for operator_name in list(operator_names)[:5]:
                question = f"Tell me about the operator {operator_name}."
                gemini_response = gemini.generate_content(question)
                information_text = gemini_response.text if gemini_response else "No information available."
                information_text = information_text.replace('\n', ' ').replace('**', '').replace("*",'')
                profiles.append({
                    "operator_name": operator_name,
                    "information": information_text
                })
            final_response = {
                "stop_id": stop_id,
                "profiles": profiles
            }
            json_data = json.dumps(final_response, ensure_ascii=False)
            response = make_response(json_data, 200)
            response.headers["Content-Type"] = "application/json; charset=utf-8"
            return response
        except requests.exceptions.HTTPError as e:
            error_code = e.response.status_code
            if error_code == 400:
                return make_response('{"message": "Bad request."}', 400)
            elif error_code == 404:
                return make_response('{"message": "Stop not found."}', 404)
            else:
                return make_response('{"message": "An unexpected error occurred."}', error_code)
        except requests.exceptions.ConnectionError:
            return make_response('{"message": "Service unavailable."}', 503)


@api.route('/guide')
class TourismGuide(Resource):
    def build_stop_payload(self,stop_id, stop_name, latitude, longitude):
        return {
            'type': 'stop',
            'id': stop_id,
            'name': stop_name,
            'location': {
                'type': 'location',
                'latitude': latitude,
                'longitude': longitude
            }
        }
    def find_nearby_pois(self,latitude, longitude):
        response = requests.get(
            'https://v6.db.transport.rest/locations/nearby',
            params={
                'latitude': latitude,
                'longitude': longitude,
                'results': 10,
                'distance': 1000,
                'stops': 'true',
                'poi': 'true'
            }
        )
        # Parses the response and filters out items with poi=true
        poi_items = [item for item in response.json() if item.get('poi') is True]
        if not poi_items:
            return False
        poi_names = [item['name'] for item in poi_items]
        return poi_names

    def extract_origin_destination(journey_data):
        # Extract the first leg of non-walking as the starting point
        origin_leg = next((leg for leg in journey_data['legs'] if 'walking' not in leg.get('mode', '').lower()), None)
        # Extract the last leg of the non-walk as the end point
        destination_leg = next(
            (leg for leg in reversed(journey_data['legs']) if 'walking' not in leg.get('mode', '').lower()), None)

        if origin_leg and destination_leg:
            origin_name = origin_leg['origin']['name']
            destination_name = destination_leg['destination']['name']
            return origin_name, destination_name
        else:
            return None, None

    def check_journey_exists(self, from_id, to_id):
        params = {
            'from': from_id,
            'to': to_id
        }
        response = requests.get(
            'https://v6.db.transport.rest/journeys',
            params=params
        )
        if response.status_code == 200:
            data = response.json()
            if data.get('journeys'):
                return True, data['journeys'][0]
            else:
                return False, None
        else:
            print(f"API call failed with status code {response.status_code}")
            return False, None

    def get(self):
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT stop_id, name, latitude, longitude FROM stops')
            stops = cursor.fetchall()
        for i, from_stop in enumerate(stops):
            for to_stop in stops[i+1:]:
                has_journey, journey_data = self.check_journey_exists(from_stop[0],to_stop[0])
                if has_journey:
                    # Obtain POI names using the location of the start and end stops in the trip data
                    poi_names_o = self.find_nearby_pois(from_stop[2], from_stop[3])
                    poi_names_d = self.find_nearby_pois(to_stop[2], to_stop[3])
                    #If there are POIs near the start and end points, then send a request to the Gemini API
                    if poi_names_o and poi_names_d:
                        question = f"Create a guide for a journey from {from_stop[1]} to {to_stop[1]}, " \
                                   f"including points of interest like {', '.join(poi_names_o)} and {', '.join(poi_names_d)}."
                        response = gemini.generate_content(question)
                        guide_content = response.text
                        temp_dir = tempfile.gettempdir()
                        guide_file_name = f"{studentid}.txt"
                        guide_file_path = os.path.join(temp_dir, guide_file_name)

                        with open(guide_file_path, 'w', encoding='utf-8') as file:
                            file.write(guide_content)

                        return send_file(guide_file_path, as_attachment=True, download_name=guide_file_name)
                    else:
                        return {'message': 'Failed to generate content with Gemini API'}, 500

        return {'message': 'No valid journey or POIs found between stops'}, 404





if __name__ == '__main__':
    init_db()
    app.run(debug=True)
