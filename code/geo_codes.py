from geopy.geocoders import HereV7
import openrouteservice
import Configuration as c

def geocode_p(address):
    """
    example:
        location = geocode_p('אחד העם 17, פתח תקווה')
        print(location.latitude, location.longitude)

    """
    geolocator = HereV7(apikey=c.geocode_api_key)
    location = geolocator.geocode(address)
    return location

def get_distance(lat1, lon1, lat2, lon2):
    client = openrouteservice.Client(key=c.distance_api_key)
    coords = ((lon1, lat1), (lon2, lat2))
    route = client.directions(coords)
    return route.get('routes')[0].get('summary').get('distance')