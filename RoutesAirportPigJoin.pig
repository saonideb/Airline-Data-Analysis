routes = load 'routes.dat.csv' using PigStorage(',');
airports = load 'airports.dat.csv' using PigStorage(',');
routes_airport = JOIN routes BY $3, airports BY $0;
STORE routes_airport INTO 'RouteAirportPigJoin';

