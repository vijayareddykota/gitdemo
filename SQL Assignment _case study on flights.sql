create database if not exists flights_db;

use flights_db;

-- 1.Create a Table Flights with schemas of Table.

create table if not exists flight_db(ID INT, YEAR INT, MONTH INT, DAY INT, DAY_OF_WEEK INT, AIRLINE VARCHAR(20), 
FLIGHT_NUMBER INT, TAIL_NUMBER VARCHAR(30),
ORIGIN_AIRPORT VARCHAR(20), DESTINATION_AIRPORT VARCHAR(20), SCHEDULED_DEPARTURE INT, DEPARTURE_TIME INT,
 DEPARTURE_DELAY INT, TAXI_OUT INT, WHEELS_OFF INT, SCHEDULED_TIME INT, ELAPSED_TIME INT, AIR_TIME INT,
 DISTANCE INT, WHEELS_ON INT, TAXI_IN INT, SCHEDULED_ARRIVAL INT, ARRIVAL_TIME INT, ARRIVAL_DELAY INT, 
 DIVERTED INT, CANCELLED INT, CANCELLATION_REASON VARCHAR(10),AIR_SYSTEM_DELAY INT, SECURITY_DELAY INT, 
 AIRLINE_DELAY INT, LATE_AIRCRAFT_DELAY INT, WEATHER_DELAY INT, primary key (ID));

SET GLOBAL local_infile = true;

-- 2.Insert all records into flights table. Use dataset Flights_Delay.csv
-- Write a MySQL Queries to display the results.



LOAD DATA LOCAL INFILE 'E:\Flights_Delay.csv' INTO 
TABLE flight_db FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' IGNORE 1 ROWS;

 select * from  flight_db;
 
-- 3.Average Arrival delay caused by airlines ?
select airline,avg(Arrival_delay) as Avg_Arrival_Delay from flight_db group by airline order by Avg_Arrival_Delay;
-- 4.	Display the Day of Month with AVG Delay [Hint: Add Count() of Arrival & Departure Delay]
select month,day,(departure_delay + arrival_delay)/2 as average_delay from flight_db group by month,day order by month;

-- 5.	Analysis for each month with total number of cancellations
select month,sum(cancelled)as no_of_cancellation from flight_db group by month order by no_of_cancellation desc;

-- 6.Find the airlines that make maximum number of cancellations
select AIRLINE, CANCELLED, count(*) as COUNT_FLY from flight_db group by AIRLINE, CANCELLED ORDER BY COUNT_FLY DESC;

-- 7.Finding the Busiest Airport [Hint: Find Count() of origin airport and destination airport]
select ORIGIN_AIRPORT,DESTINATION_AIRPORT, count(*) as Busiest_Airport from flight_db group by ORIGIN_AIRPORT,DESTINATION_AIRPORT order by Busiest_Airport desc;

-- 8.	Find the airlines that make maximum number of Diversions [Hint: Diverted = 1 indicate Diversion]
select AIRLINE, DIVERTED, count(*) as COUNT_MAX from flight_db group by AIRLINE, DIVERTED ORDER BY COUNT_MAX DESC;

-- 9.	Finding all diverted Route from a source to destination Airport & which route is the most diverted route.
select  DIVERTED,DESTINATION_AIRPORT, count(*) as COUNT_MAX from flight_db group by DESTINATION_AIRPORT,DIVERTED ORDER BY COUNT_MAX DESC;

-- 10.Finding all Route from origin to destination Airport & which route got delayed. 
select DIVERTED, ORIGIN_AIRPORT,DESTINATION_AIRPORT, count(DIVERTED) as COUNT_MAX from flight_db group by DIVERTED,DESTINATION_AIRPORT,ORIGIN_AIRPORT ORDER BY COUNT_MAX DESC;

-- 11.Finding the Route which Got Delayed the Most [Hint: Route include Origin Airport and Destination Airport, Group By Both ]
SELECT ORIGIN_AIRPORT,DESTINATION_AIRPORT ,SUM(ARRIVAL_DELAY) AS MOST FROM flight_db group by ORIGIN_AIRPORT,DESTINATION_AIRPORT order by MOST DESC;