-- COMMAND TO EXECUTE THE SCRIPT
-- source /home/laurent/docker/airflight_project/Airflight-Simulator/database/build_db.sql
-- DROP DATABASE AIRFLIGHT_DB;

-- Create the database
CREATE DATABASE AIRFLIGHT_DB;

-- Give Access
GRANT SELECT, INSERT, UPDATE, DELETE ON AIRFLIGHT_DB.* TO 'laurent'@'localhost';

-- Switch to the new database
USE AIRFLIGHT_DB;

-- Create the Company table
CREATE TABLE Company (
    CompanyID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Country VARCHAR(255) NOT NULL,
    IATACode CHAR(3) UNIQUE NOT NULL
);

-- Create the Company table
CREATE TABLE Company_Income (
    IncomeID INT AUTO_INCREMENT PRIMARY KEY,
    Income FLOAT NOT NULL,
    TransactionDate DATETIME NOT NULL,
    Topic VARCHAR(255) NOT NULL,
    CompanyID INT NOT NULL,
    FOREIGN KEY (CompanyID) REFERENCES Company(CompanyID) ON DELETE CASCADE
);

-- Create the Plane table
CREATE TABLE Airport (
    AirportID INT AUTO_INCREMENT PRIMARY KEY,
    AirportCode CHAR(3) NOT NULL,
    Latitude FLOAT,
    Longitude FLOAT,
    LandingPrice INT,
    AirportCountry VARCHAR(255) NOT NULL
);

-- Create the Plane table
CREATE TABLE Plane (
    PlaneID INT AUTO_INCREMENT PRIMARY KEY,
    Model VARCHAR(255) NOT NULL,
    Manufacturer VARCHAR(255) NOT NULL,
    RangeKM INT NOT NULL,
    PassengerCapacity INT NOT NULL,
    CruisingSpeedKPH INT NOT NULL,
    WeightKG INT NOT NULL,
    TankCapacityInGallon INT NOT NULL,
    CompanyID INT NOT NULL,
    FOREIGN KEY (CompanyID) REFERENCES Company(CompanyID) ON DELETE CASCADE
);

-- Create the Flight table
CREATE TABLE Flight (
    FlightID INT AUTO_INCREMENT PRIMARY KEY,  -- Auto-increment primary key
    FlightCode VARCHAR(13) NOT NULL,
    AirportDeparture INT,  -- Foreign key referencing AirportID in the Airport table
    AirportArrival INT,    -- Foreign key referencing AirportID in the Airport table
    TimeDeparture DATETIME NOT NULL,
    TimeArrival DATETIME NOT NULL,
    Distance INT NOT NULL,
    FlightTimeMinutes INT NOT NULL,
    NbPassenger INT NOT NULL,
    PlaneID INT NOT NULL,
    FOREIGN KEY (PlaneID) REFERENCES Plane(PlaneID) ON DELETE CASCADE,
    FOREIGN KEY (AirportDeparture) REFERENCES Airport(AirportID) ON DELETE CASCADE,
    FOREIGN KEY (AirportArrival) REFERENCES Airport(AirportID) ON DELETE CASCADE
);

-- Create the Person table
CREATE TABLE Passenger (
    PassengerID INT AUTO_INCREMENT PRIMARY KEY,  -- Auto-increment primary key
    Name VARCHAR(255) NOT NULL,
    Surname VARCHAR(255) NOT NULL,
    PhoneNumber VARCHAR(10) NOT NULL,
    Mail VARCHAR(255) NOT NULL,
    Gender VARCHAR(255) NOT NULL,
    TicketPriceDollar FLOAT,
    PurchaseDate DATETIME,
    FlightID INT NOT NULL,
    FOREIGN KEY (FlightID) REFERENCES Flight(FlightID) ON DELETE CASCADE
);
/*
-- Create the Person table
CREATE TABLE Ticket (
    TicketID INT AUTO_INCREMENT PRIMARY KEY,  -- Auto-increment primary key
    PriceDollar INT NOT NULL,
    PassengerID INT NOT NULL,
    FOREIGN KEY (PassengerID) REFERENCES Passenger(PassengerID) ON DELETE CASCADE
);
*/
-- Create the Consumption table
CREATE TABLE Consumption (
    ConsumptionID INT AUTO_INCREMENT PRIMARY KEY,  -- Auto-increment primary key
    BarrelPriceDollar FLOAT NOT NULL, -- Scrap in Real time
    TotalFuelPriceDollar FLOAT NOT NULL, -- Consumption per person, km, and flight
    TotalFuelVolumeGallons FLOAT NOT NULL, -- Full Price to fill a tank plane
    FlightID INT NOT NULL,
    FOREIGN KEY (FlightID) REFERENCES Flight(FlightID) ON DELETE CASCADE
);