const express = require('express')
const app = express()
const path = require('path')
const bodyParser = require('body-parser');
const sqlite3 = require('sqlite3').verbose();

const db_name = path.resolve(__dirname, "dbs/vuelos.db");

console.log(db_name)

let db = new sqlite3.Database(db_name, (err) => {
  if (err) {
    return console.error(err.message);
  } else {
    console.log('Connected to the database.');
  }
});

const sql_create = "CREATE TABLE IF NOT EXISTS Clusters(cluster_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," +
    "cluster_name VARCHAR(30) NOT NULL, cluster_img VARCHAR(100) NOT NULL)"

const sql_create_inercia = "CREATE TABLE IF NOT EXISTS Inercias(inercia_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," +
    "inercia_img VARCHAR(100) NOT NULL)"



const port = 3002

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}));


app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
  next();
});


app.get('/api/rutas', (req, res) => {
  sql_rutas = "SELECT DISTINCT Origin, Dest FROM vuelos;";
  db.all(sql_rutas, function (err, rows) {
    if(err){
      console.log(err);
    }else{
      rutas = rows;
      res.status(200).send(rutas);
    }
  });
})

app.get('/api/aerolineas', (req, res) => {
  sql_aerolineas = "SELECT DISTINCT UniqueCarrier FROM vuelos;";
  db.all(sql_aerolineas, function (err, rows) {
    if(err){
      console.log(err);
    }else{
      aerolineas = rows;
      res.status(200).send(aerolineas);
    }
  });
})

// ***************** QUERY 1
app.get('/api/q1', (req, res) => {
  const Origin = req.query.Origin;
  const Dest = req.query.Dest;
  if(Origin && Dest){
    sql_q1 = "SELECT Origin, Dest, COUNT(Origin) as DelayCount FROM vuelos " +
        "WHERE Origin == \"" + Origin + "\" AND Dest == \"" + Dest + "\" AND ArrDelay > 0 AND DepDelay > 0 AND ArrDelay != \"NA\" AND DepDelay != \"NA\" " +
        "GROUP BY Origin, Dest;";
  } else {
    sql_q1 = "SELECT Origin, Dest, COUNT(Origin) as DelayCount FROM vuelos " +
        "WHERE ArrDelay > 0 AND DepDelay > 0 AND ArrDelay != \"NA\" AND DepDelay != \"NA\" " +
        "GROUP BY Origin, Dest;";
  }
  db.all(sql_q1, function (err, rows) {
    if(err){
      console.log(err);
    }else{
      q1 = rows;
      res.status(200).send(q1);
    }
  });
})

// ***************** QUERY 2
app.get('/api/q2', (req, res) => {
  const UniqueCarrier = req.query.UniqueCarrier;
  if(UniqueCarrier){
    sql_q2 = "SELECT UniqueCarrier, COUNT(UniqueCarrier) as DelayCount FROM vuelos " +
        "WHERE UniqueCarrier == \"" + UniqueCarrier + "\" AND ArrDelay > 0 AND DepDelay > 0 AND ArrDelay != \"NA\" AND DepDelay != \"NA\" " +
        "GROUP BY UniqueCarrier;";
  } else {
    sql_q2 = "SELECT UniqueCarrier, COUNT(UniqueCarrier) as DelayCount FROM vuelos " +
        "WHERE ArrDelay > 0 AND DepDelay > 0 AND ArrDelay != \"NA\" AND DepDelay != \"NA\" " +
        "GROUP BY UniqueCarrier;";
  }
  db.all(sql_q2, function (err, rows) {
    if(err){
      console.log(err);
    }else{
      q2 = rows;
      res.status(200).send( q2);
    }
  });
})

// ***************** QUERY 3
app.get('/api/q3', (req, res) => {
  const Origin = req.query.Origin;
  const Dest = req.query.Dest;

  if(Origin && Dest){

    sql_q3 = "SELECT Origin, Dest, COUNT(Origin) as DelayCount FROM vuelos " +
        "WHERE Origin == \"" + Origin + "\" AND Dest == \"" + Dest + "\" AND CarrierDelay > 0 AND WeatherDelay > 0 AND CarrierDelay != \"NA\" AND WeatherDelay != \"NA\" " +
        "GROUP BY Origin, Dest;";
  } else {
    sql_q3 = "SELECT Origin, Dest, COUNT(Origin) as DelayCount FROM vuelos " +
        "WHERE CarrierDelay > 0 AND WeatherDelay > 0 AND CarrierDelay != \"NA\" AND WeatherDelay != \"NA\" " +
        "GROUP BY Origin, Dest;";
  }
  db.all(sql_q3, function (err, rows) {
    if(err){
      console.log(err);
    }else{
      q3 = rows;
      res.status(200).send(q3);
    }
  });
})

// ***************** QUERY 4
app.get('/api/q4', (req, res) => {
  const CancellationCode = req.query.CancellationCode;
  if(CancellationCode){
    sql_q4 = "SELECT Origin, Dest, CancellationCode, COUNT(CancellationCode) as CancellationCount FROM vuelos " +
        "WHERE CancellationCode == \"" + CancellationCode + "\" AND CancellationCode != \"NA\" AND CancellationCode != \"\" " +
        "GROUP BY Origin, Dest;";
  } else {
    sql_q4 = "SELECT Origin, Dest, CancellationCode, COUNT(CancellationCode) as CancellationCount FROM vuelos " +
        "WHERE CancellationCode != \"NA\" AND CancellationCode != \"\" " +
        "GROUP BY Origin, Dest;";
  }

  db.all(sql_q4, function (err, rows) {
    if(err){
      console.log(err);
    }else{
      q4 = rows;
      res.status(200).send(q4);
    }
  });
})

// ***************** QUERY 5
app.get('/api/q5', (req, res) => {
  const Origin = req.query.Origin;
  const Dest = req.query.Dest;

  if(Origin && Dest){
    sql_q5 = "SELECT Origin, Dest, COUNT(Origin) as DivertedCount FROM vuelos " +
        "WHERE Diverted == \"1\" AND  Origin == \"" + Origin + "\" AND Dest == \"" + Dest + "\" " +
        "GROUP BY Origin, Dest;";
  } else {
    sql_q5 = "SELECT Origin, Dest, COUNT(Origin) as DivertedCount FROM vuelos " +
        "WHERE Diverted == \"1\"  " +
        "GROUP BY Origin, Dest;";
  }

  db.all(sql_q5, function (err, rows) {
    if(err){
      console.log(err);
    }else{
      q5 = rows;
      res.status(200).send(q5);
    }
  });
})

app.listen(port, () => {
  console.log(`Server listening at http://localhost:${port}`)
})
