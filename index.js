const express = require('express');
const pg = require('pg');

const app = express();
require('dotenv').config();

const pool = new pg.Pool();

const queryHandler = (req, res, next) => {
  pool.query(req.sqlQuery).then((r) => {
    return res.json(r.rows || [])
  }).catch(next)
}

app.listen(process.env.PORT || 5555, (err) => {
  if (err) {
    console.error(err)
    process.exit(1)
  } else {
    console.log(`Running on ${process.env.PORT || 5555}`)
  }
})

app.get('/', (req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.send('Welcome to EQ Works 😎');
})

app.get('/events/hourly', (req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  req.sqlQuery = `
    SELECT date, hour, events
    FROM public.hourly_events
    ORDER BY date, hour
    LIMIT 50;
  `
  return next()
}, queryHandler)

app.get('/events/daily', (req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  req.sqlQuery = `
    SELECT date, SUM(events) AS events
    FROM public.hourly_events
    GROUP BY date
    ORDER BY date
    LIMIT 7;
  `
  return next()
}, queryHandler)

app.get('/stats/hourly', (req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  req.sqlQuery = `
    SELECT poi_id, date, hour, impressions, clicks, revenue
    FROM public.hourly_stats
    ORDER BY date, hour
    LIMIT 168;
  `
  return next()
}, queryHandler)

app.get('/stats/daily', (req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  req.sqlQuery = `
    SELECT date,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(revenue) AS revenue
    FROM public.hourly_stats
    GROUP BY date
    ORDER BY date
    LIMIT 10;
  `
  return next()
}, queryHandler)

app.get('/poi', (req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  req.sqlQuery = `
  SELECT *
  FROM public.poi;
  `
  return next()
}, queryHandler)

app.get('/join/poi/stats_hourly', (req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  req.sqlQuery = `
  SELECT DISTINCT *
  FROM public.poi, public.hourly_stats
  WHERE public.poi.poi_id = public.hourly_stats.poi_id
  LIMIT 10;
  `
  return next()
}, queryHandler)



// last resorts
process.on('uncaughtException', (err) => {
  console.log(`Caught exception: ${err}`)
  process.exit(1)
})
process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason)
  process.exit(1)
})
