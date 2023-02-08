const dotenv = require('dotenv');
const { Client } = require('pg');
const axios = require('axios');
const geocoding = require('reverse-geocoding-google');

dotenv.config();

const client = new Client({
  host: process.env.POSTGRES_HOST,
  port: process.env.POSTGRES_PORT,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DATABASE
});

try {
  client.connect((err) => {
    if (err) {
      console.error('Connection Error', err.stack);
      process.exit(1);
    } else {
      console.log('Connected to database!');
    }
  })} catch(e) {
  console.log('Error', e);
}

(async () => {
  const res = await client.query('SELECT DISTINCT geo_link FROM tweets_depremaddress WHERE geo_link IS NOT NULL');
  const existing_links = await client.query('SELECT geo_link FROM geo_location WHERE geo_link IS NOT NULL');
  const existing_geo_links = existing_links.rows.map(x => x.geo_link);
  const links = res.rows.map(x => x.geo_link ).filter((x, i, s) =>  {
    return x !== undefined &&
        x !== '' &&
        x.length > 0 &&
        x.includes('goo') &&
        !existing_geo_links.includes((x));
  });

  for(let geo_link of links) {
    const res2 = await axios.get(geo_link).catch(() => {
      console.log(`Failed to resolve ${geo_link}`);
    });

    if(!res2) {
      continue;
    }

    let latLngLink = res2.request.res.responseUrl.split('@')[1]?.split(',');

    let lat = latLngLink?.[0]
    let lon = latLngLink?.[1]

    if(lat && lon) {
      geocoding.location({
        latitude: lat,
        longitude: lon,
        key: process.env.GOOGLE_API_KEY
      }, async (err, data) => {
        if (!err) {
          const addr = data.results[0].address_components;
          const il = addr.filter(x => x.types.includes("administrative_area_level_1"))[0]?.short_name;
          const ilce = addr.filter(x => x.types.includes("administrative_area_level_2"))[0]?.short_name;
          const mahalle = addr.filter(x => x.types.includes("administrative_area_level_4"))[0]?.short_name;
          const sokak = addr.filter(x => x.types.includes("route"))[0]?.short_name;
          const numara = addr.filter(x => x.types.includes("street_number"))[0]?.short_name;

          const toInsert = {
            geo_link: geo_link,
            latitude: lat,
            longitude: lon,
            il: il,
            ilce: ilce,
            mahalle: mahalle,
            sokak: sokak,
            numara: numara
          }

          const insertStmt = `INSERT INTO geo_location(${Object.keys(toInsert).join(', ')}) VALUES($1, $2, $3, $4, $5, $6, $7, $8)`;

          const insertQuery = {
            text: insertStmt,
            values: Object.values(toInsert),
          }

          await client.query(insertQuery);

          console.log(`Processed link ${geo_link}, address: ${data.results[0].formatted_address}`);
        }
      });
    } else {
      const addr = decodeURIComponent(res2.request.res.responseUrl.split('?')[1].split('&')[0]?.replace(/\+/g, ' ')).replace('q=', '');
      if(addr !== 'utm_source=mstt_1' && addr !== 'shorturl=1') {
        console.log(`Tried to resolve ${geo_link}, but inserting ${addr}`);
        const insertStmt = `INSERT INTO geo_location(geo_link, mahalle) VALUES ($1, $2)`;
        const insertQuery = {
          text: insertStmt,
          values: [geo_link, addr],
        }
        await client.query(insertQuery);
      }
    }
  }
  process.exit(0);
})();
