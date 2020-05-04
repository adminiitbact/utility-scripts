const mysql = require('mysql');
const axios = require('axios');
const stream = require('stream');

// dashboard requires these values as number instaed of text
var NUMERIC_TYPES = ["total_beds",
"covid_icu_beds",
"total_icu_beds",
"total_ventilators",
"total_icu_beds_occupied",
"ventilators_allocated_covid"];
var connection = mysql.createConnection({
  host     : process.env.DB_HOST,
  user     : process.env.DB_USER,
  password : process.env.DB_PASS,
  database : process.env.DB_NAME,
  typeCast: function (field, next) {
    //Buffer types need to converted to their real values
    if (field.type === 'BIT' && field.length === 1) {
      return (field.string() === '1'); // 1 = true, 0 = false
    } else if(NUMERIC_TYPES.includes(field.name)) {
        return Number(field.string());
    } else if(field.name === 'government_hospital') {
      return (field.string() === '1')?'government': 'private';
    } else {
      return next();
    }
  }
});

function pushData(data) {
    axios.post(process.env.PUSH_URI, data)
    .then(res => {
      console.log(`status: ${res.status}`)
    })
    .catch(error => {
      console.error(error)
    })
}
const CHUNK_PUSH_SIZE=10;

connection.connect();
// query being used for dashboard data
const query = "SELECT temp1.*, IFNULL(temp2.`mild-suspected-total`,0) `mild_suspected_total`, IFNULL(temp2.`mild-suspected-occupied`,0)`mild_suspected_occupied`, IFNULL(temp2.`mild-confirmed-total`,0)`mild_confirmed_total`, IFNULL(temp2.`mild-confirmed-occupied`,0)`mild_confirmed_occupied`, IFNULL(temp2.`moderate-suspected-total`,0)`moderate_suspected_total`, IFNULL(temp2.`moderate-suspected-occupied`,0)`moderate_suspected_occupied`, IFNULL(temp2.`moderate-confirmed-total`,0)`moderate_confirmed_total`, IFNULL(temp2.`moderate-confirmed-occupied`,0)`moderate_confirmed_occupied`, IFNULL(temp2.`severe-suspected-total`,0)`severe_suspected_total`, IFNULL(temp2.`severe-suspected-occupied`,0)`severe_suspected_occupied`, IFNULL(temp2.`severe-confirmed-total`,0)`severe_confirmed_total`, IFNULL(temp2.`severe-confirmed-occupied`,0)`severe_confirmed_occupied`, IFNULL(temp2.`ventilators-earmarked-for-covid`,0)`ventilators_earmarked_for_covid`, IFNULL(temp2.`ventilator-in-use-covid`,0)`ventilator_in_use_covid`, IFNULL(temp2.`oxygen-cylinder-available`,0)`oxygen_cylinder_available`, IFNULL(temp2.`wall-oxygen-supplied-beds`,0)`wall_oxygen_supplied_beds`, IFNULL(temp2.`central-oxygen-supplied-beds`,0)`central_oxygen_supplied_beds`, IFNULL(temp2.`pulse-oximeters`,0)`pulse_oximeters`, IFNULL(temp2.`infusion-pumps`,0)`infusion_pumps`, IFNULL(temp2.`independent-beds`,0)`independent_beds` \
FROM ( \
SELECT f.facility_id,f.name,f.area,f.jurisdiction,f.institution_type,f.covid_facility_type,f.government_hospital,f.telephone,f.email,f.facility_status,f.hospital_category,f.agreement_status,f.is_seperate_entry_exit_available,f.is_fever_clinic_available,f.address,f.ulb_ward_name,f.ulb_zone_name,IF(json_type(a.`data` ->> '$.total_beds')='NULL',0,(a.`data` ->> '$.total_beds')) total_beds, IF(json_type(a.`data` ->> '$.covid_icu_beds')='NULL',0,(a.`data` ->> '$.covid_icu_beds')) covid_icu_beds, IF(json_type(a.`data` ->> '$.total_icu_beds')='NULL',0,(a.`data` ->> '$.total_icu_beds')) total_icu_beds, IF(json_type(a.`data` ->> '$.total_ventilators')='NULL',0,(a.`data` ->> '$.total_ventilators')) total_ventilators, IF(json_type(a.`data` ->> '$.total_icu_beds_occupied')='NULL',0,(a.`data` ->> '$.total_icu_beds_occupied')) total_icu_beds_occupied, IF(json_type(a.`data` ->> '$.ventilators_allocated_covid')='NULL',0,(a.`data` ->> '$.ventilators_allocated_covid')) ventilators_allocated_covid \
FROM facilities f \
LEFT JOIN facility_assets a ON f.facility_id=a.facility_id \
WHERE f.facility_id NOT IN (999,2,3,4)) AS temp1 \
LEFT JOIN ( \
SELECT w.facility_id fid, IFNULL(SUM(CASE WHEN (w.severity = 'MILD' AND w.covid_status='suspected') THEN w.total_beds ELSE 0 END),0) AS 'mild-suspected-total', IFNULL(SUM(CASE WHEN (w.severity = 'MILD' AND w.covid_status='suspected') THEN (w.total_beds - w.available_beds) ELSE 0 END),0) AS 'mild-suspected-occupied', IFNULL(SUM(CASE WHEN (w.severity = 'MILD' AND w.covid_status='confirmed') THEN w.total_beds ELSE 0 END),0) AS 'mild-confirmed-total', IFNULL(SUM(CASE WHEN (w.severity = 'MILD' AND w.covid_status='confirmed') THEN (w.total_beds - w.available_beds) ELSE 0 END),0) AS 'mild-confirmed-occupied' \
, IFNULL(SUM(CASE WHEN (w.severity = 'moderate' AND w.covid_status='suspected') THEN w.total_beds ELSE 0 END),0) AS 'moderate-suspected-total', IFNULL(SUM(CASE WHEN (w.severity = 'moderate' AND w.covid_status='suspected') THEN (w.total_beds - w.available_beds) ELSE 0 END),0) AS 'moderate-suspected-occupied', IFNULL(SUM(CASE WHEN (w.severity = 'moderate' AND w.covid_status='confirmed') THEN w.total_beds ELSE 0 END),0) AS 'moderate-confirmed-total', IFNULL(SUM(CASE WHEN (w.severity = 'moderate' AND w.covid_status='confirmed') THEN (w.total_beds - w.available_beds) ELSE 0 END),0) AS 'moderate-confirmed-occupied', IFNULL(SUM(CASE WHEN (w.severity = 'severe' AND w.covid_status='suspected') THEN w.total_beds ELSE 0 END),0) AS 'severe-suspected-total', IFNULL(SUM(CASE WHEN (w.severity = 'severe' AND w.covid_status='suspected') THEN (w.total_beds - w.available_beds) ELSE 0 END),0) AS 'severe-suspected-occupied', IFNULL(SUM(CASE WHEN (w.severity = 'severe' AND w.covid_status='confirmed') THEN w.total_beds ELSE 0 END),0) AS 'severe-confirmed-total', IFNULL(SUM(CASE WHEN (w.severity = 'severe' AND w.covid_status='confirmed') THEN (w.total_beds - w.available_beds) ELSE 0 END),0) AS 'severe-confirmed-occupied', IFNULL(SUM(w.ventilators),0) 'ventilators-earmarked-for-covid', IFNULL(SUM(w.ventilators_occupied),0) 'ventilator-in-use-covid', IFNULL(SUM(w.extra_fields ->> '$.oxygenCylinder'),0) 'oxygen-cylinder-available', IFNULL(SUM(w.extra_fields ->> '$.wallOxygenSuppliedBeds'),0) 'wall-oxygen-supplied-beds', IFNULL(SUM(w.extra_fields ->> '$.centralOxygenSuppliedBeds'),0) 'central-oxygen-supplied-beds', IFNULL(SUM(w.extra_fields ->> '$.pulseOximeters'),0) 'pulse-oximeters', IFNULL(SUM(w.extra_fields ->> '$.infusionPumps'),0) 'infusion-pumps' \
, IFNULL(SUM(w.extra_fields ->> '$.independentBeds'),0) 'independent-beds' \
FROM wards w \
WHERE facility_id NOT IN (999,2,3,4) \
GROUP BY w.facility_id) AS temp2 ON temp1.facility_id=temp2.fid;";

var dataToPush = [];
connection.query(query).stream().pipe(stream.Transform({
    objectMode: true,
    transform: function(data,encoding,callback) {
      dataToPush.push(data);
      if(dataToPush.length < CHUNK_PUSH_SIZE) {
        callback();
      } else {
        pushData(JSON.stringify(dataToPush));
        dataToPush = [];
        setTimeout(()=>callback(), 500);
      }
    }
   }))
   .on('finish',function() { 
        pushData(JSON.stringify(dataToPush));
        console.log('done');
    })

connection.end();