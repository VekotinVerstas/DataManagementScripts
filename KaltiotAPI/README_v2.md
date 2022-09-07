# Outdoor gym usage data

This directory contains usage data of KuVa's outdoor gyms.

Data is in two formats, csv and json and split into files covering smaller period of time.

Both formats contain fields listed below:

* **utcdate** or **utctimestamp**: start time of the time period / aikajakson alkuaika
* **area**: the name of the sports venue / liikuntapaikan nimi
* **groupId**: type of gym equipment / kuntosalilaitteen tyyppi
* **trackableId**: the name of gym equipment / kuntosalilaitteen nimi
* **sets**: number of sets performed in the selected time interval (hour/day) /  
  tehtyjen sarjojen määrä valitulla aikavälilä (tunti/päivä)
* **usageMinutes**: usage minutes in the selected interval (hour/day) /  
  käyttöminuutit valitulla aikavälillä (tunti/päivä)
* **repetitions**: the number of repetitions made in the selected time interval (hour/day) /   
  tehtyjen toistojen määrä valitulla aikavälilä (tunti/päivä)

## Sample values

```
"utcdate": "2022-01-04T00:00:00.000Z",
"area": "Pirkkola",
"groupId": "OG30",
"trackableId": "OG30_Pirkkola",
"usageMinutes": 306,
"sets": 275,
"repetitions": 1713
```

## Omnigym equipment types

* OG10 = Squat / Jalkakyykky
* OG23 = Low Row / Vaakasoutu
* OG24 = Lat Pulldown / Ylävetolaite
* OG30 = Bench Press / Penkkipunnerrus
* OG31 = Incline Bench Press / Vinopenkkipunnerrus
* OG41 = Front Press / Etupunnerrus
* OGFA41 = Free Access Front Press / Esteetön Etupunnerrus
* OG70 = Biceps Curl / Hauiskääntö
* OG80 = Triceps Press / Ojentajapunnerrus

# Contact
If you use this data, but want e.g. nicer file layout, contact  
aapo.rista at forumvirium.fi.

