# glasgowtrafficdata
Get traffic data via Glasgow City Council Open Data API

1/ writecount.py writes SCOOT vehicle flow and concentration to database every 5 minutes.

2/ writeParking.py writes parking availability of car parks to database. Not sure how frequent it is updated.

3/ writeVMS.py writes message board messages to database. Not sure how frequent it is updated.

4/ writeEvents.py writes road events such as road closure to database. (data error???)

All these require an API key from the Glasgow City Council.
(data@glasgow.gov.uk)

To be done:

fix data error of (4).

improve (1). Now it is written in a very inefficient way (for loop; because I failed to disentangle the very nested json file).

download historical traffic movement data of both SCOOT (1028 locations) and 65 (more detailed count locations).
https://data.glasgow.gov.uk/dataset/live-traffic-data-real-time-historic (from 2018 May) But this link stopped working these days.
