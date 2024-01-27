# Data Script for CS425 Project 1


- `SuperchargeLocations.csv` sourced from Kaggle
- Distances found using the `Open Route Service`
 
- Quick Run Steps
> Install Packages
```bash
pip install -r requirements.txt
```
> Set Route Service API Key
- Copy `EXAMPLE.env` to `.env`
- Generate key on `Open Router Service`
- Set `OPEN_ROUTE_KEY` provided by the service in `.env`

> Generate Dataset of top 100 cities 
```bash
python main.py 100
```

### Output Datastructure Reference

- References
    - ![⚡🚗Tesla Supercharge Locations Globally🌍⚡](https://www.kaggle.com/datasets/omarsobhy14/supercharge-locations)
    - ![Open Route Service](https://openrouteservice.org)
