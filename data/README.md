# Fetching data

To fetch data, navigate to the project's home directory and run

```
python3 data/get_data.py
```

This will download the raw data from the [source](https://datarepo.eng.ucsd.edu/mcauley_group/gdrive/googlelocal/), extract it, and create subsets with the specified number of businesses (set to 1k for the purpose of the submission, can modify this in the ```main()``` function of ```get_data_py```). 

The data subsets will be available in ```data/samples```. For e.g., for the subset with 1000 businesses, the relevant data files generated will be:
- ```data/samples/metadata_1000.csv``` - contains business metadata
- ```data/samples/ratings_1000.csv``` - contains ratings data

