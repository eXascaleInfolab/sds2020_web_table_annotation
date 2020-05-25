
### Installations

Install required packages:

`pip3 install -r requirements.txt `

### Download required Files

#### Embedding Model
Download all corresponding files to the Model. These files can be found on [this link](http://https://drive.google.com/drive/folders/1lEEANC7M8xc5Jtw1IdVPL8nCeciD5WGW "this link") as follow:
```bash
|-- data
|   |-- Models
|   |   |-- 3-wikidata-20190229-truthy-BETA-cbow-size=100-window=1-min_count=1
|   |   |-- 3-wikidata-20190229-truthy-BETA-cbow-size=100-window=1-min_count=1.trainables.vectors_lockf.npy
|   |   `-- 3-wikidata-20190229-truthy-BETA-cbow-size=100-window=1-min_count=1.wv.vectors.npy
```

#### Surface index form and Levenshtein files
```bash
|-- data
|   `-- surface
|       |-- Levenshtein
|       |   |-- LimayeLevenshtein_allcols.pickle
|       |   `-- T2DLevenshtein_allcols.pickle
|       `-- Surface_Lower_NoPunc.pickle
```

#### Gold standards and their mapping files
Download the Limaye and T2D datasets as well as their annotations. To run the code faster, you can add the already loaded type files for each dataset.
```bash
|-- data
|   |-- Limaye
|   |   |-- DBpedia2Wikidata_Limaye.csv
|   |   |-- entities_instance
|   |   `-- tables_instance
|   |-- LimayeTypes.pickle
|   |-- T2D
|   |   |-- DBpedia2Wikidata_T2D.csv
|   |   |-- entities_instance
|   |   `-- tables_instance
|   |-- T2DTypes.pickle
```

### Wikidata local endpoint

All the steps are described in detail in [this link](http://https://github.com/wikimedia/wikidata-query-rdf/blob/master/docs/getting-started.md "this link"). In the reported result, we used the 2019 version of Wikidata dump.
Keep in mind that the server should be running while running the code.

### Execution

To run the algorithms, having your .env file set up, execute:

`python interface.py`


### Link to the paper
For more detail, please refer to our paper *Annotating Web Tables through Knowledge Bases: A Context-Based Approach, available* [here](https://exascale.info/assets/pdf/eslahi2020sds.pdf "here").


