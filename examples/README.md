## Data Enrichment & Ingestion into Watchful
This readme describes how the Watchful module `enrich.py` can be used to enrich data and then add the enriched data (features or attributes) to a Watchful project.


### Overview
- `enrich.py` extracts external attributes from a CSV file, or by creating attributes from the dataset CSV file, and then adds them to the current opened Watchful project
- After the external attributes are added, they are queriable in the Watchful application; including being queriable with features of the original dataset already in the Watchful application
- Note that the attributes are not displayed in the list view or table view in the Watchful application
- As the attributes from the latest used CSV file currently overwrite those from the previous CSV file, ensure that the latest CSV file contains all the required attributes


### CSV File

#### Features or Attributes CSV File
- The features or attributes CSV file should have a header containing the names of the attributes, and the same number of data examples as the original dataset in the Watchful application
- Example:
  | Attribute_name_1 | Speed | Color | ... | Attribute_name_n |
  | ---------------- | ----- | ----- | --- | ---------------- |
  | 0.44             | 24    | White | ... | 320              |
  | 0.50             | 69    | White | ... | 210              |
  | 0.12             | 13    | Blue  | ... | 453              |
  | ⋮                 | ⋮     | ⋮     | ⋱   | ⋮                |
- Each row of features or attributes will be associated with the corresponding row in the original dataset in the Watchful application

#### Dataset CSV File
- The dataset CSV file should be the original dataset that is loaded into Watchful and currently opened in the Watchful application

### Usage
- Show all the enrichment options
```
python3 -m watchful.enrich -h
```

- Run data enrichment with desired _options_
```
python3 -m watchful.enrich [options]
```

### Requirements
- Environment: python >=3.7.13
- Install Watchful SDK
  - `pip3 install watchful --upgrade`
- Download Spacy model
  - `python3 -m spacy download en`
  > If you enrich using this model, it can be used to identify the part-of-speech, tag, lemma and case of the tokens, entities, noun chunks and sentences (refer to https://spacy.io/usage/linguistic-features).
