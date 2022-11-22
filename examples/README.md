# Data Enrichment & Ingestion into Watchful
This readme describes how the Watchful module `enrich.py` can be directly used to enrich data and then add the enriched data (features or attributes) to a Watchful project. For the rest of the document, we will be using the term "attributes".

## Overview
- `enrich.py` extracts external attributes from a CSV file, or by creating attributes from the original dataset CSV file, and then adds them to the currently opened Watchful project
- After the external attributes are added, they are queriable in the Watchful application; including being queriable with features of the original dataset already in the Watchful application
- Note that the attributes are not displayed in the list view or table view in the Watchful application
- As the Watchful application uses the attributes from the latest enrichment performed, ensure that the latest enrichment creates all the required attributes

## Data Source for Enrichment

### Attributes CSV File
- This is a CSV file of ready-to-use attributes if you are intending to enrich your original dataset with ready-to-use enriched data
- The CSV file of attributes should have a header containing the names of the attributes, and the same number of data examples (rows) as the original dataset in the Watchful application
- Example:
  | Attribute_name_1 | Percentage | Color | ... | Attribute_name_n |
  | ---------------- | ---------- | ----- | --- | ---------------- |
  | 0.44             | 24         | White | ... | 320              |
  | 0.50             | 69         | White | ... | 210              |
  | 0.12             | 13         | Blue  | ... | 453              |
  | ⋮                 | ⋮          | ⋮     | ⋱   | ⋮                |
- Each row of attributes will be associated with the corresponding data example (row) of the original dataset in the Watchful application

### Dataset CSV File
- You are also able to enrich your original dataset using off-the-shelf models that we have already pre-packed, or using your custom enrichment code that can be seamlessly plugged into Watchful (refer to [Usage](#usage))
<!-- - Refer to our [enrichment introduction notebook](https://github.com/Watchfulio/watchful-py/blob/main/examples/enrichment_intro.ipynb) to get started on custom enrichment-->
- The dataset CSV file to be used for the enrichment would be the original dataset that is loaded into Watchful and currently opened in your Watchful application

## Usage
- Show all the enrichment `options`
```
python3 -m watchful.enrich -h
```
- Run data enrichment with your desired `options`
```
python3 -m watchful.enrich [options]
```

## Requirements
- Environment: The Python 3.8.12 environment is used to test the data enrichment. Generally, Python >=3.7 should work.
- Install Watchful Python Package
```
pip3 install watchful --upgrade
```
- Download spaCy model (optional)
```
python3 -m spacy download en
```
> If you enrich using spaCy via the enrichment `options`, it can be used to identify the part-of-speech, tag, lemma and case of the tokens, entities, noun chunks and sentences (refer to https://spacy.io/usage/linguistic-features).
