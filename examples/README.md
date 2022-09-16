### Ingestion of Features or Attributes into Watchful
This readme describes how the script `enrich.py` can be used to add external features or attributes to a Watchful project.<sup>1</sup>


#### Overview
- `enrich.py` adds external features or attributes from a CSV file to the current Watchful project
- After the external features or attributes are added, they are queriable in the Watchful application; including being queriable with features of the original dataset already in the Watchful application
- Note that the features or attributes are not displayed in the list view or table view in the Watchful application
- As the features or attributes from the latest used CSV file will overwrite those from the previous CSV file, ensure that the latest CSV file contains all the required features or attributes


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


#### Usage
`python3 ./enrich.py \`  
`[--in_file=/path/to/original/dataset.csv] \`  
`[--out_file=/path/to/output/features/or/attributes/file.attrs] \`  
`[--attr_file=/path/to/input/features/or/attributes/file.csv] \`  
`[--attr_names=attr_name_1,attr_name_2,attr_name_n] \`  
`[--wf_port=9001]`

- `in_file: str`
  </br>optional filepath of the original dataset; if not given the current dataset opened in Watchful will be used
- `out_file: str`
  </br>optional filepath of the output features or attributes; if given it must end with the \".attrs\" extension
- `attr_file: str`
  </br>optional filepath of the input CSV features or attributes; if not given off-the-shelf _spaCy attributes_<sup>2</sup> will be created as the desired attributes
- `attr_names: str`
  </br>optional comma-delimited string of attribute names to be used, if not given all attribute names from `attr_file` will be used
- `wf_port: str`
  </br>optional port number running Watchful, if not given "9001" will be used


#### Requirements
- Environment: python >=3.7
- Install packages: 
  - `pip install -r requirements_enrich.txt`
  - `pip install -r requirements_notebook.txt` (if you are going to run in Jupyter)
- Install the Spacy package: `python3 -m spacy download en`


#### Notes
1. This readme also applies to `attributes.py` which outputs the specific features or attributes file but does not add them to a Watchful project.  
2. These are the part-of-speech, tag, lemma and case of the tokens, entities, noun chunks and sentences (refer to https://spacy.io/usage/linguistic-features).
