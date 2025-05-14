## Contributing to ORION
ORION welcomes contributions to the code base. Implementing parsers for new data sources or improving existing ones is always helpful. Feel free to contact the [maintainer](https://github.com/EvanDietzMorris/) or submit a github issue with any questions.

### For Developers

To add a new data source to ORION, create a new parser. Each parser extends the SourceDataLoader interface in Common/loader_interface.py.

To implement the interface you will need to write a class that fulfills the following.

Set the class level variables for the source ID and provenance:

```
source_id: str = 'ExampleSourceID'
provenance_id: str = 'infores:example_source'
```

In initialization, call the parent init function first and pass the initialization arguments.
Then set the file names for the data file or files.

```
super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

self.data_file = 'example_file.gz'
OR
self.example_file_1 = 'example_file_1.csv'
self.example_file_2 = 'example_file_2.csv'
self.data_files = [self.example_file_1, self.example_file_2]
```

Note that self.data_path is set by the parent class and by default refers to a specific directory for the current version of that source in the storage directory.

Implement get_latest_source_version(). This function should return a string representing the latest available version of the source data.

Implement get_data(). This function should retrieve any source data files. The files should be stored with the file names specified by self.data_file or self.data_files. They should be saved in the directory specified by self.data_path.

Implement parse_data(). This function should parse the data files and populate lists of node and edge objects: self.final_node_list (kgxnode), self.final_edge_list (kgxedge).

Finally, add your source to the list of sources in Common/data_sources.py. The source ID string here should match the one specified in the new parser. Also add your source to the SOURCE_DATA_LOADER_CLASS_IMPORTS dictionary, mapping it to the new parser class.

Now you can use that source ID in a graph spec to include your new source in a graph, or as the source id using load_manager.py.

Always run the pytest tests after altering the codebase.