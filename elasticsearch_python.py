from operator import index
from pydoc import doc
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# Initialize elasticsearch with credentials.  
es = Elasticsearch(['http://localhost:9200'], basic_auth=('username', 'password'), verify_certs=False)

# Index CRD
new_index = es.indices.create(index="movies")

#see if there's any mapping
mapping = es.indices.get_mapping(index='movies')

# cretae your own mapping
es.indices.put_mapping(
    index="movies",
    body=
        {
                "properties": {
                    "city": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "country": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "datetime": {
                        "type": "date",
                        "format":"yyyy,MM,dd,hh,mm,ss"
                    }
                }
            }
)
#Now again see the mapping set by you
mapping = es.indices.get_mapping(index='movies')

# Read index
es.indices.exists(index="movies")

# Delete index
es.indices.delete(index="movies")

#scan for large data search
scanning = helpers.scan(es,
    query={"query": {"match": {"city": "london"}}},
    index="movies",
    doc_type="books"
)

# Document insertion
document = {
    "name" : "planets composition data",
    "description" : "This data contains chemical composition data of planets"
}
document1 = {
    "name" : "planets dimensional data",
    "description" : "This data contains data of dimensions of planets"
}

indexed_1 = es.index(index= "solarsystem",  id = 1, document=document)
indexed_2 = es.index(index= "solarsystem", id = 2, document=document1)

# Document get
doc1 = es.get(index="solarsystem", id=1)
doc2 = es.get(index="solarsystem", id=2)

# Using regular expressions for searching
es.search(index="solarsystem", body = {"from":0, "size":2,"query": {"regexp":{"sentence": "planets"}}})


# Delete a document
es.delete(index="solarsystem", id=1)

# Check if deleted documet is deleted.
doc1 = es.get(index="solarsystem", id=2)



# Analyzer analyzes incomming stream of data in a certain way, has multiple modes to choose from. 
#Here we are testing multiple analyzers on a single stream of incomming data.
analyzer = ['standard','simple','whitespace','stop','keyword','pattern','fingerprint']

for analyze in analyzer:
    res = es.indices.analyze(body={
      "analyzer" : analyze,
      "text" : ["HII PEOPLE!! Today we are learning about elasticsearch!!!!     it is soooo cool."]
    })
    print("======",analyze,"========")
    for i in res['tokens']:
        print(i['token'])
    print("\n")