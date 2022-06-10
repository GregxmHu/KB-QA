from SPARQLWrapper import SPARQLWrapper, JSON
sparql = SPARQLWrapper("http://localhost:2020/sparql")
prefix="""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vocab: <http://localhost:2020/resource/vocab/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX map: <http://localhost:2020/resource/#>
PREFIX db: <http://localhost:2020/resource/>
"""

query="""
SELECT DISTINCT * WHERE{
	?s ?p ?o
}
LIMIT 10
"""
QueryP2M="""
SELECT DISTINCT ?o WHERE{
    ?s vocab:person_to_movie ?p.
    ?p vocab:person_person_name ?n.
    FILTER ( ?n = "<place_holder>")
    ?s vocab:movie_movie_title ?o
}
"""
QueryM2P="""
SELECT DISTINCT ?o WHERE{
    ?s vocab:person_to_movie ?p.
    ?s vocab:movie_movie_title ?n.
    FILTER ( ?n = "<place_holder>")
    ?p vocab:person_person_name ?o
}
"""

QueryM2G="""
SELECT DISTINCT ?o WHERE{
    ?s vocab:movie_to_genre ?p.
    ?s vocab:movie_movie_title ?n.
    FILTER ( ?n = "<place_holder>")
    ?p vocab:genre_genre_name ?o
}
"""

QueryG2M="""
SELECT DISTINCT ?o WHERE{
    ?s vocab:movie_to_genre ?p.
    ?p vocab:genre_genre_name ?n.
    FILTER ( ?n = "<place_holder>")
    ?s vocab:movie_movie_title ?o
}
"""

## demo : 查找成龙演过洪金宝没演过的电影
print("成龙没和洪金宝合作的电影有：")
sparql.setQuery(prefix+QueryP2M.replace("<place_holder>","成龙"))
sparql.setReturnFormat(JSON)
movies = sparql.query().convert()
movie_mapping=[]
for movie in movies["results"]["bindings"]:
    m_item=movie["o"]["value"]
    sparql.setQuery(prefix+QueryM2P.replace("<place_holder>",m_item))
    sparql.setReturnFormat(JSON)
    persons = sparql.query().convert()
    sub_p=[]
    for person in persons["results"]["bindings"]:
        sub_p.append(person["o"]["value"])
    if "洪金宝" not in sub_p:
        movie_mapping.append(m_item)
print("成龙没和洪金宝合作的电影有：")
print(movie_mapping)
## 查找这些电影中，题材最多的是哪些
Genres=[]
for movie in movie_mapping:
    sparql.setQuery(prefix+QueryM2G.replace("<place_holder>",movie))
    sparql.setReturnFormat(JSON)
    genres = sparql.query().convert()
    for genre in genres["results"]["bindings"]:
        Genres.append(genre["o"]["value"])
print("这些电影中，题材最多的是：",max(Genres,key=Genres.count))




