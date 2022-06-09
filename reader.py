from neo4j import GraphDatabase
class READER():
    def __init__(self,graphDB_Driver):
        self.driver=graphDB_Driver
        self.cqlPersonQuery="MATCH (x:person) RETURN x.person_name"
        self.cqlMovieQuery="MATCH (x:movie) RETURN x.movie_name"
        self.cqlGenreQuery="MATCH (x:movie) RETURN x.genre_name"
        self.cqlP2MQuery="MATCH (x:person {person_name:'person_name_place_holder'})-[r:acted_in]->(y:movie) RETURN y.movie_name"
        self.cqlM2GQuery="MATCH (x:movie {movie_name:'movie_name_place_holder'})-[r:has_genre]->(y:genre) RETURN y.genre_name"
        self.cqlM2PQuery="MATCH (x:person )-[r:acted_in]->(y:movie {movie_name:'movie_name_place_holder'}) RETURN x.person_name"
        self.cqlG2MQuery="MATCH (x:movie )-[r:has_genre]->(y:genre {genre_name:'genre_name_place_holder'}) RETURN x.movie_name"

    def queryP2M(self,person_name):
        movie_names=[]
        with self.driver.session() as graphDB_Session:
            nodes=graphDB_Session.run(
                self.cqlP2MQuery.replace("person_name_place_holder",person_name)
            )
            for node in nodes:
                movie_names.append(str(node)[21:-1])
            graphDB_Session.close()
        return movie_names

    def queryM2G(self,movie_name):
        genre_names=[]
        with self.driver.session() as graphDB_Session:
            nodes=graphDB_Session.run(
                self.cqlM2GQuery.replace("movie_name_place_holder",movie_name)
            )
            for node in nodes:
                genre_names.append(str(node)[21:-1])
            graphDB_Session.close()
        return genre_names

    def queryG2M(self,genre_name):
        movie_names=[]
        with self.driver.session() as graphDB_Session:
            nodes=graphDB_Session.run(
                self.cqlG2MQuery.replace("genre_name_place_holder",genre_name)
            )
            for node in nodes:
                movie_names.append(str(node)[21:-1])
            graphDB_Session.close()
        return movie_names

    def queryM2P(self,movie_name):
        person_names=[]
        with self.driver.session() as graphDB_Session:
            nodes=graphDB_Session.run(
                self.cqlM2PQuery.replace("movie_name_place_holder",movie_name)
            )
            for node in nodes:
                person_names.append(str(node)[22:-1])
            graphDB_Session.close()
        return person_names
    
    def queryPerson(self):
        with self.driver.session() as graphDB_Session:
            nodes=graphDB_Session.run(
                self.cqlPersonQuery
            )
            for node in nodes:
                print(node)
            graphDB_Session.close()
    
    def queryMovie(self):
        with self.driver.session() as graphDB_Session:
            nodes=graphDB_Session.run(
                self.cqlMovieQuery
            )
            for node in nodes:
                print(node)
            graphDB_Session.close()
    
    def queryGenre(self):
        with self.driver.session() as graphDB_Session:
            nodes=graphDB_Session.run(
                self.cqlGenreQuery
            )
            for node in nodes:
                print(node)
            graphDB_Session.close()

if __name__=="__main__":
    url             = "bolt://localhost:7687"
    userName        = "neo4j"
    password        = "123"
    graphDB_Driver  = GraphDatabase.driver(url, auth=(userName, password))
    reader=READER(graphDB_Driver=graphDB_Driver)
    movie_names=reader.queryP2M("成龙")
    colaborators=[]
    genres=[]
    print("成龙演过的电影有:")
    for item in movie_names:
        print(item[1:-1])
        genre=reader.queryM2G(item[1:-1])
        for it in genre:
            genres.append(it[1:-1])
        sub_person=reader.queryM2P(item[1:-1])
        for sp in sub_person:
            if sp != "'成龙'":
                colaborators.append(sp[1:-1])
    print("成龙合作伙伴有:")
    counts=[]
    for pn in set(colaborators):
        print(pn,colaborators.count(pn))
        counts.append(colaborators.count(pn))
    print("与成龙合作最多的是",max(colaborators,key=colaborators.count),"合作次数为",max(counts))
    print("成龙演过最多的题材是",max(genres,key=genres.count),"参演次数为",genres.count(max(genres,key=genres.count)))
    max_genre=max(genres,key=genres.count)
    movies=reader.queryG2M(max_genre)
    print("知识库中{}题材的电影有：".format(max_genre))
    for item in movies:
        print(item[1:-1])
