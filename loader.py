from neo4j import GraphDatabase
import pymysql
from tqdm import tqdm
class LOADER():
    def __init__(self,mysql_db_conn,graphDB_Driver):
        self.mysql_db_conn=mysql_db_conn
        self.graphDB_Driver=graphDB_Driver

    def __load_entity_from_sql__(self):
        cursor = self.mysql_db_conn.cursor()
        cursor.execute("SELECT movie_id,movie_title FROM movie WHERE movie_title <> 'NULL' ")
        self.movie = cursor.fetchall()
        cursor.execute("SELECT person_id,person_name FROM person WHERE person_name <> 'NULL' ")
        self.person = cursor.fetchall()
        cursor.execute("SELECT genre_id,genre_name FROM genre WHERE genre_name <> 'NULL' ")
        self.genre = cursor.fetchall()
        self.genre_map={}
        self.person_map={}
        self.movie_map={}
        for genre_id,genre_name in self.genre:
            self.genre_map[genre_id]=genre_name.replace("'","")
        for movie_id,movie_name in self.movie:
            self.movie_map[movie_id]=movie_name.replace("'","")
        for person_id,person_name in self.person:
            self.person_map[person_id]=person_name.replace("'","")

    def __load_relation_from_sql__(self):
        cursor = self.mysql_db_conn.cursor()
        cursor.execute("SELECT person_id,movie_id FROM person_to_movie  ")
        self.person2movie = cursor.fetchall()
        cursor.execute("SELECT movie_id,genre_id FROM movie_to_genre  ")
        self.movie2genre = cursor.fetchall()
        
    def load(self):
        self.__load_entity_from_sql__()
        self.__load_relation_from_sql__()
    
    def save(self):
        self.__save_entities__()
        self.__save_relations__()

    def __save_entities__(self):
        with self.graphDB_Driver.session() as graphDB_Session:
            for _,genre_name in tqdm(self.genre_map.items(),desc="save genres"):
                    graphDB_Session.run(
                        "CREATE (:genre { genre_name: 'genre_name_place_holder'})".replace("genre_name_place_holder",genre_name)
                    )
            for _,movie_name in tqdm(self.movie_map.items(),desc="save movies"):
                    graphDB_Session.run(
                        "CREATE (:movie { movie_name: 'movie_name_place_holder'})".replace("movie_name_place_holder",movie_name)
                    )
            for _,person_name in tqdm(self.person_map.items(),desc="save persons"):
                    graphDB_Session.run(
                        "CREATE (:person { person_name: 'person_name_place_holder'})".replace("person_name_place_holder",person_name)
                    )
            
            graphDB_Session.close()

    def __save_relations__(self):
        with self.graphDB_Driver.session() as graphDB_Session:
            for movie_id,genre_id in tqdm(self.movie2genre,desc="create movie2genre relations"):
                if movie_id not in self.movie_map or genre_id not in self.genre_map:
                    continue
                graphDB_Session.run(
                    """MATCH (m:movie {movie_name:'movie_name_place_holder'}), (g:genre {genre_name:'genre_name_place_holder'})
                       WITH m, g CREATE (m)-[:has_genre]->(g)
                    """.replace("movie_name_place_holder",self.movie_map[movie_id]).replace("genre_name_place_holder",self.genre_map[genre_id])
                    )
            for person_id,movie_id in self.person2movie:
                if movie_id not in self.movie_map or person_id not in self.person_map:
                    continue
                print(self.movie_map[movie_id],self.person_map[person_id])
                graphDB_Session.run(
                    """MATCH (p:person {person_name:'person_name_place_holder'}), (m:movie {movie_name:'movie_name_place_holder'})
                       WITH p, m CREATE (p)-[:acted_in]->(m)
                    """.replace("movie_name_place_holder",self.movie_map[movie_id]).replace("person_name_place_holder",self.person_map[person_id])
                    )
            
            graphDB_Session.close()
    
if __name__=="__main__":
    conn = pymysql.connect(host="127.0.0.1",
                            port=3306,
                            user="root",
                            password="123",
                            db="kg_demo_movie",
                            charset="utf8")
    url             = "bolt://localhost:7687"
    userName        = "neo4j"
    password        = "123"
    graphDB_Driver  = GraphDatabase.driver(url, auth=(userName, password))
    loader=LOADER(conn,graphDB_Driver)
    loader.load()
    loader.save()

