import psycopg2
import csv


def db_connecting():
    """
    Функция подключения к базе данных
    :return: Возвращает
    """
    try:
        connection = psycopg2.connect(user="postgres"
                                      , password="postgres"
                                      , host="127.0.0.1"
                                      , port="5432"
                                      ,database="animals")
        connection.autocommit = True
        print('Подключение к базе данных установленно')
        return connection
    except:
        print('Ошибка подключение')
        return False


def creating_structure_database(cursor):
    req = """CREATE TABLE  IF NOT EXISTS "outcome_type" (
    "id_outcome" serial PRIMARY KEY ,
    "name_outcome_type" VARCHAR 
    );
     
    CREATE TABLE  IF NOT EXISTS "database_animals"(
        "index" INTEGER PRIMARY KEY,
        "age_upon_outcome" VARCHAR,
        "animal_id" VARCHAR,
        "animal_type" VARCHAR,
        "name" VARCHAR,
        "breed" VARCHAR,
        "color1" VARCHAR,
        "color2" VARCHAR,
        "date_of_birth" VARCHAR,
        "outcome_subtype" VARCHAR,
        "outcome_type" VARCHAR,
        "outcome_month" INTEGER,
        "outcome_year" INTEGER
    );

     
    CREATE TABLE IF NOT EXISTS "color" (
    "id_color" serial PRIMARY KEY NOT NULL,
    "name_color" VARCHAR
    );
    
    
    
    CREATE TABLE IF NOT EXISTS "outcome_subtype" (
        "id_outcome_subtype" serial PRIMARY KEY NOT NULL,
        "name_outcome_subtype" VARCHAR 
        );
    
    
    
    CREATE TABLE IF NOT EXISTS "breed" (
        "id_breed" serial NOT NULL PRIMARY KEY,
        "name_breed" varchar(255) 
    );
    
    
    
    CREATE TABLE IF NOT EXISTS "animal" (
        "animal_id" VARCHAR PRIMARY KEY NOT NULL,
        "fk_animal_type" integer ,
        "name" varchar(255) ,
        "fk_breed" integer ,
        "fk_color1" integer ,
        "fk_color2" integer ,
        "birth" TIMESTAMP  
    );
    
    
    
    CREATE TABLE  IF NOT EXISTS "shelter_info" (
        "id_shelter_info" serial NOT NULL,
        "fk_animal_id" varchar NOT NULL,
        "fk_outcome_subtype" integer NOT NULL,
        "outcome_month" integer NOT NULL,
        "outcome_year" integer NOT NULL,
        "fk_outcome_type" integer NOT NULL,
        "age_upon_outcome" varchar(255) NOT NULL
    );
    
    CREATE TABLE  IF NOT EXISTS "type" (
        "id_type" serial  PRIMARY KEY,
        "name_type" varchar 
    );
    
    ALTER TABLE  "animal" ADD CONSTRAINT "animal_fk0" FOREIGN KEY ("fk_breed") REFERENCES "breed"("id_breed");
    ALTER TABLE "animal" ADD CONSTRAINT "animal_fk1" FOREIGN KEY ("fk_color1") REFERENCES "color"("id_color");
    ALTER TABLE "animal" ADD CONSTRAINT "animal_fk2" FOREIGN KEY ("fk_color2") REFERENCES "color"("id_color");
    ALTER TABLE "animal" ADD CONSTRAINT "animal_fk3" FOREIGN KEY ("fk_animal_type") REFERENCES "type"("id_type");

    ALTER TABLE "shelter_info" ADD CONSTRAINT "shelter_info_fk0" FOREIGN KEY ("fk_animal_id") REFERENCES "animal"("animal_id");
    ALTER TABLE "shelter_info" ADD CONSTRAINT "shelter_info_fk1" FOREIGN KEY ("fk_outcome_subtype") REFERENCES "outcome_subtype"("id_outcome_subtype");
    ALTER TABLE "shelter_info" ADD CONSTRAINT "shelter_info_fk2" FOREIGN KEY ("fk_outcome_type") REFERENCES "outcome_type"("id_outcome");
    """
    cursor.execute(req)


def data_distribution(cursor):
    """
    Функция занесения данных в breed
    :param cursor:
    :return: Ничего не возвращает
    """

    req = """
    COPY database_animals (index,age_upon_outcome,animal_id,animal_type,name,
                           breed,color1,color2,date_of_birth,outcome_subtype,
                           outcome_type,outcome_month,outcome_year)
    FROM 'C:\\database_animals.csv'
    DELIMITER ','
    CSV HEADER;
    
    
    INSERT INTO breed (name_breed)
    SELECT DISTINCT database_animals.breed
    FROM database_animals ;
    
    INSERT INTO outcome_type (name_outcome_type)
    SELECT DISTINCT database_animals.outcome_type
    FROM database_animals ;
    
    INSERT INTO outcome_subtype (name_outcome_subtype)
    SELECT DISTINCT database_animals.outcome_subtype
    FROM database_animals ;
    
    INSERT INTO type (name_type)
    SELECT DISTINCT database_animals.animal_type
    FROM database_animals;
    
    INSERT INTO color (name_color)
    SELECT DISTINCT database_animals.color1
    FROM database_animals ;
    """
    cursor.execute(req)


def creating_summary_tables(cursor):
    req = """
    INSERT INTO animal(animal_id, fk_animal_type, name,
                 fk_breed, fk_color1, fk_color2, birth)
    SELECT DISTINCT da.animal_id, type.id_type, da.name,  breed.id_breed, 
                         cl1.id_color, cl2.id_color, da.date_of_birth::TIMESTAMP
    FROM  database_animals AS da
    JOIN color cl1 ON da.color1 = cl1.name_color
    LEFT JOIN color cl2 ON da.color2 = cl2.name_color
    JOIN breed ON breed.name_breed = da.breed
    JOIN  type ON type.name_type = da.animal_type ;
    
    
    INSERT INTO shelter_info(fk_animal_id, fk_outcome_subtype,
                            outcome_month, outcome_year,fk_outcome_type,
                            age_upon_outcome)
    SELECT  da.animal_id, os.id_outcome_subtype, da.outcome_month, 
                         da.outcome_year, ot.id_outcome,  da.outcome_year -  EXTRACT(YEAR FROM da.date_of_birth::TIMESTAMP)::INTEGER 
    FROM  database_animals AS da
    JOIN outcome_subtype os ON os.name_outcome_subtype = da.outcome_subtype
    JOIN outcome_type ot ON ot.name_outcome_type = da.outcome_type;
    """

    cursor.execute(req)


def main():
    conn = db_connecting()
    if conn:
        cursor = conn.cursor()
    # creating_structure_database(cursor)
    # data_distribution(cursor)
    creating_summary_tables(cursor)
    conn.close()
    cursor.close()
if __name__ == '__main__':
    main()
