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
    "id_outcome" serial NOT NULL,
    "name_outcome_type" VARCHAR NOT NULL,
    CONSTRAINT "outcome_type_pk" PRIMARY KEY ("id_outcome")
     );
     
    CREATE TABLE IF NOT EXISTS "color" (
    "id_colour" serial NOT NULL,
    "name_colour" VARCHAR NOT NULL,
    CONSTRAINT "color_pk" PRIMARY KEY ("id_colour")
    );
    
    
    
    CREATE TABLE IF NOT EXISTS "outcome_subtype" (
        "id_outcome_subtype" serial NOT NULL,
        "name_outcome_subtype" VARCHAR NOT NULL,
        CONSTRAINT "outcome_subtype_pk" PRIMARY KEY ("id_outcome_subtype")
    );
    
    
    
    CREATE TABLE IF NOT EXISTS "breed" (
        "id_breed" serial NOT NULL,
        "name_breed" varchar(255) NOT NULL,
        CONSTRAINT "breed_pk" PRIMARY KEY ("id_breed")
    );
    
    
    
    CREATE TABLE IF NOT EXISTS "animal" (
        "animal_id" serial NOT NULL,
        "fl_animal_type" integer NOT NULL,
        "name" varchar(255) NOT NULL,
        "fk_breed" integer NOT NULL,
        "fk_color1" integer NOT NULL,
        "fk_color2" integer NOT NULL,
        "birth" TIMESTAMP  NOT NULL,
        CONSTRAINT "animal_pk" PRIMARY KEY ("animal_id")
    );
    
    
    
    CREATE TABLE  IF NOT EXISTS "shelter_info" (
        "id_shelter_info" integer NOT NULL,
        "fk_animal_id" integer NOT NULL,
        "fk_outcome_subtype" integer NOT NULL,
        "outcome_month" integer NOT NULL,
        "outcome_year" integer NOT NULL,
        "fk_outcome_type" integer NOT NULL,
        "age_upon_outcome" varchar(255) NOT NULL
    );
    
    CREATE TABLE  IF NOT EXISTS "type" (
        "id_type" integer NOT NULL PRIMARY KEY,
        "name_type" varchar(255) NOT NULL
    );
    
    ALTER TABLE  "animal" ADD CONSTRAINT "animal_fk0" FOREIGN KEY ("fk_breed") REFERENCES "breed"("id_breed");
    ALTER TABLE "animal" ADD CONSTRAINT "animal_fk1" FOREIGN KEY ("fk_color1") REFERENCES "color"("id_colour");
    ALTER TABLE "animal" ADD CONSTRAINT "animal_fk2" FOREIGN KEY ("fk_color2") REFERENCES "color"("id_colour");
    
    ALTER TABLE "shelter_info" ADD CONSTRAINT "shelter_info_fk0" FOREIGN KEY ("fk_animal_id") REFERENCES "animal"("animal_id");
    ALTER TABLE "shelter_info" ADD CONSTRAINT "shelter_info_fk1" FOREIGN KEY ("fk_outcome_subtype") REFERENCES "outcome_subtype"("id_outcome_subtype");
    ALTER TABLE "shelter_info" ADD CONSTRAINT "shelter_info_fk2" FOREIGN KEY ("fk_outcome_type") REFERENCES "outcome_type"("id_outcome");
    ALTER TABLE "animal" ADD CONSTRAINT "animal_fk3" FOREIGN KEY ("fl_animal_type") REFERENCES "type"("id_type");
    """
    cursor.execute(req)


def main():
    conn = db_connecting()
    if conn:
        cursor = conn.cursor()
    creating_structure_database(cursor)


if __name__ == '__main__':
    main()
